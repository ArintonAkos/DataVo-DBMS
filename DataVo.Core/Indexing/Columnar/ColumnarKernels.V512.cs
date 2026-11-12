#if NET10_0_OR_GREATER
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace DataVo.Core.Indexing.Columnar;

// The kernel loops at 512 bits. Generated from ColumnarKernels.V128.cs by
// scripts/generate-columnar-kernels.sh; do not edit.
internal static partial class ColumnarKernels
{
    private static void CompareV512<T, TOp>(ReadOnlySpan<T> values, T constant, Span<ulong> mask)
        where T : unmanaged
        where TOp : struct, ICompareOp
    {
        int lanes = Vector512<T>.Count;
        Vector512<T> right = Vector512.Create(constant);
        ref T first = ref MemoryMarshal.GetReference(values);
        nuint at = 0;
        for (int word = 0; word < mask.Length; word++)
        {
            ulong bits = 0;
            for (int shift = 0; shift < 64; shift += lanes, at += (nuint)lanes)
            {
                bits |= (ulong)TOp.Test512(Vector512.LoadUnsafe(ref first, at), right).ExtractMostSignificantBits() << shift;
            }

            mask[word] = bits;
        }
    }

    private static long SumInt32V512(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask)
    {
        int lanes = Vector512<int>.Count;
        Vector512<long> low = Vector512<long>.Zero;
        Vector512<long> high = Vector512<long>.Zero;
        ref int first = ref MemoryMarshal.GetReference(values);
        for (int word = 0; word < mask.Length; word++)
        {
            ulong bits = mask[word];
            if (bits == 0)
            {
                continue;
            }

            nuint at = (nuint)(word << 6);
            for (int shift = 0; shift < 64; shift += lanes, at += (nuint)lanes)
            {
                Vector512<int> selected = Vector512.LoadUnsafe(ref first, at) & Lanes512<int>(bits >> shift);
                (Vector512<long> lower, Vector512<long> upper) = Vector512.Widen(selected);
                low += lower;
                high += upper;
            }
        }

        return Vector512.Sum(low + high);
    }

    private static long SumInt64V512(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, out ulong magnitude)
    {
        int lanes = Vector512<long>.Count;
        Vector512<long> sum = Vector512<long>.Zero;
        Vector512<long> seen = Vector512<long>.Zero;
        ref long first = ref MemoryMarshal.GetReference(values);
        for (int word = 0; word < mask.Length; word++)
        {
            ulong bits = mask[word];
            if (bits == 0)
            {
                continue;
            }

            nuint at = (nuint)(word << 6);
            for (int shift = 0; shift < 64; shift += lanes, at += (nuint)lanes)
            {
                Vector512<long> selected = Vector512.LoadUnsafe(ref first, at) & Lanes512<long>(bits >> shift);
                sum += selected;
                seen |= selected ^ (selected >> 63);
            }
        }

        long all = 0;
        for (int lane = 0; lane < lanes; lane++)
        {
            all |= seen.GetElement(lane);
        }

        magnitude = (ulong)all;
        return Vector512.Sum(sum);
    }

    private static void ExtremeV512<T, TOp>(ReadOnlySpan<T> values, ReadOnlySpan<ulong> mask, T identity, out T best, out bool anyNumber, out bool anyNaN)
        where T : unmanaged, IComparisonOperators<T, T, bool>
        where TOp : struct, IExtremeOp
    {
        int lanes = Vector512<T>.Count;
        Vector512<T> found = Vector512.Create(identity);
        Vector512<T> numbers = Vector512<T>.Zero;
        Vector512<T> nans = Vector512<T>.Zero;
        ref T first = ref MemoryMarshal.GetReference(values);
        for (int word = 0; word < mask.Length; word++)
        {
            ulong bits = mask[word];
            if (bits == 0)
            {
                continue;
            }

            nuint at = (nuint)(word << 6);
            for (int shift = 0; shift < 64; shift += lanes, at += (nuint)lanes)
            {
                Vector512<T> value = Vector512.LoadUnsafe(ref first, at);
                Vector512<T> selected = Lanes512<T>(bits >> shift);

                // A NaN is not equal to itself. NaN lanes are left out, so the vector minimum and maximum never
                // see one and give the same answer on every processor.
                Vector512<T> ordered = Vector512.Equals(value, value);
                Vector512<T> taken = selected & ordered;
                found = TOp.Pick512(found, Vector512.ConditionalSelect(taken, value, found));
                numbers |= taken;
                nans |= Vector512.AndNot(selected, ordered);
            }
        }

        T result = identity;
        for (int lane = 0; lane < lanes; lane++)
        {
            T candidate = found.GetElement(lane);
            if (TOp.Better(candidate, result))
            {
                result = candidate;
            }
        }

        best = result;
        anyNumber = numbers.AsByte() != Vector512<byte>.Zero;
        anyNaN = nans.AsByte() != Vector512<byte>.Zero;
    }
}
#endif
