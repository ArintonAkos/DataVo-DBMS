#if NET10_0_OR_GREATER
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace DataVo.Core.Indexing.Columnar;

// The kernel loops at 256 bits. Generated from ColumnarKernels.V128.cs by
// scripts/generate-columnar-kernels.sh; do not edit.
internal static partial class ColumnarKernels
{
    private static void CompareV256<T, TOp>(ReadOnlySpan<T> values, T constant, Span<ulong> mask)
        where T : unmanaged
        where TOp : struct, ICompareOp
    {
        int lanes = Vector256<T>.Count;
        Vector256<T> right = Vector256.Create(constant);
        ref T first = ref MemoryMarshal.GetReference(values);
        nuint at = 0;
        for (int word = 0; word < mask.Length; word++)
        {
            ulong bits = 0;
            for (int shift = 0; shift < 64; shift += lanes, at += (nuint)lanes)
            {
                bits |= (ulong)TOp.Test256(Vector256.LoadUnsafe(ref first, at), right).ExtractMostSignificantBits() << shift;
            }

            mask[word] = bits;
        }
    }

    private static long SumInt32V256(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask)
    {
        int lanes = Vector256<int>.Count;
        Vector256<long> low = Vector256<long>.Zero;
        Vector256<long> high = Vector256<long>.Zero;
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
                Vector256<int> selected = Vector256.LoadUnsafe(ref first, at) & Lanes256<int>(bits >> shift);
                (Vector256<long> lower, Vector256<long> upper) = Vector256.Widen(selected);
                low += lower;
                high += upper;
            }
        }

        return Vector256.Sum(low + high);
    }

    private static long SumInt64V256(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, out ulong magnitude)
    {
        int lanes = Vector256<long>.Count;
        Vector256<long> sum = Vector256<long>.Zero;
        Vector256<long> seen = Vector256<long>.Zero;
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
                Vector256<long> selected = Vector256.LoadUnsafe(ref first, at) & Lanes256<long>(bits >> shift);
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
        return Vector256.Sum(sum);
    }

    private static void ExtremeV256<T, TOp>(ReadOnlySpan<T> values, ReadOnlySpan<ulong> mask, T identity, out T best, out bool anyNumber, out bool anyNaN)
        where T : unmanaged, IComparisonOperators<T, T, bool>
        where TOp : struct, IExtremeOp
    {
        int lanes = Vector256<T>.Count;
        Vector256<T> found = Vector256.Create(identity);
        Vector256<T> numbers = Vector256<T>.Zero;
        Vector256<T> nans = Vector256<T>.Zero;
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
                Vector256<T> value = Vector256.LoadUnsafe(ref first, at);
                Vector256<T> selected = Lanes256<T>(bits >> shift);

                // A NaN is not equal to itself. NaN lanes are left out, so the vector minimum and maximum never
                // see one and give the same answer on every processor.
                Vector256<T> ordered = Vector256.Equals(value, value);
                Vector256<T> taken = selected & ordered;
                found = TOp.Pick256(found, Vector256.ConditionalSelect(taken, value, found));
                numbers |= taken;
                nans |= Vector256.AndNot(selected, ordered);
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
        anyNumber = numbers.AsByte() != Vector256<byte>.Zero;
        anyNaN = nans.AsByte() != Vector256<byte>.Zero;
    }
}
#endif
