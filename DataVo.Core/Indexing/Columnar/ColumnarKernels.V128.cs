#if NET10_0_OR_GREATER
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace DataVo.Core.Indexing.Columnar;

// The kernel loops at 128 bits. ColumnarKernels.V256.cs and .V512.cs are this file with the width replaced
// (scripts/generate-columnar-kernels.sh); edit this one and regenerate.
internal static partial class ColumnarKernels
{
    private static void CompareV128<T, TOp>(ReadOnlySpan<T> values, T constant, Span<ulong> mask)
        where T : unmanaged
        where TOp : struct, ICompareOp
    {
        int lanes = Vector128<T>.Count;
        Vector128<T> right = Vector128.Create(constant);
        ref T first = ref MemoryMarshal.GetReference(values);
        nuint at = 0;
        for (int word = 0; word < mask.Length; word++)
        {
            ulong bits = 0;
            for (int shift = 0; shift < 64; shift += lanes, at += (nuint)lanes)
            {
                bits |= (ulong)TOp.Test128(Vector128.LoadUnsafe(ref first, at), right).ExtractMostSignificantBits() << shift;
            }

            mask[word] = bits;
        }
    }

    private static long SumInt32V128(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask)
    {
        int lanes = Vector128<int>.Count;
        Vector128<long> low = Vector128<long>.Zero;
        Vector128<long> high = Vector128<long>.Zero;
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
                Vector128<int> selected = Vector128.LoadUnsafe(ref first, at) & Lanes128<int>(bits >> shift);
                (Vector128<long> lower, Vector128<long> upper) = Vector128.Widen(selected);
                low += lower;
                high += upper;
            }
        }

        return Vector128.Sum(low + high);
    }

    private static long SumInt64V128(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, out ulong magnitude)
    {
        int lanes = Vector128<long>.Count;
        Vector128<long> sum = Vector128<long>.Zero;
        Vector128<long> seen = Vector128<long>.Zero;
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
                Vector128<long> selected = Vector128.LoadUnsafe(ref first, at) & Lanes128<long>(bits >> shift);
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
        return Vector128.Sum(sum);
    }

    private static void ExtremeV128<T, TOp>(ReadOnlySpan<T> values, ReadOnlySpan<ulong> mask, T identity, out T best, out bool anyNumber, out bool anyNaN)
        where T : unmanaged, IComparisonOperators<T, T, bool>
        where TOp : struct, IExtremeOp
    {
        int lanes = Vector128<T>.Count;
        Vector128<T> found = Vector128.Create(identity);
        Vector128<T> numbers = Vector128<T>.Zero;
        Vector128<T> nans = Vector128<T>.Zero;
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
                Vector128<T> value = Vector128.LoadUnsafe(ref first, at);
                Vector128<T> selected = Lanes128<T>(bits >> shift);

                // A NaN is not equal to itself. NaN lanes are left out, so the vector minimum and maximum never
                // see one and give the same answer on every processor.
                Vector128<T> ordered = Vector128.Equals(value, value);
                Vector128<T> taken = selected & ordered;
                found = TOp.Pick128(found, Vector128.ConditionalSelect(taken, value, found));
                numbers |= taken;
                nans |= Vector128.AndNot(selected, ordered);
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
        anyNumber = numbers.AsByte() != Vector128<byte>.Zero;
        anyNaN = nans.AsByte() != Vector128<byte>.Zero;
    }
}
#endif
