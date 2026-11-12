#if NET10_0_OR_GREATER
using System.Numerics;
using System.Runtime.CompilerServices;
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

    // The aggregates below take four vectors a step into four running results. One running result would make
    // every step wait for the one before it; four let the processor work on them at once.

    private static long SumInt32V512(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask)
    {
        int lanes = Vector512<int>.Count;
        Vector512<long> sum0 = Vector512<long>.Zero;
        Vector512<long> sum1 = Vector512<long>.Zero;
        Vector512<long> sum2 = Vector512<long>.Zero;
        Vector512<long> sum3 = Vector512<long>.Zero;
        ref int first = ref MemoryMarshal.GetReference(values);
        for (int word = 0; word < mask.Length; word++)
        {
            ulong bits = mask[word];
            if (bits == 0)
            {
                continue;
            }

            nuint at = (nuint)(word << 6);
            for (int shift = 0; shift < 64; shift += 4 * lanes, at += (nuint)(4 * lanes))
            {
                sum0 += Widened512(ref first, at, bits >> shift);
                sum1 += Widened512(ref first, at + (nuint)lanes, bits >> (shift + lanes));
                sum2 += Widened512(ref first, at + (nuint)(2 * lanes), bits >> (shift + (2 * lanes)));
                sum3 += Widened512(ref first, at + (nuint)(3 * lanes), bits >> (shift + (3 * lanes)));
            }
        }

        return Vector512.Sum((sum0 + sum1) + (sum2 + sum3));
    }

    // One vector of selected 32-bit values, as the sums of its lower and upper halves widened to 64 bits.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector512<long> Widened512(ref int first, nuint at, ulong bits)
    {
        (Vector512<long> lower, Vector512<long> upper) = Vector512.Widen(Vector512.LoadUnsafe(ref first, at) & Lanes512<int>(bits));
        return lower + upper;
    }

    private static long SumInt64V512(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, out ulong magnitude)
    {
        int lanes = Vector512<long>.Count;
        Vector512<long> sum0 = Vector512<long>.Zero;
        Vector512<long> sum1 = Vector512<long>.Zero;
        Vector512<long> sum2 = Vector512<long>.Zero;
        Vector512<long> sum3 = Vector512<long>.Zero;
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
            for (int shift = 0; shift < 64; shift += 4 * lanes, at += (nuint)(4 * lanes))
            {
                Vector512<long> value0 = Vector512.LoadUnsafe(ref first, at) & Lanes512<long>(bits >> shift);
                Vector512<long> value1 = Vector512.LoadUnsafe(ref first, at + (nuint)lanes) & Lanes512<long>(bits >> (shift + lanes));
                Vector512<long> value2 = Vector512.LoadUnsafe(ref first, at + (nuint)(2 * lanes)) & Lanes512<long>(bits >> (shift + (2 * lanes)));
                Vector512<long> value3 = Vector512.LoadUnsafe(ref first, at + (nuint)(3 * lanes)) & Lanes512<long>(bits >> (shift + (3 * lanes)));
                sum0 += value0;
                sum1 += value1;
                sum2 += value2;
                sum3 += value3;
                seen |= ((value0 ^ (value0 >> 63)) | (value1 ^ (value1 >> 63))) | ((value2 ^ (value2 >> 63)) | (value3 ^ (value3 >> 63)));
            }
        }

        long all = 0;
        for (int lane = 0; lane < lanes; lane++)
        {
            all |= seen.GetElement(lane);
        }

        magnitude = (ulong)all;
        return Vector512.Sum((sum0 + sum1) + (sum2 + sum3));
    }

    private static void ExtremeV512<T, TOp>(ReadOnlySpan<T> values, ReadOnlySpan<ulong> mask, T identity, out T best, out bool anyNumber, out bool anyNaN)
        where T : unmanaged, IComparisonOperators<T, T, bool>
        where TOp : struct, IExtremeOp
    {
        int lanes = Vector512<T>.Count;
        Vector512<T> none = Vector512.Create(identity);
        Vector512<T> found0 = none;
        Vector512<T> found1 = none;
        Vector512<T> found2 = none;
        Vector512<T> found3 = none;
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
            for (int shift = 0; shift < 64; shift += 4 * lanes, at += (nuint)(4 * lanes))
            {
                Vector512<T> value0 = Taken512(ref first, at, bits >> shift, none, out Vector512<T> taken0, out Vector512<T> nan0);
                Vector512<T> value1 = Taken512(ref first, at + (nuint)lanes, bits >> (shift + lanes), none, out Vector512<T> taken1, out Vector512<T> nan1);
                Vector512<T> value2 = Taken512(ref first, at + (nuint)(2 * lanes), bits >> (shift + (2 * lanes)), none, out Vector512<T> taken2, out Vector512<T> nan2);
                Vector512<T> value3 = Taken512(ref first, at + (nuint)(3 * lanes), bits >> (shift + (3 * lanes)), none, out Vector512<T> taken3, out Vector512<T> nan3);
                found0 = TOp.Pick512(found0, value0);
                found1 = TOp.Pick512(found1, value1);
                found2 = TOp.Pick512(found2, value2);
                found3 = TOp.Pick512(found3, value3);
                numbers |= (taken0 | taken1) | (taken2 | taken3);
                nans |= (nan0 | nan1) | (nan2 | nan3);
            }
        }

        Vector512<T> found = TOp.Pick512(TOp.Pick512(found0, found1), TOp.Pick512(found2, found3));
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

    // One vector of values with every lane that is not a selected number replaced by the identity, so it cannot
    // win. A NaN is not equal to itself; NaN lanes are reported apart, and so the vector minimum and maximum
    // never see one and give the same answer on every processor. Integer lanes are always numbers.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector512<T> Taken512<T>(ref T first, nuint at, ulong bits, Vector512<T> none, out Vector512<T> taken, out Vector512<T> nan)
    {
        Vector512<T> value = Vector512.LoadUnsafe(ref first, at);
        Vector512<T> selected = Lanes512<T>(bits);
        if (typeof(T) == typeof(float) || typeof(T) == typeof(double))
        {
            Vector512<T> ordered = Vector512.Equals(value, value);
            taken = selected & ordered;
            nan = Vector512.AndNot(selected, ordered);
        }
        else
        {
            taken = selected;
            nan = Vector512<T>.Zero;
        }

        return Vector512.ConditionalSelect(taken, value, none);
    }
}
#endif
