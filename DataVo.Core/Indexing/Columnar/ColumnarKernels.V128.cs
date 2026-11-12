#if NET10_0_OR_GREATER
using System.Numerics;
using System.Runtime.CompilerServices;
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

    // The aggregates below take four vectors a step into four running results. One running result would make
    // every step wait for the one before it; four let the processor work on them at once.

    private static long SumInt32V128(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask)
    {
        int lanes = Vector128<int>.Count;
        Vector128<long> sum0 = Vector128<long>.Zero;
        Vector128<long> sum1 = Vector128<long>.Zero;
        Vector128<long> sum2 = Vector128<long>.Zero;
        Vector128<long> sum3 = Vector128<long>.Zero;
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
                sum0 += Widened128(ref first, at, bits >> shift);
                sum1 += Widened128(ref first, at + (nuint)lanes, bits >> (shift + lanes));
                sum2 += Widened128(ref first, at + (nuint)(2 * lanes), bits >> (shift + (2 * lanes)));
                sum3 += Widened128(ref first, at + (nuint)(3 * lanes), bits >> (shift + (3 * lanes)));
            }
        }

        return Vector128.Sum((sum0 + sum1) + (sum2 + sum3));
    }

    // One vector of selected 32-bit values, as the sums of its lower and upper halves widened to 64 bits.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<long> Widened128(ref int first, nuint at, ulong bits)
    {
        (Vector128<long> lower, Vector128<long> upper) = Vector128.Widen(Vector128.LoadUnsafe(ref first, at) & Lanes128<int>(bits));
        return lower + upper;
    }

    private static long SumInt64V128(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, out ulong magnitude)
    {
        int lanes = Vector128<long>.Count;
        Vector128<long> sum0 = Vector128<long>.Zero;
        Vector128<long> sum1 = Vector128<long>.Zero;
        Vector128<long> sum2 = Vector128<long>.Zero;
        Vector128<long> sum3 = Vector128<long>.Zero;
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
            for (int shift = 0; shift < 64; shift += 4 * lanes, at += (nuint)(4 * lanes))
            {
                Vector128<long> value0 = Vector128.LoadUnsafe(ref first, at) & Lanes128<long>(bits >> shift);
                Vector128<long> value1 = Vector128.LoadUnsafe(ref first, at + (nuint)lanes) & Lanes128<long>(bits >> (shift + lanes));
                Vector128<long> value2 = Vector128.LoadUnsafe(ref first, at + (nuint)(2 * lanes)) & Lanes128<long>(bits >> (shift + (2 * lanes)));
                Vector128<long> value3 = Vector128.LoadUnsafe(ref first, at + (nuint)(3 * lanes)) & Lanes128<long>(bits >> (shift + (3 * lanes)));
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
        return Vector128.Sum((sum0 + sum1) + (sum2 + sum3));
    }

    private static void ExtremeV128<T, TOp>(ReadOnlySpan<T> values, ReadOnlySpan<ulong> mask, T identity, out T best, out bool anyNumber, out bool anyNaN)
        where T : unmanaged, IComparisonOperators<T, T, bool>
        where TOp : struct, IExtremeOp
    {
        int lanes = Vector128<T>.Count;
        Vector128<T> none = Vector128.Create(identity);
        Vector128<T> found0 = none;
        Vector128<T> found1 = none;
        Vector128<T> found2 = none;
        Vector128<T> found3 = none;
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
            for (int shift = 0; shift < 64; shift += 4 * lanes, at += (nuint)(4 * lanes))
            {
                Vector128<T> value0 = Taken128(ref first, at, bits >> shift, none, out Vector128<T> taken0, out Vector128<T> nan0);
                Vector128<T> value1 = Taken128(ref first, at + (nuint)lanes, bits >> (shift + lanes), none, out Vector128<T> taken1, out Vector128<T> nan1);
                Vector128<T> value2 = Taken128(ref first, at + (nuint)(2 * lanes), bits >> (shift + (2 * lanes)), none, out Vector128<T> taken2, out Vector128<T> nan2);
                Vector128<T> value3 = Taken128(ref first, at + (nuint)(3 * lanes), bits >> (shift + (3 * lanes)), none, out Vector128<T> taken3, out Vector128<T> nan3);
                found0 = TOp.Pick128(found0, value0);
                found1 = TOp.Pick128(found1, value1);
                found2 = TOp.Pick128(found2, value2);
                found3 = TOp.Pick128(found3, value3);
                numbers |= (taken0 | taken1) | (taken2 | taken3);
                nans |= (nan0 | nan1) | (nan2 | nan3);
            }
        }

        Vector128<T> found = TOp.Pick128(TOp.Pick128(found0, found1), TOp.Pick128(found2, found3));
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

    // One vector of values with every lane that is not a selected number replaced by the identity, so it cannot
    // win. A NaN is not equal to itself; NaN lanes are reported apart, and so the vector minimum and maximum
    // never see one and give the same answer on every processor. Integer lanes are always numbers.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<T> Taken128<T>(ref T first, nuint at, ulong bits, Vector128<T> none, out Vector128<T> taken, out Vector128<T> nan)
    {
        Vector128<T> value = Vector128.LoadUnsafe(ref first, at);
        Vector128<T> selected = Lanes128<T>(bits);
        if (typeof(T) == typeof(float) || typeof(T) == typeof(double))
        {
            Vector128<T> ordered = Vector128.Equals(value, value);
            taken = selected & ordered;
            nan = Vector128.AndNot(selected, ordered);
        }
        else
        {
            taken = selected;
            nan = Vector128<T>.Zero;
        }

        return Vector128.ConditionalSelect(taken, value, none);
    }
}
#endif
