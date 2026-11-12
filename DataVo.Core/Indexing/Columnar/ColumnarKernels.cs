using System.Runtime.CompilerServices;
using DataVo.Core.Memory.Flat;

namespace DataVo.Core.Indexing.Columnar;

/// <summary>How a kernel compares a column's values with one constant.</summary>
internal enum ColumnarCompare : byte
{
    /// <summary><c>value == constant</c>.</summary>
    Equal,

    /// <summary><c>value != constant</c>.</summary>
    NotEqual,

    /// <summary><c>value &lt; constant</c>.</summary>
    Less,

    /// <summary><c>value &lt;= constant</c>.</summary>
    LessOrEqual,

    /// <summary><c>value &gt; constant</c>.</summary>
    Greater,

    /// <summary><c>value &gt;= constant</c>.</summary>
    GreaterOrEqual,
}

/// <summary>The vector width a kernel runs at.</summary>
internal enum SimdWidth : byte
{
    /// <summary>One value at a time.</summary>
    Scalar,

    /// <summary>128-bit vectors: SSE2 and ARM64 NEON.</summary>
    V128,

    /// <summary>256-bit vectors: AVX2.</summary>
    V256,

    /// <summary>512-bit vectors: AVX-512.</summary>
    V512,
}

/// <summary>
/// The inner loops of a columnar scan. Each works on one chunk: a dense run of one column's values and bit
/// masks of <c>mask.Length</c> words covering its first <c>mask.Length * 64</c> values. A comparison writes a
/// mask; an aggregate reads the values a mask selects. Nothing is allocated.
/// </summary>
/// <remarks>
/// Every kernel has a scalar form, which defines the result, and vector forms that must agree with it. The
/// public entry points pick the widest form the processor accelerates.
/// </remarks>
internal static partial class ColumnarKernels
{
    /// <summary>The widest vector form this processor accelerates.</summary>
    public static readonly SimdWidth Best = DetectWidth();

    // Largest magnitude, as a bit pattern, at which a chunk of 64-bit values cannot overflow its sum:
    // 4,096 values below 2^50 add up to less than 2^62.
    private const ulong SafeSumMagnitude = (1UL << 50) - 1;

    // For each comparison, the outcomes it accepts: bit 0 unordered (a NaN is involved), bit 1 less,
    // bit 2 greater, bit 4 equal.
    private static int Truth(ColumnarCompare op) => op switch
    {
        ColumnarCompare.Equal => 0b1_0000,
        ColumnarCompare.NotEqual => 0b0_0111,
        ColumnarCompare.Less => 0b0_0010,
        ColumnarCompare.LessOrEqual => 0b1_0010,
        ColumnarCompare.Greater => 0b0_0100,
        ColumnarCompare.GreaterOrEqual => 0b1_0100,
        _ => throw new ArgumentOutOfRangeException(nameof(op)),
    };

    /// <summary>Sets bit <c>n</c> of <paramref name="mask"/> when value <c>n</c> meets the comparison.</summary>
    public static void CompareScalar(ReadOnlySpan<int> values, ColumnarCompare op, int constant, Span<ulong> mask)
    {
        int truth = Truth(op);
        for (int word = 0; word < mask.Length; word++)
        {
            ReadOnlySpan<int> run = values.Slice(word << 6, 64);
            ulong bits = 0;
            for (int bit = 0; bit < run.Length; bit++)
            {
                int value = run[bit];
                int outcome = (value < constant ? 1 : 0) | (value > constant ? 2 : 0) | (value == constant ? 4 : 0);
                bits |= (ulong)((truth >> outcome) & 1) << bit;
            }

            mask[word] = bits;
        }
    }

    /// <inheritdoc cref="CompareScalar(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong})"/>
    public static void CompareScalar(ReadOnlySpan<long> values, ColumnarCompare op, long constant, Span<ulong> mask)
    {
        int truth = Truth(op);
        for (int word = 0; word < mask.Length; word++)
        {
            ReadOnlySpan<long> run = values.Slice(word << 6, 64);
            ulong bits = 0;
            for (int bit = 0; bit < run.Length; bit++)
            {
                long value = run[bit];
                int outcome = (value < constant ? 1 : 0) | (value > constant ? 2 : 0) | (value == constant ? 4 : 0);
                bits |= (ulong)((truth >> outcome) & 1) << bit;
            }

            mask[word] = bits;
        }
    }

    /// <inheritdoc cref="CompareScalar(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong})"/>
    public static void CompareScalar(ReadOnlySpan<float> values, ColumnarCompare op, float constant, Span<ulong> mask)
    {
        int truth = Truth(op);
        for (int word = 0; word < mask.Length; word++)
        {
            ReadOnlySpan<float> run = values.Slice(word << 6, 64);
            ulong bits = 0;
            for (int bit = 0; bit < run.Length; bit++)
            {
                float value = run[bit];
                int outcome = (value < constant ? 1 : 0) | (value > constant ? 2 : 0) | (value == constant ? 4 : 0);
                bits |= (ulong)((truth >> outcome) & 1) << bit;
            }

            mask[word] = bits;
        }
    }

    /// <inheritdoc cref="CompareScalar(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong})"/>
    public static void CompareScalar(ReadOnlySpan<double> values, ColumnarCompare op, double constant, Span<ulong> mask)
    {
        int truth = Truth(op);
        for (int word = 0; word < mask.Length; word++)
        {
            ReadOnlySpan<double> run = values.Slice(word << 6, 64);
            ulong bits = 0;
            for (int bit = 0; bit < run.Length; bit++)
            {
                double value = run[bit];
                int outcome = (value < constant ? 1 : 0) | (value > constant ? 2 : 0) | (value == constant ? 4 : 0);
                bits |= (ulong)((truth >> outcome) & 1) << bit;
            }

            mask[word] = bits;
        }
    }

    /// <summary>Adds up the selected values. The sum of one chunk always fits.</summary>
    public static long SumInt32Scalar(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask)
    {
        long sum = 0;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                sum += values[(word << 6) + Bitmask.LowestSet(bits)];
            }
        }

        return sum;
    }

    /// <summary>Adds the selected values to <paramref name="total"/> one at a time, in order.</summary>
    /// <exception cref="OverflowException">The running total left the range of <see cref="long"/>.</exception>
    public static void SumInt64Checked(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, ref long total)
    {
        long sum = total;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                sum = checked(sum + values[(word << 6) + Bitmask.LowestSet(bits)]);
            }
        }

        total = sum;
    }

    /// <summary>
    /// Adds the selected values without overflow checks, and reports how large they get:
    /// <paramref name="magnitude"/> is the OR of each value's magnitude bits (<c>v</c>, or <c>~v</c> when negative).
    /// </summary>
    public static long SumInt64Scalar(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, out ulong magnitude)
    {
        long sum = 0;
        long seen = 0;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                long value = values[(word << 6) + Bitmask.LowestSet(bits)];
                sum = unchecked(sum + value);
                seen |= value ^ (value >> 63);
            }
        }

        magnitude = (ulong)seen;
        return sum;
    }

    /// <summary>
    /// Adds the selected values to <paramref name="total"/> one at a time, in order. Floating-point sums are
    /// never vectorised: a different order of additions gives a different result.
    /// </summary>
    public static void SumDouble(ReadOnlySpan<double> values, ReadOnlySpan<ulong> mask, ref double total)
    {
        double sum = total;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                sum += values[(word << 6) + Bitmask.LowestSet(bits)];
            }
        }

        total = sum;
    }

    /// <inheritdoc cref="SumDouble"/>
    public static void SumSingle(ReadOnlySpan<float> values, ReadOnlySpan<ulong> mask, ref double total)
    {
        double sum = total;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                sum += values[(word << 6) + Bitmask.LowestSet(bits)];
            }
        }

        total = sum;
    }

    /// <summary>The smallest or largest selected value; <paramref name="best"/> is meaningless when none is selected.</summary>
    public static void ExtremeScalar(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask, bool smallest, out int best)
    {
        int found = smallest ? int.MaxValue : int.MinValue;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                int value = values[(word << 6) + Bitmask.LowestSet(bits)];
                found = smallest ? Math.Min(found, value) : Math.Max(found, value);
            }
        }

        best = found;
    }

    /// <inheritdoc cref="ExtremeScalar(ReadOnlySpan{int}, ReadOnlySpan{ulong}, bool, out int)"/>
    public static void ExtremeScalar(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, bool smallest, out long best)
    {
        long found = smallest ? long.MaxValue : long.MinValue;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                long value = values[(word << 6) + Bitmask.LowestSet(bits)];
                found = smallest ? Math.Min(found, value) : Math.Max(found, value);
            }
        }

        best = found;
    }

    /// <summary>
    /// The smallest or largest selected number, with NaN values set aside: <paramref name="anyNumber"/> says
    /// whether a number was selected (else <paramref name="best"/> is meaningless) and <paramref name="anyNaN"/>
    /// whether a NaN was. Of two zeros of different sign either may be returned.
    /// </summary>
    public static void ExtremeScalar(ReadOnlySpan<double> values, ReadOnlySpan<ulong> mask, bool smallest, out double best, out bool anyNumber, out bool anyNaN)
    {
        double found = smallest ? double.PositiveInfinity : double.NegativeInfinity;
        anyNumber = false;
        anyNaN = false;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                double value = values[(word << 6) + Bitmask.LowestSet(bits)];
                if (double.IsNaN(value))
                {
                    anyNaN = true;
                }
                else
                {
                    anyNumber = true;
                    found = smallest ? (value < found ? value : found) : (value > found ? value : found);
                }
            }
        }

        best = found;
    }

    /// <inheritdoc cref="ExtremeScalar(ReadOnlySpan{double}, ReadOnlySpan{ulong}, bool, out double, out bool, out bool)"/>
    public static void ExtremeScalar(ReadOnlySpan<float> values, ReadOnlySpan<ulong> mask, bool smallest, out float best, out bool anyNumber, out bool anyNaN)
    {
        float found = smallest ? float.PositiveInfinity : float.NegativeInfinity;
        anyNumber = false;
        anyNaN = false;
        for (int word = 0; word < mask.Length; word++)
        {
            for (ulong bits = mask[word]; bits != 0; bits &= bits - 1)
            {
                float value = values[(word << 6) + Bitmask.LowestSet(bits)];
                if (float.IsNaN(value))
                {
                    anyNaN = true;
                }
                else
                {
                    anyNumber = true;
                    found = smallest ? (value < found ? value : found) : (value > found ? value : found);
                }
            }
        }

        best = found;
    }

    /// <summary>
    /// Adds the selected values of a chunk to <paramref name="total"/>. The vector form is used when the
    /// chunk's values are small enough that its own sum cannot overflow; otherwise the values are added one at a
    /// time. An overflow that a later value of the same chunk cancels is therefore not always reported.
    /// </summary>
    /// <exception cref="OverflowException">The total left the range of <see cref="long"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SumInt64(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, ref long total) =>
        SumInt64(values, mask, ref total, Best);

    /// <inheritdoc cref="SumInt64(ReadOnlySpan{long}, ReadOnlySpan{ulong}, ref long)"/>
    public static void SumInt64(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, ref long total, SimdWidth width)
    {
        long sum = SumInt64Wrapping(values, mask, out ulong magnitude, width);
        if (magnitude <= SafeSumMagnitude)
        {
            total = checked(total + sum);
        }
        else
        {
            SumInt64Checked(values, mask, ref total);
        }
    }

#if !NET10_0_OR_GREATER
    private static SimdWidth DetectWidth() => SimdWidth.Scalar;

    public static void Compare(ReadOnlySpan<int> values, ColumnarCompare op, int constant, Span<ulong> mask, SimdWidth width) => CompareScalar(values, op, constant, mask);

    public static void Compare(ReadOnlySpan<long> values, ColumnarCompare op, long constant, Span<ulong> mask, SimdWidth width) => CompareScalar(values, op, constant, mask);

    public static void Compare(ReadOnlySpan<float> values, ColumnarCompare op, float constant, Span<ulong> mask, SimdWidth width) => CompareScalar(values, op, constant, mask);

    public static void Compare(ReadOnlySpan<double> values, ColumnarCompare op, double constant, Span<ulong> mask, SimdWidth width) => CompareScalar(values, op, constant, mask);

    public static long SumInt32(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask, SimdWidth width) => SumInt32Scalar(values, mask);

    private static long SumInt64Wrapping(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, out ulong magnitude, SimdWidth width) => SumInt64Scalar(values, mask, out magnitude);

    public static void Extreme(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask, bool smallest, out int best, SimdWidth width) => ExtremeScalar(values, mask, smallest, out best);

    public static void Extreme(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, bool smallest, out long best, SimdWidth width) => ExtremeScalar(values, mask, smallest, out best);

    public static void Extreme(ReadOnlySpan<float> values, ReadOnlySpan<ulong> mask, bool smallest, out float best, out bool anyNumber, out bool anyNaN, SimdWidth width) => ExtremeScalar(values, mask, smallest, out best, out anyNumber, out anyNaN);

    public static void Extreme(ReadOnlySpan<double> values, ReadOnlySpan<ulong> mask, bool smallest, out double best, out bool anyNumber, out bool anyNaN, SimdWidth width) => ExtremeScalar(values, mask, smallest, out best, out anyNumber, out anyNaN);
#endif
}
