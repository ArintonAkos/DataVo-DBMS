using System.Runtime.CompilerServices;

namespace DataVo.Core.Indexing.Columnar;

internal static partial class ColumnarKernels
{
    /// <summary>Sets bit <c>n</c> of <paramref name="mask"/> when value <c>n</c> meets the comparison.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Compare(ReadOnlySpan<int> values, ColumnarCompare op, int constant, Span<ulong> mask) =>
        Compare(values, op, constant, mask, Best);

    /// <inheritdoc cref="Compare(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong})"/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Compare(ReadOnlySpan<long> values, ColumnarCompare op, long constant, Span<ulong> mask) =>
        Compare(values, op, constant, mask, Best);

    /// <inheritdoc cref="Compare(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong})"/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Compare(ReadOnlySpan<float> values, ColumnarCompare op, float constant, Span<ulong> mask) =>
        Compare(values, op, constant, mask, Best);

    /// <inheritdoc cref="Compare(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong})"/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Compare(ReadOnlySpan<double> values, ColumnarCompare op, double constant, Span<ulong> mask) =>
        Compare(values, op, constant, mask, Best);

    /// <summary>Adds up the selected values. The sum of one chunk always fits.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long SumInt32(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask) => SumInt32(values, mask, Best);

    /// <summary>The smallest or largest selected value; meaningless when none is selected.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Extreme(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask, bool smallest, out int best) =>
        Extreme(values, mask, smallest, out best, Best);

    /// <inheritdoc cref="Extreme(ReadOnlySpan{int}, ReadOnlySpan{ulong}, bool, out int)"/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Extreme(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, bool smallest, out long best) =>
        Extreme(values, mask, smallest, out best, Best);

    /// <summary>The smallest or largest selected number, with NaN values set aside; see <see cref="ExtremeScalar(ReadOnlySpan{double}, ReadOnlySpan{ulong}, bool, out double, out bool, out bool)"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Extreme(ReadOnlySpan<float> values, ReadOnlySpan<ulong> mask, bool smallest, out float best, out bool anyNumber, out bool anyNaN) =>
        Extreme(values, mask, smallest, out best, out anyNumber, out anyNaN, Best);

    /// <inheritdoc cref="Extreme(ReadOnlySpan{float}, ReadOnlySpan{ulong}, bool, out float, out bool, out bool)"/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Extreme(ReadOnlySpan<double> values, ReadOnlySpan<ulong> mask, bool smallest, out double best, out bool anyNumber, out bool anyNaN) =>
        Extreme(values, mask, smallest, out best, out anyNumber, out anyNaN, Best);
}
