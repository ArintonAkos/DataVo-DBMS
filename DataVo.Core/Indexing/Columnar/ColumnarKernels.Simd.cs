#if NET10_0_OR_GREATER
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace DataVo.Core.Indexing.Columnar;

// Picks a vector form for each kernel. The loops themselves are in ColumnarKernels.V128/V256/V512.cs, which
// are the same text at three widths; .NET lowers Vector128 to SSE2 or NEON, Vector256 to AVX2 and Vector512
// to AVX-512, and runs any of them in software where the processor has no such instructions.
internal static partial class ColumnarKernels
{
    private static SimdWidth DetectWidth() =>
        Vector512.IsHardwareAccelerated ? SimdWidth.V512
        : Vector256.IsHardwareAccelerated ? SimdWidth.V256
        : Vector128.IsHardwareAccelerated ? SimdWidth.V128
        : SimdWidth.Scalar;

    /// <summary>One comparison, at each vector width. A struct, so each kernel loop is compiled around it.</summary>
    private interface ICompareOp
    {
        static abstract Vector128<T> Test128<T>(Vector128<T> left, Vector128<T> right);

        static abstract Vector256<T> Test256<T>(Vector256<T> left, Vector256<T> right);

        static abstract Vector512<T> Test512<T>(Vector512<T> left, Vector512<T> right);
    }

    private readonly struct EqualOp : ICompareOp
    {
        public static Vector128<T> Test128<T>(Vector128<T> left, Vector128<T> right) => Vector128.Equals(left, right);

        public static Vector256<T> Test256<T>(Vector256<T> left, Vector256<T> right) => Vector256.Equals(left, right);

        public static Vector512<T> Test512<T>(Vector512<T> left, Vector512<T> right) => Vector512.Equals(left, right);
    }

    private readonly struct NotEqualOp : ICompareOp
    {
        public static Vector128<T> Test128<T>(Vector128<T> left, Vector128<T> right) => ~Vector128.Equals(left, right);

        public static Vector256<T> Test256<T>(Vector256<T> left, Vector256<T> right) => ~Vector256.Equals(left, right);

        public static Vector512<T> Test512<T>(Vector512<T> left, Vector512<T> right) => ~Vector512.Equals(left, right);
    }

    private readonly struct LessOp : ICompareOp
    {
        public static Vector128<T> Test128<T>(Vector128<T> left, Vector128<T> right) => Vector128.LessThan(left, right);

        public static Vector256<T> Test256<T>(Vector256<T> left, Vector256<T> right) => Vector256.LessThan(left, right);

        public static Vector512<T> Test512<T>(Vector512<T> left, Vector512<T> right) => Vector512.LessThan(left, right);
    }

    private readonly struct LessOrEqualOp : ICompareOp
    {
        public static Vector128<T> Test128<T>(Vector128<T> left, Vector128<T> right) => Vector128.LessThanOrEqual(left, right);

        public static Vector256<T> Test256<T>(Vector256<T> left, Vector256<T> right) => Vector256.LessThanOrEqual(left, right);

        public static Vector512<T> Test512<T>(Vector512<T> left, Vector512<T> right) => Vector512.LessThanOrEqual(left, right);
    }

    private readonly struct GreaterOp : ICompareOp
    {
        public static Vector128<T> Test128<T>(Vector128<T> left, Vector128<T> right) => Vector128.GreaterThan(left, right);

        public static Vector256<T> Test256<T>(Vector256<T> left, Vector256<T> right) => Vector256.GreaterThan(left, right);

        public static Vector512<T> Test512<T>(Vector512<T> left, Vector512<T> right) => Vector512.GreaterThan(left, right);
    }

    private readonly struct GreaterOrEqualOp : ICompareOp
    {
        public static Vector128<T> Test128<T>(Vector128<T> left, Vector128<T> right) => Vector128.GreaterThanOrEqual(left, right);

        public static Vector256<T> Test256<T>(Vector256<T> left, Vector256<T> right) => Vector256.GreaterThanOrEqual(left, right);

        public static Vector512<T> Test512<T>(Vector512<T> left, Vector512<T> right) => Vector512.GreaterThanOrEqual(left, right);
    }

    /// <summary>Keeps the smaller or the larger of two values, at each vector width and for one value.</summary>
    private interface IExtremeOp
    {
        static abstract Vector128<T> Pick128<T>(Vector128<T> left, Vector128<T> right);

        static abstract Vector256<T> Pick256<T>(Vector256<T> left, Vector256<T> right);

        static abstract Vector512<T> Pick512<T>(Vector512<T> left, Vector512<T> right);

        static abstract bool Better<T>(T candidate, T best)
            where T : IComparisonOperators<T, T, bool>;
    }

    private readonly struct MinOp : IExtremeOp
    {
        public static Vector128<T> Pick128<T>(Vector128<T> left, Vector128<T> right) => Vector128.Min(left, right);

        public static Vector256<T> Pick256<T>(Vector256<T> left, Vector256<T> right) => Vector256.Min(left, right);

        public static Vector512<T> Pick512<T>(Vector512<T> left, Vector512<T> right) => Vector512.Min(left, right);

        public static bool Better<T>(T candidate, T best)
            where T : IComparisonOperators<T, T, bool> => candidate < best;
    }

    private readonly struct MaxOp : IExtremeOp
    {
        public static Vector128<T> Pick128<T>(Vector128<T> left, Vector128<T> right) => Vector128.Max(left, right);

        public static Vector256<T> Pick256<T>(Vector256<T> left, Vector256<T> right) => Vector256.Max(left, right);

        public static Vector512<T> Pick512<T>(Vector512<T> left, Vector512<T> right) => Vector512.Max(left, right);

        public static bool Better<T>(T candidate, T best)
            where T : IComparisonOperators<T, T, bool> => candidate > best;
    }

    // Turns the low bits of a selection word into lanes that are all ones where the bit is set: the word is
    // copied to every lane and each lane tests its own bit.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<T> Lanes128<T>(ulong bits)
    {
        if (Unsafe.SizeOf<T>() == sizeof(long))
        {
            Vector128<long> select = Vector128.Create(1L, 2L);
            return Vector128.Equals(Vector128.Create((long)bits) & select, select).As<long, T>();
        }
        else
        {
            Vector128<int> select = Vector128.Create(1, 2, 4, 8);
            return Vector128.Equals(Vector128.Create((int)bits) & select, select).As<int, T>();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<T> Lanes256<T>(ulong bits)
    {
        if (Unsafe.SizeOf<T>() == sizeof(long))
        {
            Vector256<long> select = Vector256.Create(1L, 2L, 4L, 8L);
            return Vector256.Equals(Vector256.Create((long)bits) & select, select).As<long, T>();
        }
        else
        {
            Vector256<int> select = Vector256.Create(1, 2, 4, 8, 16, 32, 64, 128);
            return Vector256.Equals(Vector256.Create((int)bits) & select, select).As<int, T>();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector512<T> Lanes512<T>(ulong bits)
    {
        if (Unsafe.SizeOf<T>() == sizeof(long))
        {
            Vector512<long> select = Vector512.Create(1L, 2L, 4L, 8L, 16L, 32L, 64L, 128L);
            return Vector512.Equals(Vector512.Create((long)bits) & select, select).As<long, T>();
        }
        else
        {
            Vector512<int> select = Vector512.Create(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768);
            return Vector512.Equals(Vector512.Create((int)bits) & select, select).As<int, T>();
        }
    }

    private static void Covers(int values, int words)
    {
        if (values < words << 6)
        {
            throw new ArgumentException("The mask covers more values than the chunk holds.");
        }
    }

    /// <summary>Sets bit <c>n</c> of <paramref name="mask"/> when value <c>n</c> meets the comparison, at a chosen width.</summary>
    public static void Compare(ReadOnlySpan<int> values, ColumnarCompare op, int constant, Span<ulong> mask, SimdWidth width)
    {
        if (width == SimdWidth.Scalar)
        {
            CompareScalar(values, op, constant, mask);
        }
        else
        {
            CompareVector(values, op, constant, mask, width);
        }
    }

    /// <inheritdoc cref="Compare(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong}, SimdWidth)"/>
    public static void Compare(ReadOnlySpan<long> values, ColumnarCompare op, long constant, Span<ulong> mask, SimdWidth width)
    {
        if (width == SimdWidth.Scalar)
        {
            CompareScalar(values, op, constant, mask);
        }
        else
        {
            CompareVector(values, op, constant, mask, width);
        }
    }

    /// <inheritdoc cref="Compare(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong}, SimdWidth)"/>
    public static void Compare(ReadOnlySpan<float> values, ColumnarCompare op, float constant, Span<ulong> mask, SimdWidth width)
    {
        if (width == SimdWidth.Scalar)
        {
            CompareScalar(values, op, constant, mask);
        }
        else
        {
            CompareVector(values, op, constant, mask, width);
        }
    }

    /// <inheritdoc cref="Compare(ReadOnlySpan{int}, ColumnarCompare, int, Span{ulong}, SimdWidth)"/>
    public static void Compare(ReadOnlySpan<double> values, ColumnarCompare op, double constant, Span<ulong> mask, SimdWidth width)
    {
        if (width == SimdWidth.Scalar)
        {
            CompareScalar(values, op, constant, mask);
        }
        else
        {
            CompareVector(values, op, constant, mask, width);
        }
    }

    private static void CompareVector<T>(ReadOnlySpan<T> values, ColumnarCompare op, T constant, Span<ulong> mask, SimdWidth width)
        where T : unmanaged
    {
        Covers(values.Length, mask.Length);
        switch (op)
        {
            case ColumnarCompare.Equal:
                CompareVector<T, EqualOp>(values, constant, mask, width);
                break;
            case ColumnarCompare.NotEqual:
                CompareVector<T, NotEqualOp>(values, constant, mask, width);
                break;
            case ColumnarCompare.Less:
                CompareVector<T, LessOp>(values, constant, mask, width);
                break;
            case ColumnarCompare.LessOrEqual:
                CompareVector<T, LessOrEqualOp>(values, constant, mask, width);
                break;
            case ColumnarCompare.Greater:
                CompareVector<T, GreaterOp>(values, constant, mask, width);
                break;
            case ColumnarCompare.GreaterOrEqual:
                CompareVector<T, GreaterOrEqualOp>(values, constant, mask, width);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(op));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CompareVector<T, TOp>(ReadOnlySpan<T> values, T constant, Span<ulong> mask, SimdWidth width)
        where T : unmanaged
        where TOp : struct, ICompareOp
    {
        if (width == SimdWidth.V512)
        {
            CompareV512<T, TOp>(values, constant, mask);
        }
        else if (width == SimdWidth.V256)
        {
            CompareV256<T, TOp>(values, constant, mask);
        }
        else
        {
            CompareV128<T, TOp>(values, constant, mask);
        }
    }

    /// <summary>Adds up the selected values at a chosen width. The sum of one chunk always fits.</summary>
    public static long SumInt32(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask, SimdWidth width)
    {
        Covers(values.Length, mask.Length);
        return width switch
        {
            SimdWidth.V512 => SumInt32V512(values, mask),
            SimdWidth.V256 => SumInt32V256(values, mask),
            SimdWidth.V128 => SumInt32V128(values, mask),
            _ => SumInt32Scalar(values, mask),
        };
    }

    private static long SumInt64Wrapping(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, out ulong magnitude, SimdWidth width)
    {
        Covers(values.Length, mask.Length);
        return width switch
        {
            SimdWidth.V512 => SumInt64V512(values, mask, out magnitude),
            SimdWidth.V256 => SumInt64V256(values, mask, out magnitude),
            SimdWidth.V128 => SumInt64V128(values, mask, out magnitude),
            _ => SumInt64Scalar(values, mask, out magnitude),
        };
    }

    /// <summary>The smallest or largest selected value at a chosen width; meaningless when none is selected.</summary>
    public static void Extreme(ReadOnlySpan<int> values, ReadOnlySpan<ulong> mask, bool smallest, out int best, SimdWidth width)
    {
        if (width == SimdWidth.Scalar)
        {
            ExtremeScalar(values, mask, smallest, out best);
        }
        else
        {
            ExtremeVector(values, mask, smallest, smallest ? int.MaxValue : int.MinValue, out best, out _, out _, width);
        }
    }

    /// <inheritdoc cref="Extreme(ReadOnlySpan{int}, ReadOnlySpan{ulong}, bool, out int, SimdWidth)"/>
    public static void Extreme(ReadOnlySpan<long> values, ReadOnlySpan<ulong> mask, bool smallest, out long best, SimdWidth width)
    {
        if (width == SimdWidth.Scalar)
        {
            ExtremeScalar(values, mask, smallest, out best);
        }
        else
        {
            ExtremeVector(values, mask, smallest, smallest ? long.MaxValue : long.MinValue, out best, out _, out _, width);
        }
    }

    /// <summary>The smallest or largest selected number at a chosen width, with NaN values set aside.</summary>
    public static void Extreme(ReadOnlySpan<float> values, ReadOnlySpan<ulong> mask, bool smallest, out float best, out bool anyNumber, out bool anyNaN, SimdWidth width)
    {
        if (width == SimdWidth.Scalar)
        {
            ExtremeScalar(values, mask, smallest, out best, out anyNumber, out anyNaN);
        }
        else
        {
            ExtremeVector(values, mask, smallest, smallest ? float.PositiveInfinity : float.NegativeInfinity, out best, out anyNumber, out anyNaN, width);
        }
    }

    /// <inheritdoc cref="Extreme(ReadOnlySpan{float}, ReadOnlySpan{ulong}, bool, out float, out bool, out bool, SimdWidth)"/>
    public static void Extreme(ReadOnlySpan<double> values, ReadOnlySpan<ulong> mask, bool smallest, out double best, out bool anyNumber, out bool anyNaN, SimdWidth width)
    {
        if (width == SimdWidth.Scalar)
        {
            ExtremeScalar(values, mask, smallest, out best, out anyNumber, out anyNaN);
        }
        else
        {
            ExtremeVector(values, mask, smallest, smallest ? double.PositiveInfinity : double.NegativeInfinity, out best, out anyNumber, out anyNaN, width);
        }
    }

    private static void ExtremeVector<T>(ReadOnlySpan<T> values, ReadOnlySpan<ulong> mask, bool smallest, T identity, out T best, out bool anyNumber, out bool anyNaN, SimdWidth width)
        where T : unmanaged, IComparisonOperators<T, T, bool>
    {
        Covers(values.Length, mask.Length);
        if (smallest)
        {
            ExtremeVector<T, MinOp>(values, mask, identity, out best, out anyNumber, out anyNaN, width);
        }
        else
        {
            ExtremeVector<T, MaxOp>(values, mask, identity, out best, out anyNumber, out anyNaN, width);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ExtremeVector<T, TOp>(ReadOnlySpan<T> values, ReadOnlySpan<ulong> mask, T identity, out T best, out bool anyNumber, out bool anyNaN, SimdWidth width)
        where T : unmanaged, IComparisonOperators<T, T, bool>
        where TOp : struct, IExtremeOp
    {
        if (width == SimdWidth.V512)
        {
            ExtremeV512<T, TOp>(values, mask, identity, out best, out anyNumber, out anyNaN);
        }
        else if (width == SimdWidth.V256)
        {
            ExtremeV256<T, TOp>(values, mask, identity, out best, out anyNumber, out anyNaN);
        }
        else
        {
            ExtremeV128<T, TOp>(values, mask, identity, out best, out anyNumber, out anyNaN);
        }
    }
}
#endif
