using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Memory.Flat;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Every kernel at every vector width against a plain loop. Widths the processor lacks run in software, so all
/// of them are checked on every machine.
/// </summary>
public class ColumnarKernelsTests
{
    private const int Values = FlatChunk.Slots;

    public static TheoryData<int> Widths => [(int)SimdWidth.Scalar, (int)SimdWidth.V128, (int)SimdWidth.V256, (int)SimdWidth.V512];

    private static readonly ColumnarCompare[] Operators =
    [
        ColumnarCompare.Equal, ColumnarCompare.NotEqual, ColumnarCompare.Less,
        ColumnarCompare.LessOrEqual, ColumnarCompare.Greater, ColumnarCompare.GreaterOrEqual,
    ];

    private static bool Holds(ColumnarCompare op, int order, bool unordered) => op switch
    {
        ColumnarCompare.Equal => !unordered && order == 0,
        ColumnarCompare.NotEqual => unordered || order != 0,
        ColumnarCompare.Less => !unordered && order < 0,
        ColumnarCompare.LessOrEqual => !unordered && order <= 0,
        ColumnarCompare.Greater => !unordered && order > 0,
        _ => !unordered && order >= 0,
    };

    private static ulong[] RandomMask(Random random, int words, int style)
    {
        var mask = new ulong[words];
        for (int i = 0; i < words; i++)
        {
            mask[i] = style switch
            {
                0 => 0,
                1 => ulong.MaxValue,
                2 => (ulong)random.NextInt64() ^ ((ulong)random.NextInt64() << 1),
                _ => random.Next(4) == 0 ? (ulong)random.NextInt64() << random.Next(64) : 0,
            };
        }

        return mask;
    }

    private static IEnumerable<int> Selected(ulong[] mask)
    {
        for (int i = 0; i < mask.Length * 64; i++)
        {
            if (Bitmask.Test(mask, i))
            {
                yield return i;
            }
        }
    }

    [Fact]
    public void BestWidthIsOneTheProcessorAccelerates()
    {
        SimdWidth expected = System.Runtime.Intrinsics.Vector512.IsHardwareAccelerated ? SimdWidth.V512
            : System.Runtime.Intrinsics.Vector256.IsHardwareAccelerated ? SimdWidth.V256
            : System.Runtime.Intrinsics.Vector128.IsHardwareAccelerated ? SimdWidth.V128
            : SimdWidth.Scalar;
        Assert.Equal(expected, ColumnarKernels.Best);
    }

    [Theory]
    [MemberData(nameof(Widths))]
    public void Compare_Int32_MatchesPlainComparison(int level)
    {
        var width = (SimdWidth)level;
        var random = new Random(11);
        int[] values = new int[Values];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = random.Next(6) switch { 0 => int.MinValue, 1 => int.MaxValue, 2 => 50, _ => random.Next(-100, 100) };
        }

        foreach (int words in new[] { 1, 3, Bitmask.WordsPerChunk })
        {
            foreach (int constant in new[] { 50, 0, int.MinValue, int.MaxValue })
            {
                foreach (ColumnarCompare op in Operators)
                {
                    var mask = new ulong[words];
                    ColumnarKernels.Compare(values, op, constant, mask, width);
                    for (int i = 0; i < words * 64; i++)
                    {
                        Assert.Equal(Holds(op, values[i].CompareTo(constant), false), Bitmask.Test(mask, i));
                    }
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Widths))]
    public void Compare_Int64_MatchesPlainComparison(int level)
    {
        var width = (SimdWidth)level;
        var random = new Random(12);
        long[] values = new long[Values];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = random.Next(6) switch { 0 => long.MinValue, 1 => long.MaxValue, 2 => 5_000_000_000, _ => random.NextInt64(-100, 100) };
        }

        foreach (long constant in new[] { 5_000_000_000, 0, long.MinValue, long.MaxValue })
        {
            foreach (ColumnarCompare op in Operators)
            {
                var mask = new ulong[Bitmask.WordsPerChunk];
                ColumnarKernels.Compare(values, op, constant, mask, width);
                for (int i = 0; i < Values; i++)
                {
                    Assert.Equal(Holds(op, values[i].CompareTo(constant), false), Bitmask.Test(mask, i));
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Widths))]
    public void Compare_Double_TreatsNaNAsUnordered(int level)
    {
        var width = (SimdWidth)level;
        var random = new Random(13);
        double[] values = new double[Values];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = random.Next(8) switch
            {
                0 => double.NaN,
                1 => double.PositiveInfinity,
                2 => double.NegativeInfinity,
                3 => -0.0,
                4 => 0.5,
                _ => random.NextDouble() * 2 - 1,
            };
        }

        foreach (double constant in new[] { 0.5, 0.0, double.NaN, double.PositiveInfinity })
        {
            foreach (ColumnarCompare op in Operators)
            {
                var mask = new ulong[Bitmask.WordsPerChunk];
                ColumnarKernels.Compare(values, op, constant, mask, width);
                for (int i = 0; i < Values; i++)
                {
                    bool unordered = double.IsNaN(values[i]) || double.IsNaN(constant);
                    int order = values[i] < constant ? -1 : values[i] > constant ? 1 : 0;
                    Assert.Equal(Holds(op, order, unordered), Bitmask.Test(mask, i));
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Widths))]
    public void Compare_Single_TreatsNaNAsUnordered(int level)
    {
        var width = (SimdWidth)level;
        var random = new Random(14);
        float[] values = new float[Values];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = random.Next(6) switch { 0 => float.NaN, 1 => float.PositiveInfinity, 2 => 0.25f, _ => random.NextSingle() };
        }

        foreach (float constant in new[] { 0.25f, float.NaN })
        {
            foreach (ColumnarCompare op in Operators)
            {
                var mask = new ulong[Bitmask.WordsPerChunk];
                ColumnarKernels.Compare(values, op, constant, mask, width);
                for (int i = 0; i < Values; i++)
                {
                    bool unordered = float.IsNaN(values[i]) || float.IsNaN(constant);
                    int order = values[i] < constant ? -1 : values[i] > constant ? 1 : 0;
                    Assert.Equal(Holds(op, order, unordered), Bitmask.Test(mask, i));
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Widths))]
    public void SumInt32_AddsOnlySelectedValuesWithoutOverflow(int level)
    {
        var width = (SimdWidth)level;
        var random = new Random(15);
        int[] values = new int[Values];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = random.Next(3) == 0 ? int.MaxValue : random.Next(int.MinValue, int.MaxValue);
        }

        for (int style = 0; style < 4; style++)
        {
            ulong[] mask = RandomMask(random, Bitmask.WordsPerChunk, style);
            long expected = Selected(mask).Sum(i => (long)values[i]);
            Assert.Equal(expected, ColumnarKernels.SumInt32(values, mask, width));
        }

        Array.Fill(values, int.MaxValue);
        Assert.Equal((long)int.MaxValue * Values, ColumnarKernels.SumInt32(values, RandomMask(random, Bitmask.WordsPerChunk, 1), width));
    }

    [Theory]
    [MemberData(nameof(Widths))]
    public void SumInt64_AddsSelectedValuesAndReportsOverflow(int level)
    {
        var width = (SimdWidth)level;
        var random = new Random(16);
        long[] values = new long[Values];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = random.NextInt64(-1_000_000, 1_000_000);
        }

        for (int style = 0; style < 4; style++)
        {
            ulong[] mask = RandomMask(random, Bitmask.WordsPerChunk, style);
            long total = 7;
            ColumnarKernels.SumInt64(values, mask, ref total, width);
            Assert.Equal(7 + Selected(mask).Sum(i => values[i]), total);
        }

        // Large values take the checked path: still exact when the total fits...
        values[10] = long.MaxValue - 5;
        values[11] = -(long.MaxValue - 5);
        ulong[] two = new ulong[Bitmask.WordsPerChunk];
        Bitmask.Set(two, 10);
        Bitmask.Set(two, 11);
        long cancelled = 3;
        ColumnarKernels.SumInt64(values, two, ref cancelled, width);
        Assert.Equal(3, cancelled);

        // ...and an error when it does not.
        values[11] = 100;
        Assert.Throws<OverflowException>(() =>
        {
            long total = 0;
            ColumnarKernels.SumInt64(values, two, ref total, width);
        });

        // Small values whose chunk sum pushes the running total over the edge.
        Array.Fill(values, 1L << 40);
        Assert.Throws<OverflowException>(() =>
        {
            long total = long.MaxValue - 10;
            ColumnarKernels.SumInt64(values, RandomMask(random, Bitmask.WordsPerChunk, 1), ref total, width);
        });
    }

    [Fact]
    public void SumDouble_AddsInRowOrder()
    {
        var random = new Random(17);
        double[] values = new double[Values];
        float[] singles = new float[Values];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = (random.NextDouble() - 0.5) * Math.Pow(10, random.Next(-8, 9));
            singles[i] = (float)values[i];
        }

        ulong[] mask = RandomMask(random, Bitmask.WordsPerChunk, 2);
        double expected = 0.1;
        double expectedSingles = 0.1;
        foreach (int i in Selected(mask))
        {
            expected += values[i];
            expectedSingles += singles[i];
        }

        double total = 0.1;
        ColumnarKernels.SumDouble(values, mask, ref total);
        double totalSingles = 0.1;
        ColumnarKernels.SumSingle(singles, mask, ref totalSingles);

        Assert.Equal(BitConverter.DoubleToInt64Bits(expected), BitConverter.DoubleToInt64Bits(total));
        Assert.Equal(BitConverter.DoubleToInt64Bits(expectedSingles), BitConverter.DoubleToInt64Bits(totalSingles));
    }

    [Theory]
    [MemberData(nameof(Widths))]
    public void Extreme_Integers_FindSmallestAndLargestSelected(int level)
    {
        var width = (SimdWidth)level;
        var random = new Random(18);
        int[] ints = new int[Values];
        long[] longs = new long[Values];
        for (int i = 0; i < Values; i++)
        {
            ints[i] = random.Next(50) == 0 ? (random.Next(2) == 0 ? int.MinValue : int.MaxValue) : random.Next(-1000, 1000);
            longs[i] = random.Next(50) == 0 ? (random.Next(2) == 0 ? long.MinValue : long.MaxValue) : random.NextInt64(-1000, 1000);
        }

        for (int style = 1; style < 4; style++)
        {
            ulong[] mask = RandomMask(random, Bitmask.WordsPerChunk, style);
            if (Bitmask.Count(mask) == 0)
            {
                Bitmask.Set(mask, 77);
            }

            int[] selected = [.. Selected(mask)];
            ColumnarKernels.Extreme(ints, mask, true, out int smallInt, width);
            ColumnarKernels.Extreme(ints, mask, false, out int largeInt, width);
            ColumnarKernels.Extreme(longs, mask, true, out long smallLong, width);
            ColumnarKernels.Extreme(longs, mask, false, out long largeLong, width);

            Assert.Equal(selected.Min(i => ints[i]), smallInt);
            Assert.Equal(selected.Max(i => ints[i]), largeInt);
            Assert.Equal(selected.Min(i => longs[i]), smallLong);
            Assert.Equal(selected.Max(i => longs[i]), largeLong);
        }
    }

    [Theory]
    [MemberData(nameof(Widths))]
    public void Extreme_Floats_SetNaNAsideAndReportIt(int level)
    {
        var width = (SimdWidth)level;
        var random = new Random(19);
        double[] doubles = new double[Values];
        float[] floats = new float[Values];
        for (int i = 0; i < Values; i++)
        {
            doubles[i] = random.Next(20) switch
            {
                0 => double.NaN,
                1 => double.PositiveInfinity,
                2 => double.NegativeInfinity,
                _ => random.NextDouble() * 200 - 100,
            };
            floats[i] = (float)doubles[i];
        }

        for (int style = 1; style < 4; style++)
        {
            ulong[] mask = RandomMask(random, Bitmask.WordsPerChunk, style);
            int[] selected = [.. Selected(mask)];
            double[] numbers = [.. selected.Select(i => doubles[i]).Where(v => !double.IsNaN(v))];
            bool nan = selected.Any(i => double.IsNaN(doubles[i]));

            foreach (bool smallest in new[] { true, false })
            {
                ColumnarKernels.Extreme(doubles, mask, smallest, out double best, out bool anyNumber, out bool anyNaN, width);
                ColumnarKernels.Extreme(floats, mask, smallest, out float bestSingle, out bool anySingle, out bool anySingleNaN, width);

                Assert.Equal(numbers.Length > 0, anyNumber);
                Assert.Equal(nan, anyNaN);
                Assert.Equal(numbers.Length > 0, anySingle);
                Assert.Equal(nan, anySingleNaN);
                if (numbers.Length > 0)
                {
                    Assert.Equal(smallest ? numbers.Min() : numbers.Max(), best);
                    Assert.Equal((float)(smallest ? numbers.Min() : numbers.Max()), bestSingle);
                }
            }
        }

        // Only NaN selected, and only infinities selected.
        Array.Fill(doubles, double.NaN);
        ulong[] all = RandomMask(random, Bitmask.WordsPerChunk, 1);
        ColumnarKernels.Extreme(doubles, all, true, out _, out bool number, out bool notNumber, width);
        Assert.False(number);
        Assert.True(notNumber);

        Array.Fill(doubles, double.PositiveInfinity);
        ColumnarKernels.Extreme(doubles, all, true, out double infinite, out number, out notNumber, width);
        Assert.True(number);
        Assert.False(notNumber);
        Assert.Equal(double.PositiveInfinity, infinite);
    }
}
