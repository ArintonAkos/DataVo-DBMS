using System.Globalization;
using DataVo.Core.Compat;
using DataVo.Core.CompiledQueries;

namespace DataVo.Tests.Performance;

public sealed class ScalarKeyFormattingAllocationTests
{
    [Fact]
    public void TypedNumericKeys_AllocateOnlyTheirResultString()
    {
        AssertOutputOnlyAllocation(
            static () => DataVoCompiledQuery.BuildScalarComparisonKey(42),
            static () => string.Create(CultureInfo.InvariantCulture, $"[{42}]"));
        AssertOutputOnlyAllocation(
            static () => DataVoCompiledQuery.BuildScalarComparisonKey(4_294_967_296L),
            static () => string.Create(CultureInfo.InvariantCulture, $"[{4_294_967_296L}]"));
        AssertOutputOnlyAllocation(
            static () => DataVoCompiledQuery.BuildScalarComparisonKey(12.5d),
            static () => string.Create(CultureInfo.InvariantCulture, $"[{12.5d}]"));

        AssertOutputOnlyAllocation(
            static () => ScalarKeyFormatter.FormatBracketed(12.5f, CultureInfo.InvariantCulture),
            static () => string.Create(CultureInfo.InvariantCulture, $"[{12.5f}]"));
        AssertOutputOnlyAllocation(
            static () => ScalarKeyFormatter.FormatBracketed(12.5m, CultureInfo.InvariantCulture),
            static () => string.Create(CultureInfo.InvariantCulture, $"[{12.5m}]"));
    }

    [Fact]
    public void CompiledNumericKeys_RemainInvariantUnderCommaDecimalCulture()
    {
        CultureInfo original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");

            Assert.Equal("[42]", DataVoCompiledQuery.BuildScalarComparisonKey(42));
            Assert.Equal("[4294967296]", DataVoCompiledQuery.BuildScalarComparisonKey(4_294_967_296L));
            Assert.Equal("[12.5]", DataVoCompiledQuery.BuildScalarComparisonKey(12.5d));
            Assert.Equal("[12.5]", DataVoCompiledQuery.BuildScalarComparisonKey((object)12.5f));
            Assert.Equal("[12.5]", DataVoCompiledQuery.BuildScalarComparisonKey((object)12.5m));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    private static void AssertOutputOnlyAllocation(Func<string> actual, Func<string> outputOnly)
    {
        long actualBytes = MeasureAllocation(actual);
        long outputOnlyBytes = MeasureAllocation(outputOnly);

        Assert.True(actualBytes <= outputOnlyBytes,
            $"scalar key allocated {actualBytes} B/call; output-only formatting allocated {outputOnlyBytes} B/call");
    }

    /// <summary>
    /// Steady-state bytes per call. Code first runs unoptimized and is recompiled in the background
    /// (tiered JIT); under a loaded parallel test run that can lag behind the warm-up, and unoptimized
    /// code may still box. Measuring several rounds and keeping the lowest value isolates the steady
    /// state this test is about.
    /// </summary>
    private static long MeasureAllocation(Func<string> action)
    {
        long best = long.MaxValue;
        for (int round = 0; round < 5 && best > 0; round++)
        {
            best = Math.Min(best, MeasureRound(action));
            if (round < 4)
            {
                Thread.Sleep(50); // give background tier-up a chance between rounds
            }
        }

        return best;
    }

    private static long MeasureRound(Func<string> action)
    {
        string? sink = null;
        for (int i = 0; i < 2_000; i++)
        {
            sink = action();
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        const int measured = 10_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < measured; i++)
        {
            sink = action();
        }

        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / measured;
        GC.KeepAlive(sink);
        return perCall;
    }
}
