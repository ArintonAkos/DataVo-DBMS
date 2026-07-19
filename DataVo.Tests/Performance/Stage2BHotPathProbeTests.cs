using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Transactions;
using Xunit.Abstractions;

namespace DataVo.Tests.Performance;

public sealed class Stage2BProbeFactAttribute : FactAttribute
{
    public Stage2BProbeFactAttribute()
    {
        if (!string.Equals(
                Environment.GetEnvironmentVariable("DATAVO_RUN_STAGE2B_PROBE"),
                "1",
                StringComparison.Ordinal))
        {
            Skip = "Set DATAVO_RUN_STAGE2B_PROBE=1 and run Release to collect Stage 2B evidence.";
        }
    }
}

public sealed class Stage2BHotPathProbeTests
{
    private const int SampleCount = 5;
    private const int CrcIterations = 10_000;
    private const int ScalarIterations = 25_000;
    private const int GuidIterations = 250_000;

    private static readonly byte[] CrcPayload = CreateCrcPayload();
    private static readonly Guid ExpectedGuid = new("fedcba98-7654-3210-fedc-ba9876543210");

    private readonly ITestOutputHelper _output;

    public Stage2BHotPathProbeTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Stage2BProbeFact]
    public void HotPaths_ReportRepeatableAllocationAndTimingEvidence()
    {
#if DEBUG
        throw new InvalidOperationException("The Stage 2B hot-path probe must run in Release configuration.");
#endif

        string revision = Escape(Environment.GetEnvironmentVariable("DATAVO_STAGE2B_REVISION") ?? "working-tree");
        string profile = Escape(Environment.GetEnvironmentVariable("DATAVO_STAGE2B_PROBE_PROFILE") ?? "modern");
        Assert.True(profile is "modern" or "compat-regression",
            $"Unknown DATAVO_STAGE2B_PROBE_PROFILE '{profile}'.");

        _output.WriteLine(FormattableString.Invariant(
            $"STAGE2B_PROBE|revision={revision}|profile={profile}|framework={Escape(RuntimeInformation.FrameworkDescription)}|os={Escape(RuntimeInformation.OSDescription)}|arch={RuntimeInformation.ProcessArchitecture}|process_id={Environment.ProcessId}"));

        Assert.Equal(ReferenceCrc32C(CrcPayload), WalCrc32C.HashToUInt32(CrcPayload));
        MetricResult crc = Measure(CrcIterations, operationsPerIteration: 1, RunCrc);
        PrintMetric(revision, "crc32c-1024", CrcIterations, crc);
        Assert.Equal(0d, crc.MedianBytesPerOperation);

        Assert.Equal("[42]", DataVoCompiledQuery.BuildScalarComparisonKey(42));
        Assert.Equal("[4294967296]", DataVoCompiledQuery.BuildScalarComparisonKey(4_294_967_296L));
        Assert.Equal("[12.5]", DataVoCompiledQuery.BuildScalarComparisonKey(12.5d));

        MetricResult scalar = Measure(ScalarIterations, operationsPerIteration: 3, RunTypedScalarKeys);
        MetricResult scalarControl = Measure(ScalarIterations, operationsPerIteration: 3, RunTypedScalarControl);
        PrintMetric(revision, "scalar-int-long-double", ScalarIterations * 3, scalar);
        PrintMetric(revision, "scalar-output-control", ScalarIterations * 3, scalarControl);

        if (profile == "modern")
        {
            Assert.True(scalar.MedianBytesPerOperation <= scalarControl.MedianBytesPerOperation + 0.01d,
                $"Typed scalar formatting allocated {scalar.MedianBytesPerOperation:F3} B/op; " +
                $"the output-only control allocated {scalarControl.MedianBytesPerOperation:F3} B/op.");
        }
        else
        {
            Assert.True(scalar.MedianBytesPerOperation > scalarControl.MedianBytesPerOperation + 1d,
                "The compatibility-regression profile no longer demonstrates numeric boxing.");
        }

        if (TryVerifyExtendedScalarSeam(out ulong extendedScalarChecksum))
        {
            _output.WriteLine(FormattableString.Invariant(
                $"STAGE2B_PROBE|revision={revision}|metric=scalar-float-decimal|available=1|correctness_checksum={extendedScalarChecksum:x16}|allocation_timing=covered_by_focused_test"));
        }
        else
        {
            _output.WriteLine(
                $"STAGE2B_PROBE|revision={revision}|metric=scalar-float-decimal|available=0|reason=typed_seam_not_present");
        }

        CellValue guidCell = CellValue.From(ExpectedGuid);
        Assert.Equal(ExpectedGuid, guidCell.AsGuid());
        MetricResult guid = Measure(GuidIterations, operationsPerIteration: 1, RunGuidReads);
        PrintMetric(revision, "cellvalue-asguid", GuidIterations, guid);

        if (profile == "modern")
        {
            Assert.Equal(0d, guid.MedianBytesPerOperation);
        }
        else
        {
            Assert.True(guid.MedianBytesPerOperation > 0d,
                "The compatibility-regression profile no longer demonstrates the GUID read allocation.");
        }
    }

    private void PrintMetric(string revision, string metric, int iterations, MetricResult result)
    {
        _output.WriteLine(FormattableString.Invariant(
            $"STAGE2B_PROBE|revision={revision}|metric={metric}|checksum={result.Checksum:x16}|iterations={iterations}|bytes_per_op={result.MedianBytesPerOperation:F3}|median_ms={result.MedianMilliseconds:F3}|spread_ms={result.SpreadMilliseconds:F3}|samples={SampleCount}"));
    }

    private static MetricResult Measure(
        int iterations,
        int operationsPerIteration,
        Func<int, ulong> action)
    {
        _ = action(Math.Min(iterations, 2_000));

        var elapsed = new double[SampleCount];
        var bytesPerOperation = new double[SampleCount];
        ulong checksum = 0;

        for (int sample = 0; sample < SampleCount; sample++)
        {
            ForceGc();
            long beforeBytes = GC.GetAllocatedBytesForCurrentThread();
            long started = Stopwatch.GetTimestamp();
            ulong currentChecksum = action(iterations);
            long stopped = Stopwatch.GetTimestamp();
            long allocated = GC.GetAllocatedBytesForCurrentThread() - beforeBytes;

            if (sample == 0)
            {
                checksum = currentChecksum;
            }
            else
            {
                Assert.Equal(checksum, currentChecksum);
            }

            elapsed[sample] = (stopped - started) * 1_000d / Stopwatch.Frequency;
            bytesPerOperation[sample] = allocated / (double)(iterations * operationsPerIteration);
        }

        Array.Sort(elapsed);
        Array.Sort(bytesPerOperation);
        return new MetricResult(
            checksum,
            bytesPerOperation[SampleCount / 2],
            elapsed[SampleCount / 2],
            elapsed[^1] - elapsed[0]);
    }

    private static ulong RunCrc(int iterations)
    {
        ulong checksum = 1469598103934665603UL;
        for (int i = 0; i < iterations; i++)
        {
            checksum = Mix(checksum, WalCrc32C.HashToUInt32(CrcPayload));
        }

        return checksum;
    }

    private static ulong RunTypedScalarKeys(int iterations)
    {
        ulong checksum = 1469598103934665603UL;
        for (int i = 0; i < iterations; i++)
        {
            checksum = Mix(checksum, DataVoCompiledQuery.BuildScalarComparisonKey(42));
            checksum = Mix(checksum, DataVoCompiledQuery.BuildScalarComparisonKey(4_294_967_296L));
            checksum = Mix(checksum, DataVoCompiledQuery.BuildScalarComparisonKey(12.5d));
        }

        return checksum;
    }

    private static ulong RunTypedScalarControl(int iterations)
    {
        ulong checksum = 1469598103934665603UL;
        for (int i = 0; i < iterations; i++)
        {
            checksum = Mix(checksum, string.Create(CultureInfo.InvariantCulture, $"[{42}]"));
            checksum = Mix(checksum, string.Create(CultureInfo.InvariantCulture, $"[{4_294_967_296L}]"));
            checksum = Mix(checksum, string.Create(CultureInfo.InvariantCulture, $"[{12.5d}]"));
        }

        return checksum;
    }

    private static ulong RunGuidReads(int iterations)
    {
        CellValue cell = CellValue.From(ExpectedGuid);
        ulong checksum = 1469598103934665603UL;
        for (int i = 0; i < iterations; i++)
        {
            Guid value = cell.AsGuid();
            checksum = Mix(checksum, unchecked((uint)value.GetHashCode()));
        }

        return checksum;
    }

    private static bool TryVerifyExtendedScalarSeam(out ulong checksum)
    {
        Type? formatter = Type.GetType("DataVo.Core.Compat.ScalarKeyFormatter, DataVo.Core", throwOnError: false);
        MethodInfo? floatMethod = formatter?.GetMethod(
            "FormatBracketed",
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(float), typeof(IFormatProvider) },
            modifiers: null);
        MethodInfo? decimalMethod = formatter?.GetMethod(
            "FormatBracketed",
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(decimal), typeof(IFormatProvider) },
            modifiers: null);

        if (floatMethod is null || decimalMethod is null)
        {
            checksum = 0;
            return false;
        }

        string floatValue = Assert.IsType<string>(floatMethod.Invoke(
            null,
            new object?[] { 12.5f, CultureInfo.InvariantCulture }));
        string decimalValue = Assert.IsType<string>(decimalMethod.Invoke(
            null,
            new object?[] { 12.5m, CultureInfo.InvariantCulture }));
        Assert.Equal("[12.5]", floatValue);
        Assert.Equal("[12.5]", decimalValue);

        checksum = Mix(Mix(1469598103934665603UL, floatValue), decimalValue);
        return true;
    }

    private static ulong Mix(ulong checksum, uint value) =>
        unchecked((checksum ^ value) * 1099511628211UL);

    private static ulong Mix(ulong checksum, string value)
    {
        for (int i = 0; i < value.Length; i++)
        {
            checksum = unchecked((checksum ^ value[i]) * 1099511628211UL);
        }

        return checksum;
    }

    private static uint ReferenceCrc32C(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (byte value in data)
        {
            crc ^= value;
            for (int bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1u) != 0
                    ? (crc >> 1) ^ 0x82F63B78u
                    : crc >> 1;
            }
        }

        return ~crc;
    }

    private static byte[] CreateCrcPayload()
    {
        var payload = new byte[1_024];
        new Random(0x5A17).NextBytes(payload);
        return payload;
    }

    private static void ForceGc()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    private static string Escape(string value) =>
        value.Replace('|', '_').Replace('\r', ' ').Replace('\n', ' ');

    private readonly record struct MetricResult(
        ulong Checksum,
        double MedianBytesPerOperation,
        double MedianMilliseconds,
        double SpreadMilliseconds);
}
