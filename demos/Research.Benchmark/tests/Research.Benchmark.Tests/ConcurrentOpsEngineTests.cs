using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners.ConcurrentOps;

namespace Research.Benchmark.Tests;

public sealed class ConcurrentOpsEngineTests
{
    public static IEnumerable<object[]> Engines()
    {
        yield return [new DataVoConcurrentOpsEngine()];
        yield return [new SqliteConcurrentOpsEngine()];
    }

    [Theory]
    [MemberData(nameof(Engines))]
    public async Task ConcurrentWorkloadReportsReadAndWriteMetrics(IConcurrentOpsEngine engine)
    {
        await using (engine)
        {
            var options = new ConcurrentOpsOptions(
                InitialRecords: 1_000,
                Duration: TimeSpan.FromMilliseconds(250),
                ReaderWorkers: 2,
                WriterWorkers: 1,
                BusyTimeout: TimeSpan.FromSeconds(2));

            await engine.InitializeAsync(options);
            ConcurrentOpsResult result = await engine.RunAsync(options);

            Assert.True(result.ReadOperations > 0, $"{engine.Name} did not report read operations.");
            Assert.True(result.WriteOperations > 0, $"{engine.Name} did not report write operations.");
            Assert.True(result.TotalOperationsPerSecond > 0, $"{engine.Name} did not report positive OPS.");
            Assert.True(double.IsFinite(result.ReadP99LatencyMs), $"{engine.Name} read p99 was not finite.");
            Assert.True(double.IsFinite(result.WriteP99LatencyMs), $"{engine.Name} write p99 was not finite.");
        }
    }
}
