using Research.Benchmark.Runners.Whitepaper;

namespace Research.Benchmark.Tests;

public sealed class WhitepaperEngineMatrixTests
{
    [Fact]
    public void DefaultMatrixContainsRequestedWhitepaperEngines()
    {
        using var engines = new CompositeDisposable(WhitepaperEngineMatrix.Create("all"));

        string[] names = engines.Items.Select(engine => engine.Name).ToArray();

        Assert.Equal(
        [
            "DataVo (LSM Production) [DataVo.Core net10.0]",
            "DataVo (LSM Relaxed) [DataVo.Core net10.0]",
            "SQLite (WAL,normal)",
            "LiteDB",
        ], names);
    }

    private sealed class CompositeDisposable : IDisposable
    {
        public CompositeDisposable(IReadOnlyList<IWhitepaperBenchmarkEngine> items)
        {
            Items = items;
        }

        public IReadOnlyList<IWhitepaperBenchmarkEngine> Items { get; }

        public void Dispose()
        {
            foreach (IWhitepaperBenchmarkEngine item in Items)
            {
                item.Dispose();
            }
        }
    }
}
