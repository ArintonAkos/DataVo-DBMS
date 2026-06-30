using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.Whitepaper;

public interface IWhitepaperBenchmarkEngine : IDisposable
{
    string Name { get; }

    string WorkingDirectory { get; }

    void Initialize(string workingDirectory, bool fresh);

    void Preload(int records);

    FlatRecord? Read(long id);

    void Update(long id, int newValue, double newScore);

    void CloseForRecovery();

    void OpenExisting();
}
