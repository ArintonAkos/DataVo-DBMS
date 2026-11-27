using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Benchmarks.Common;

public static class DataVoBenchmarkContext
{
    /// <summary>Opens a benchmark database for <c>--storage in-memory</c> (default) or <c>--storage lsm</c>.</summary>
    /// <param name="storage">The storage name from the command line.</param>
    /// <param name="scenarioName">The scenario name used for the LSM data directory.</param>
    /// <returns>The opened database; it deletes its LSM directory on dispose.</returns>
    /// <exception cref="ArgumentException">The storage name is not <c>in-memory</c> or <c>lsm</c>.</exception>
    public static BenchmarkDatabase Create(string storage, string scenarioName)
    {
        if (storage.Equals("lsm", StringComparison.OrdinalIgnoreCase))
        {
            string path = Path.Combine(Path.GetTempPath(), $"datavo_{scenarioName}_{Guid.NewGuid():N}");
            var context = new DataVoContext(new DataVoConfig
            {
                StorageMode = StorageMode.Lsm,
                DataDirectory = path
            });

            return new BenchmarkDatabase(context, path);
        }

        if (storage.Equals("in-memory", StringComparison.OrdinalIgnoreCase) || storage.Equals("inmemory", StringComparison.OrdinalIgnoreCase))
        {
            return new BenchmarkDatabase(new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory }), null);
        }

        throw new ArgumentException($"Unknown --storage '{storage}'. Use 'in-memory' or 'lsm'.", nameof(storage));
    }

    public static void ExecuteOk(this DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }

    public static IReadOnlyList<Dictionary<string, object?>> Query(this DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }

        return result.Data ?? [];
    }
}

public sealed class BenchmarkDatabase(DataVoContext context, string? diskPath) : IDisposable
{
    public DataVoContext Context { get; } = context;

    public void Dispose()
    {
        Context.Dispose();
        if (string.IsNullOrWhiteSpace(diskPath) || !Directory.Exists(diskPath))
        {
            return;
        }

        try
        {
            Directory.Delete(diskPath, recursive: true);
        }
        catch
        {
            // Best-effort cleanup for benchmark scratch space.
        }
    }
}
