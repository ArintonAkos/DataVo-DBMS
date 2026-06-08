using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Benchmarks.Common;

public static class DataVoBenchmarkContext
{
    public static BenchmarkDatabase Create(string storage, string scenarioName)
    {
        if (storage.Equals("disk", StringComparison.OrdinalIgnoreCase))
        {
            string path = Path.Combine(Path.GetTempPath(), $"datavo_{scenarioName}_{Guid.NewGuid():N}");
            var context = new DataVoContext(new DataVoConfig
            {
                StorageMode = StorageMode.Disk,
                DiskStoragePath = path
            });

            return new BenchmarkDatabase(context, path);
        }

        return new BenchmarkDatabase(new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory }), null);
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
