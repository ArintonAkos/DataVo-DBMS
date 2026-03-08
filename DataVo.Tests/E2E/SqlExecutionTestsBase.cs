using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

/// <summary>
/// The abstract foundation for all SQL E2E tests. 
/// It encapsulates the raw SQL-to-Storage pipeline and guarantees tests run without race conditions.
/// Classes that inherit from this will define specific StorageModes (Memory/Disk) to multiplex test execution.
/// </summary>
public abstract class SqlExecutionTestsBase : IDisposable
{
    protected readonly string TestDb;
    protected DataVoConfig Config;
    protected DataVoEngine Engine;
    private readonly Guid _session = Guid.NewGuid();

    protected SqlExecutionTestsBase(DataVoConfig config, string testDbName)
    {
        Config = CreateIsolatedConfig(config);
        TestDb = $"{testDbName}_{Guid.NewGuid():N}";
        Engine = DataVoEngine.Initialize(Config);

        // Boot Database via raw SQL
        Execute($"CREATE DATABASE {TestDb}");
        Execute($"USE {TestDb}");
    }

    protected void ReinitializeEngine(DataVoConfig config)
    {
        Engine.Dispose();
        Config = CloneConfig(config);
        Engine = DataVoEngine.Initialize(Config);
    }

    /// <summary>
    /// Executes a raw SQL string through the Lexer/Parser/Evaluator pipeline.
    /// Throws if any statement execution fails.
    /// </summary>
    protected void Execute(string sql)
    {
        var engine = new QueryEngine(sql, _session, Engine);
        var results = engine.Parse();

        foreach (var result in results)
        {
            if (result.IsError || result.Messages.Any(m => !m.Contains("Rows affected") && !m.Contains("Rows selected") && !m.Contains("Database") && !m.Contains("Table") && !m.Contains("VACUUM") && !m.Contains("Transaction")))
            {
                var errors = string.Join(", ", result.Messages);
                throw new Exception($"SQL Execution Failed:\n{errors}");
            }
        }
    }

    protected Core.Contracts.Results.QueryResult ExecuteAndReturn(string sql)
    {
        var engine = new QueryEngine(sql, _session, Engine);
        var results = engine.Parse();
        var last = results.LastOrDefault();

        return last!;
    }

    public void Dispose()
    {
        CleanupResources();
    }

    private void CleanupResources()
    {
        try { Engine.Catalog.DropDatabase(TestDb); } catch { }
        try { Engine.IndexManager.DropDatabaseIndexes(TestDb); } catch { }
        try { Engine.Dispose(); } catch { }

        if (Config.StorageMode == StorageMode.Disk && !string.IsNullOrWhiteSpace(Config.DiskStoragePath) && Directory.Exists(Config.DiskStoragePath))
        {
            try { Directory.Delete(Config.DiskStoragePath, true); } catch { }
        }

        string walPath = Config.ResolveWalFilePath();
        if (File.Exists(walPath))
        {
            try { File.Delete(walPath); } catch { }
        }
    }

    private static DataVoConfig CreateIsolatedConfig(DataVoConfig config)
    {
        string? diskPath = config.StorageMode == StorageMode.Disk
            ? Path.Combine(config.DiskStoragePath ?? Path.Combine(Path.GetTempPath(), "datavo_tests"), Guid.NewGuid().ToString("N"))
            : null;

        return new DataVoConfig
        {
            StorageMode = config.StorageMode,
            DiskStoragePath = diskPath,
            WalEnabled = config.WalEnabled,
            WalFilePath = config.WalFilePath,
            WalCheckpointThreshold = config.WalCheckpointThreshold,
        };
    }

    private static DataVoConfig CloneConfig(DataVoConfig config)
    {
        return new DataVoConfig
        {
            StorageMode = config.StorageMode,
            DiskStoragePath = config.DiskStoragePath,
            WalEnabled = config.WalEnabled,
            WalFilePath = config.WalFilePath,
            WalCheckpointThreshold = config.WalCheckpointThreshold,
        };
    }
}
