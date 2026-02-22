using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser;
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
    protected readonly DataVoConfig Config;
    private readonly Guid _session = Guid.NewGuid();

    // Static lock to prevent Catalog/StorageContext stomp between parallel XUnit collections
    protected static readonly object GlobalEngineLock = new object();

    protected SqlExecutionTestsBase(DataVoConfig config, string testDbName)
    {
        Config = config;
        TestDb = testDbName;

        Monitor.Enter(GlobalEngineLock);

        // Initialize the global engine instance to use the test's config
        StorageContext.Initialize(Config);

        // Wipe and recreate catalog slate
        try { Catalog.DropDatabase(TestDb); } catch { }
        if (Config.StorageMode == StorageMode.Disk && Directory.Exists(Config.DiskStoragePath))
        {
            try { Directory.Delete(Config.DiskStoragePath, true); } catch { }
        }

        // Boot Database via raw SQL
        Execute($"CREATE DATABASE {TestDb}");
        Execute($"USE {TestDb}");
    }

    /// <summary>
    /// Executes a raw SQL string through the Lexer/Parser/Evaluator pipeline.
    /// Throws if any statement execution fails.
    /// </summary>
    protected void Execute(string sql)
    {
        var engine = new QueryEngine(sql, _session);
        var results = engine.Parse();

        foreach (var result in results)
        {
            if (result.IsError || result.Messages.Any(m => !m.Contains("Rows affected") && !m.Contains("Rows selected") && !m.Contains("Database") && !m.Contains("Table")))
            {
                var errors = string.Join(", ", result.Messages);
                if (errors.Contains("_PK_"))
                {
                    string xml = System.IO.File.ReadAllText(System.IO.Path.Combine("databases", "Catalog.xml"));
                    throw new Exception($"XML STATE: {xml}");
                }
                throw new Exception($"SQL Execution Failed: {errors}");
            }
        }
    }

    /// <summary>
    /// Executes a raw SQL string and returns the QueryResult object directly for inspection.
    /// </summary>
    protected Core.Contracts.Results.QueryResult ExecuteAndReturn(string sql)
    {
        var engine = new QueryEngine(sql, _session);
        var results = engine.Parse();
        var last = results.LastOrDefault();

        if (last != null && last.IsError)
        {
            var errors = string.Join(", ", last.Messages);
            throw new Exception($"SQL Execution Failed: {errors}");
        }

        return last!;
    }

    public void Dispose()
    {
        try { Catalog.DropDatabase(TestDb); } catch { }
        if (Config.StorageMode == StorageMode.Disk && Directory.Exists(Config.DiskStoragePath))
        {
            try { Directory.Delete(Config.DiskStoragePath, true); } catch { }
        }

        Monitor.Exit(GlobalEngineLock);
    }
}
