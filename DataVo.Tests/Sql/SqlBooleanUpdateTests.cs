using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>An UPDATE that assigns the literals TRUE and FALSE, which the parser reads as names.</summary>
public sealed class SqlBooleanUpdateTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Update_AssignsBooleanLiterals(StorageMode mode)
    {
        using var scratch = new Scratch();
        using DataVoContext db = Open(mode, scratch.Path);
        Run(db, "CREATE TABLE flags (Id INT PRIMARY KEY, Alive BIT NOT NULL, Seen BIT)");
        Run(db, "INSERT INTO flags (Id, Alive, Seen) VALUES (1, true, false), (2, false, true), (3, true, NULL)");

        Run(db, "UPDATE flags SET Alive = false WHERE Id = 1");
        Run(db, "UPDATE flags SET Alive = true, Seen = false WHERE Id = 2");
        Run(db, "UPDATE flags SET Seen = TRUE WHERE Id = 3");

        QueryResult rows = Run(db, "SELECT Id, Alive, Seen FROM flags ORDER BY Id");
        Assert.Equal([false, true, true], rows.Data.Select(static row => (bool)row["Alive"]!));
        Assert.Equal([false, false, true], rows.Data.Select(static row => (bool)row["Seen"]!));
    }

    private static QueryResult Run(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join(" | ", result.Messages));
        return result;
    }

    private static DataVoContext Open(StorageMode mode, string directory)
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Run(db, "CREATE DATABASE S");
        Run(db, "USE S");
        return db;
    }

    private sealed class Scratch : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"datavo_sqlfix_{Guid.NewGuid():N}");

        public void Dispose()
        {
            if (Directory.Exists(Path))
            {
                Directory.Delete(Path, recursive: true);
            }
        }
    }
}
