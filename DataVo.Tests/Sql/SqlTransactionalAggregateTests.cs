using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// Aggregates over BIGINT and DOUBLE columns inside a transaction that has written to the table, where the
/// general path answers and uncommitted rows can hold a number in another width than stored rows.
/// </summary>
public sealed class SqlTransactionalAggregateTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Aggregates_InsideATransactionThatWrote_MatchTheSameRowsCommitted(StorageMode mode)
    {
        using var scratch = new Scratch();
        using DataVoContext db = Open(mode, scratch.Path);
        Run(db, "CREATE TABLE scores (Id INT PRIMARY KEY, Level INT, Gold BIGINT, Ratio DOUBLE, Name VARCHAR(20))");
        for (int i = 1; i <= 40; i++)
        {
            Run(db, $"INSERT INTO scores (Id, Level, Gold, Ratio, Name) VALUES ({i}, {i % 10}, {(i % 7 == 0 ? "NULL" : (i * 100L).ToString())}, {i / 8.0}, 'n{i}')");
        }

        const string query = "SELECT COUNT(*), SUM(Gold), AVG(Gold), MIN(Level), MAX(Ratio), MIN(Gold), MAX(Gold), SUM(Ratio) FROM scores WHERE Level >= 3";

        Run(db, "BEGIN");
        Run(db, "INSERT INTO scores (Id, Level, Gold, Ratio, Name) VALUES (900, 9, 123456789, 0.25, 'staged')");
        Run(db, "DELETE FROM scores WHERE Id = 20");
        Run(db, "UPDATE scores SET Gold = 5 WHERE Id = 5");
        QueryResult inside = Run(db, query);
        Run(db, "COMMIT");
        QueryResult after = Run(db, query);

        Assert.Single(inside.Data);
        foreach (string field in after.Fields)
        {
            object? committed = after.Data[0][field];
            object? staged = inside.Data[0][field];
            if (field.StartsWith("COUNT", StringComparison.OrdinalIgnoreCase))
            {
                // COUNT is an int on the general path and a long on the typed one; only the number is compared.
                Assert.Equal(Convert.ToInt64(committed), Convert.ToInt64(staged));
            }
            else
            {
                Assert.Equal(committed, staged);
            }
        }
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
