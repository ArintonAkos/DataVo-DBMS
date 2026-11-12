using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// SQL aggregates over a table with a columnar index against the same statements over an identical table
/// without one. The answers must be the same values of the same types, and the statements the index can take
/// must have been answered by it.
/// </summary>
public sealed class ColumnarSqlPushdownTests
{
    private const string Columns = "Id INT PRIMARY KEY, Level INT, Gold BIGINT, Ratio DOUBLE, Speed FLOAT, Active BIT, Name VARCHAR(20), Rank INT";

    /// <summary>Statements the index answers. <c>{t}</c> is the table.</summary>
    public static TheoryData<string> Pushed =>
    [
        "SELECT COUNT(*) FROM {t} WHERE Level >= 50",
        "SELECT COUNT(*) FROM {t}",
        "SELECT COUNT(Gold) FROM {t} WHERE Level < 20",
        "SELECT SUM(Gold) FROM {t} WHERE Level >= 50",
        "SELECT SUM(Level) FROM {t} WHERE Gold > 0",
        "SELECT SUM(Ratio) FROM {t} WHERE Level <> 7",
        "SELECT SUM(Speed) FROM {t} WHERE Level = 7",
        "SELECT AVG(Level) FROM {t} WHERE Level > 10 AND Level <= 90",
        "SELECT AVG(Ratio) FROM {t} WHERE Gold >= -500000 AND Gold < 500000",
        "SELECT MIN(Gold) FROM {t} WHERE Level >= 50",
        "SELECT MAX(Ratio) FROM {t} WHERE Level >= 50",
        "SELECT MIN(Level), MAX(Level), MIN(Speed), MAX(Speed) FROM {t} WHERE Ratio > 0.25",
        "SELECT COUNT(*), SUM(Gold), AVG(Gold), MIN(Ratio), MAX(Gold) FROM {t} WHERE Level >= 50 AND Ratio < 0.75 AND Gold <> 12",
        "SELECT COUNT(*) FROM {t} WHERE Gold IS NULL",
        "SELECT COUNT(*), SUM(Gold) FROM {t} WHERE Gold IS NOT NULL AND Level > 3",
        "SELECT SUM(Gold) FROM {t} WHERE Speed >= 2.5",
        "SELECT COUNT(*) FROM {t} WHERE Ratio <= 0.5",
        "SELECT COUNT(*) FROM {t} WHERE Level > 1000",
    ];

    /// <summary>Statements the index must leave to the row scan, and which must still agree.</summary>
    public static TheoryData<string> NotPushed =>
    [
        "SELECT COUNT(*) FROM {t} WHERE Rank > 5",
        "SELECT SUM(Rank) FROM {t} WHERE Level >= 50",
        "SELECT COUNT(*) FROM {t} WHERE Level > 10.5",
        "SELECT COUNT(*) FROM {t} WHERE Speed > 0.1",
        "SELECT COUNT(*) FROM {t} WHERE Name = 'n7'",
        "SELECT Level, COUNT(*) FROM {t} WHERE Gold > 0 GROUP BY Level",
        "SELECT COUNT(*) FROM {t} WHERE Level >= 50 OR Gold > 0",
        "SELECT COUNT(*) FROM {t} WHERE Id = 77",
    ];

    [Theory]
    [MemberData(nameof(Pushed))]
    public void AnAggregateTheIndexCovers_IsAnsweredByIt_WithTheRowScansResult(string sql)
    {
        using DataVoContext db = Open(rows: 10_000);
        ColumnarIndex index = db.Engine.Columnar.Find("S", "indexed")!;
        long before = index.Scans;

        QueryResult fromIndex = Run(db, sql.Replace("{t}", "indexed"));
        QueryResult fromRows = Run(db, sql.Replace("{t}", "plain"));

        Assert.Equal(1, index.Scans - before);
        AssertSame(fromRows, fromIndex);
    }

    [Theory]
    [MemberData(nameof(NotPushed))]
    public void AnAggregateTheIndexCannotAnswerExactly_ReadsRows(string sql)
    {
        using DataVoContext db = Open(rows: 3_000);
        ColumnarIndex index = db.Engine.Columnar.Find("S", "indexed")!;
        long before = index.Scans;

        QueryResult fromIndexed = Run(db, sql.Replace("{t}", "indexed"));
        QueryResult fromRows = Run(db, sql.Replace("{t}", "plain"));

        Assert.Equal(0, index.Scans - before);
        AssertSame(fromRows, fromIndexed);
    }

    [Fact]
    public void PushedAggregates_FollowDeletesUpdatesAndTransactions()
    {
        using DataVoContext db = Open(rows: 6_000);
        string[] statements =
        [
            "SELECT COUNT(*), SUM(Gold), MIN(Level), MAX(Ratio), AVG(Speed) FROM {t} WHERE Level >= 30",
            "SELECT COUNT(Gold), SUM(Ratio) FROM {t} WHERE Gold IS NOT NULL",
        ];

        void Both(string sql)
        {
            Run(db, sql.Replace("{t}", "indexed"));
            Run(db, sql.Replace("{t}", "plain"));
        }

        void Agree()
        {
            foreach (string sql in statements)
            {
                AssertSame(Run(db, sql.Replace("{t}", "plain")), Run(db, sql.Replace("{t}", "indexed")));
            }
        }

        Agree();
        Both("DELETE FROM {t} WHERE Id >= 4000 AND Id < 4200");
        Both("UPDATE {t} SET Gold = 77, Level = 99 WHERE Id = 15");
        Both("UPDATE {t} SET Gold = NULL WHERE Id = 16");
        Both("INSERT INTO {t} (Id, Level, Gold, Ratio, Speed, Active, Name, Rank) VALUES (90001, 31, 5, 0.5, 1.5, true, 'x', 1)");
        Agree();

        Run(db, "BEGIN");
        Both("INSERT INTO {t} (Id, Level, Gold, Ratio, Speed, Active, Name, Rank) VALUES (90002, 99, 123456789, 0.25, 4.5, true, 'y', 1)");
        Both("DELETE FROM {t} WHERE Id = 20");
        const string inTransaction = "SELECT COUNT(*) FROM {t} WHERE Level >= 30";
        AssertSame(Run(db, inTransaction.Replace("{t}", "plain")), Run(db, inTransaction.Replace("{t}", "indexed")));
        Run(db, "ROLLBACK");
        Agree();
    }

    /// <summary>
    /// Verifies that SQL aggregate queries over an LSM file-backed table with a columnar index
    /// produce identical results to the row-scan baseline, while being answered directly from the columnar index.
    /// </summary>
    [Fact]
    public void AnAggregateTheIndexCovers_OnLsmStorage_IsAnsweredByIt_WithTheRowScansResult()
    {
        using var scratch = new Scratch();
        using DataVoContext db = Open(rows: 5_000, StorageMode.Lsm, scratch.Path);
        ColumnarIndex index = Assert.IsType<ColumnarIndex>(db.Engine.Columnar.Find("S", "indexed"));
        long before = index.Scans;

        string[] statements =
        [
            "SELECT COUNT(*) FROM {t} WHERE Level >= 50",
            "SELECT SUM(Gold) FROM {t} WHERE Level >= 50",
            "SELECT MIN(Gold) FROM {t} WHERE Level >= 50",
            "SELECT MAX(Ratio) FROM {t} WHERE Level >= 50",
            "SELECT AVG(Level) FROM {t} WHERE Level > 10 AND Level <= 90",
        ];

        foreach (string sql in statements)
        {
            QueryResult fromIndex = Run(db, sql.Replace("{t}", "indexed"));
            QueryResult fromRows = Run(db, sql.Replace("{t}", "plain"));
            AssertSame(fromRows, fromIndex);
        }

        Assert.Equal(statements.Length, index.Scans - before);
    }

    [Fact]
    public void ASumPastTheRangeOfBigInt_IsLeftToTheRowScan()
    {
        using DataVoContext db = Open(rows: 10);
        foreach (string table in new[] { "indexed", "plain" })
        {
            Run(db, $"INSERT INTO {table} (Id, Level, Gold, Ratio, Speed, Active, Name, Rank) VALUES (501, 1, {long.MaxValue - 5}, 0, 0, true, 'a', 1)");
            Run(db, $"INSERT INTO {table} (Id, Level, Gold, Ratio, Speed, Active, Name, Rank) VALUES (502, 1, {long.MaxValue - 5}, 0, 0, true, 'b', 1)");
        }

        AssertSame(Run(db, "SELECT SUM(Gold) FROM plain WHERE Id > 0"), Run(db, "SELECT SUM(Gold) FROM indexed WHERE Id > 0"));
        AssertSame(Run(db, "SELECT SUM(Gold) FROM plain"), Run(db, "SELECT SUM(Gold) FROM indexed"));
    }

    [Fact]
    public void APreparedAggregate_GivesTheRowScansResult()
    {
        using DataVoContext db = Open(rows: 5_000);
        DataVoPreparedStatement indexed = db.Prepare("SELECT COUNT(*), SUM(Gold), MAX(Ratio) FROM indexed WHERE Level >= @min");
        DataVoPreparedStatement plain = db.Prepare("SELECT COUNT(*), SUM(Gold), MAX(Ratio) FROM plain WHERE Level >= @min");

        foreach (int min in new[] { 0, 50, 99, 1000 })
        {
            AssertSame(plain.Execute(new DataVoQueryParameter("min", min)), indexed.Execute(new DataVoQueryParameter("min", min)));
        }
    }

    private static void AssertSame(QueryResult expected, QueryResult actual)
    {
        Assert.Equal(expected.Fields, actual.Fields);
        Assert.Equal(expected.Data.Count, actual.Data.Count);
        for (int row = 0; row < expected.Data.Count; row++)
        {
            foreach (string field in expected.Fields)
            {
                object? want = expected.Data[row][field];
                object? got = actual.Data[row][field];
                Assert.Equal(want?.GetType(), got?.GetType());
                if (want is double number)
                {
                    Assert.Equal(BitConverter.DoubleToInt64Bits(number), BitConverter.DoubleToInt64Bits((double)got!));
                }
                else
                {
                    Assert.Equal(want, got);
                }
            }
        }
    }

    private static QueryResult Run(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join(" | ", result.Messages));
        return result;
    }

    private static DataVoContext Open(int rows, StorageMode mode = StorageMode.InMemory, string? path = null)
    {
        var config = new DataVoConfig { StorageMode = mode };
        if (path is not null)
        {
            config.DiskStoragePath = path;
        }

        var db = new DataVoContext(config);
        Run(db, "CREATE DATABASE S");
        Run(db, "USE S");
        Run(db, $"CREATE TABLE indexed ({Columns})");
        Run(db, $"CREATE TABLE plain ({Columns})");
        Run(db, "CREATE INDEX cx ON indexed (Level, Gold, Ratio, Speed, Active) USING COLUMNAR");
        foreach (string table in new[] { "indexed", "plain" })
        {
            for (int first = 1; first <= rows; first += 500)
            {
                var values = new List<string>();
                for (int i = first; i < first + 500 && i <= rows; i++)
                {
                    uint mixed = unchecked((uint)i * 2654435761u);
                    string gold = i % 13 == 0 ? "NULL" : ((long)(mixed % 2_000_001) - 1_000_000).ToString(CultureInfo.InvariantCulture);
                    string ratio = ((mixed % 10_007) / 10_007.0).ToString("R", CultureInfo.InvariantCulture);
                    string speed = ((mixed % 21) * 0.25f).ToString("R", CultureInfo.InvariantCulture);
                    values.Add($"({i}, {mixed % 100}, {gold}, {ratio}, {speed}, {(mixed % 2 == 0 ? "true" : "false")}, 'n{i}', {i % 11})");
                }

                Run(db, $"INSERT INTO {table} (Id, Level, Gold, Ratio, Speed, Active, Name, Rank) VALUES {string.Join(", ", values)}");
            }
        }

        return db;
    }

    /// <summary>
    /// Temporary directory helper for isolating LSM storage tests on disk.
    /// Cleans up directory upon disposal.
    /// </summary>
    private sealed class Scratch : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"datavo_sql_columnar_{Guid.NewGuid():N}");

        public void Dispose()
        {
            if (Directory.Exists(Path))
            {
                Directory.Delete(Path, recursive: true);
            }
        }
    }
}
