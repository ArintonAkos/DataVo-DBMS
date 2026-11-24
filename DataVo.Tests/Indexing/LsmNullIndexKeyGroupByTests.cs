using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// A persisted (LSM) index holds the rows whose key is NULL, whichever path wrote them: single-row, multi-row and
/// prepared INSERT, UPDATE to and from NULL (SQL and prepared), DELETE (SQL and prepared), a committed or rolled-back
/// transaction, CREATE INDEX over existing rows, VACUUM, ALTER TABLE and a reopen. After each step a counting
/// GROUP BY on the indexed column is answered from the index alone (no table scan) and equals the hash plan over an
/// unindexed copy of the column.
/// </summary>
public sealed class LsmNullIndexKeyGroupByTests
{
    private const string Grouped = "SELECT {c}, COUNT(*) AS N, COUNT({c}) AS K FROM O GROUP BY {c} ORDER BY {c}";

    [Fact]
    public void NullGroup_ComesFromTheIndex_AfterEveryWritePath()
    {
        string directory = TestStorageDirectories.Create("lsm_null_index_key");
        using (DataVoContext db = Open(directory, create: true))
        {
            Run(db, "CREATE TABLE O (Id INT PRIMARY KEY, CustomerId INT, Tag INT)");
            Run(db, "INSERT INTO O (Id, CustomerId, Tag) VALUES (1, NULL, NULL), (2, 1, 1), (3, NULL, NULL)");
            Run(db, "CREATE INDEX ix_customer ON O (CustomerId)");
            AssertIndexAnswers(db, expectedNulls: 2);

            Run(db, "INSERT INTO O (Id, CustomerId, Tag) VALUES (4, NULL, NULL)");
            Run(db, "INSERT INTO O (Id, CustomerId, Tag) VALUES (5, NULL, NULL), (6, 2, 2), (7, NULL, NULL)");
            AssertIndexAnswers(db, expectedNulls: 5);

            DataVoPreparedStatement insert = db.Prepare("INSERT INTO O (Id, CustomerId, Tag) VALUES (@id, @c, @c)");
            insert.Execute(DataVoQueryParameter.FromObject("id", 8), DataVoQueryParameter.FromObject("c", null));
            insert.Execute(DataVoQueryParameter.FromObject("id", 9), DataVoQueryParameter.FromObject("c", 3));
            AssertIndexAnswers(db, expectedNulls: 6);

            Run(db, "UPDATE O SET CustomerId = 4, Tag = 4 WHERE Id = 1");
            Run(db, "UPDATE O SET CustomerId = NULL, Tag = NULL WHERE Id = 2");
            Run(db, "UPDATE O SET Tag = NULL WHERE Id = 3"); // stays NULL-keyed in place
            AssertIndexAnswers(db, expectedNulls: 6);

            DataVoPreparedStatement update = db.Prepare("UPDATE O SET CustomerId = @c, Tag = @c WHERE Id = @id");
            update.Execute(DataVoQueryParameter.FromObject("c", 5), DataVoQueryParameter.FromObject("id", 4));
            update.Execute(DataVoQueryParameter.FromObject("c", null), DataVoQueryParameter.FromObject("id", 9));
            AssertIndexAnswers(db, expectedNulls: 6);

            Run(db, "DELETE FROM O WHERE Id = 5");
            db.Prepare("DELETE FROM O WHERE Id = @id").Execute(DataVoQueryParameter.FromObject("id", 7));
            AssertIndexAnswers(db, expectedNulls: 4);

            Run(db, "BEGIN");
            Run(db, "INSERT INTO O (Id, CustomerId, Tag) VALUES (10, NULL, NULL)");
            Run(db, "UPDATE O SET CustomerId = 6, Tag = 6 WHERE Id = 3");
            Run(db, "DELETE FROM O WHERE Id = 8");
            Run(db, "COMMIT");
            AssertIndexAnswers(db, expectedNulls: 3);

            Run(db, "BEGIN");
            Run(db, "INSERT INTO O (Id, CustomerId, Tag) VALUES (11, NULL, NULL)");
            Run(db, "UPDATE O SET CustomerId = NULL, Tag = NULL WHERE Id = 6");
            Run(db, "ROLLBACK");
            AssertIndexAnswers(db, expectedNulls: 3);

            Run(db, "VACUUM O");
            AssertIndexAnswers(db, expectedNulls: 3);

            Run(db, "ALTER TABLE O ADD COLUMN Note VARCHAR(10)");
            AssertIndexAnswers(db, expectedNulls: 3);
        }

        using (DataVoContext reopened = Open(directory, create: false))
        {
            AssertIndexAnswers(reopened, expectedNulls: 3);
            Run(reopened, "INSERT INTO O (Id, CustomerId, Tag) VALUES (12, NULL, NULL)");
            AssertIndexAnswers(reopened, expectedNulls: 4);
        }
    }

    [Fact]
    public void NullKeyEntries_NeverMatchAnEqualityOrRangeLookup()
    {
        using DataVoContext db = Open(TestStorageDirectories.Create("lsm_null_index_lookup"), create: true);
        Run(db, "CREATE TABLE T (Id INT PRIMARY KEY, Code INT, Name VARCHAR(10))");
        Run(db, "CREATE INDEX ix_code ON T (Code)");
        Run(db, "CREATE INDEX ix_name ON T (Name)");
        Run(db, "INSERT INTO T (Id, Code, Name) VALUES (1, NULL, NULL), (2, 0, ''), (3, 1, 'a')");

        Assert.Equal(["2"], Ids(Run(db, "SELECT Id FROM T WHERE Code = 0")));
        Assert.Equal(["2"], Ids(Run(db, "SELECT Id FROM T WHERE Name = ''")));
        Assert.Equal(["2", "3"], Ids(Run(db, "SELECT Id FROM T WHERE Code >= 0 ORDER BY Id")));
        Assert.Equal(["1"], Ids(Run(db, "SELECT Id FROM T WHERE Code IS NULL")));
    }

    /// <summary>
    /// The index plan answers without a table scan and agrees with the hash plan over <c>Tag</c>, which always holds
    /// the same value as <c>CustomerId</c>.
    /// </summary>
    private static void AssertIndexAnswers(DataVoContext db, long expectedNulls)
    {
        List<string> expected = Rows(Run(db, Grouped.Replace("{c}", "Tag")), "Tag");
        db.Diagnostics.Enabled = true;
        QueryResult actual = Run(db, Grouped.Replace("{c}", "CustomerId"));
        var stats = db.Diagnostics.LastQuery!;
        db.Diagnostics.Enabled = false;

        Assert.Equal(expected, Rows(actual, "CustomerId"));
        Assert.Contains("ix_customer", stats.IndexesUsed, StringComparer.OrdinalIgnoreCase);
        Assert.False(stats.FullTableScan, "the NULL group must come from the index, not a scan");
        long nulls = actual.Data.Where(row => row["CustomerId"] is null).Select(row => (long)row["N"]!).SingleOrDefault();
        Assert.Equal(expectedNulls, nulls);
    }

    private static List<string> Rows(QueryResult result, string column) =>
        [.. result.Data.Select(row => string.Join("|", row.Select(pair =>
            $"{(pair.Key == column ? "C" : pair.Key)}={Convert.ToString(pair.Value, CultureInfo.InvariantCulture) ?? "NULL"}")))];

    private static List<string> Ids(QueryResult result) =>
        [.. result.Data.Select(row => Convert.ToString(row["Id"], CultureInfo.InvariantCulture)!)];

    private static DataVoContext Open(string directory, bool create)
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory, LsmStrictFsync = false });
        if (create)
        {
            Run(db, "CREATE DATABASE G");
        }

        Run(db, "USE G");
        return db;
    }

    private static QueryResult Run(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }
}
