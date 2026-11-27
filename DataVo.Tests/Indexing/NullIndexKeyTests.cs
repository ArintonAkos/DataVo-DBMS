using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// NULL keys are never indexed, whichever path wrote the row (SQL insert, multi-row insert, UPDATE, COMMIT) or built
/// the index (CREATE INDEX over existing rows, VACUUM, ALTER TABLE): a NULL never matches an empty string, NULLs never
/// clash in a UNIQUE column, and IS NULL / equality lookups answer the same before and after the index exists.
/// </summary>
public sealed class NullIndexKeyTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_nullkey_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NullKeys_AreNeverIndexed_OnAnyPath(StorageMode mode)
    {
        using var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.InMemory ? null! : _directory,
            LsmStrictFsync = false,
        });
        Run(db, "CREATE DATABASE Nk");
        Run(db, "USE Nk");
        Run(db, "CREATE TABLE T (Id INT PRIMARY KEY, Tag VARCHAR(10), Code VARCHAR(10) UNIQUE)");

        // Rows written before the index exists: CREATE INDEX builds it from them.
        Run(db, "INSERT INTO T (Id, Tag, Code) VALUES (1, NULL, NULL)");
        Run(db, "INSERT INTO T (Id, Tag, Code) VALUES (2, '', 'a')");
        Run(db, "CREATE INDEX ix_tag ON T (Tag)");

        // Rows written after: single-row, multi-row, UPDATE to and from NULL, and a committed transaction.
        Run(db, "INSERT INTO T (Id, Tag, Code) VALUES (3, NULL, NULL)");
        Run(db, "INSERT INTO T (Id, Tag, Code) VALUES (4, NULL, NULL), (5, 'x', 'b')");
        Run(db, "UPDATE T SET Tag = NULL WHERE Id = 5");
        Run(db, "UPDATE T SET Tag = 'y' WHERE Id = 4");
        Run(db, "BEGIN");
        Run(db, "INSERT INTO T (Id, Tag, Code) VALUES (6, NULL, NULL)");
        Run(db, "UPDATE T SET Tag = NULL WHERE Id = 2");
        Run(db, "COMMIT");
        Run(db, "UPDATE T SET Tag = '' WHERE Id = 2");

        AssertLookups(db);

        // Rebuilds: VACUUM and an ALTER TABLE that rewrites rows rebuild every index from the rows.
        Run(db, "VACUUM T");
        AssertLookups(db);
        Run(db, "ALTER TABLE T ADD COLUMN Extra INT");
        AssertLookups(db);
    }

    private static void AssertLookups(DataVoContext db)
    {
        Assert.Equal(new[] { 2 }, Ids(db, "SELECT Id FROM T WHERE Tag = ''"));
        Assert.Equal(new[] { 1, 3, 5, 6 }, Ids(db, "SELECT Id FROM T WHERE Tag IS NULL"));
        Assert.Equal(new[] { 4 }, Ids(db, "SELECT Id FROM T WHERE Tag = 'y'"));
        Assert.Equal(new[] { 1, 3, 4, 6 }, Ids(db, "SELECT Id FROM T WHERE Code IS NULL"));
        Assert.Equal(4L, Assert.Single(Run(db, "SELECT COUNT(*) AS C FROM T WHERE Tag IS NULL").Data)["C"]);
    }

    private static int[] Ids(DataVoContext db, string sql) =>
        [.. Run(db, sql).Data.Select(row => (int)row["Id"]!).Order()];

    private static QueryResult Run(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
