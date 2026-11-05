using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// SQL equality predicates on indexed columns must return exactly what a full scan returns. The
/// lookups used to miss every secondary index (key format mismatch) and silently fall back to a scan;
/// now an index miss is final where the index and the scan agree, so these cases pin the semantics.
/// </summary>
public sealed class SqlIndexLookupTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(Modes))]
    public void EqualityOnIndexedColumns_MatchesFullScanSemantics(StorageMode mode)
    {
        WithTable(mode, context =>
        {
            Assert.Equal([2], Ids(context, "SELECT Id FROM T WHERE Code = 102"));
            Assert.Empty(Ids(context, "SELECT Id FROM T WHERE Code = 999"));          // absent value
            Assert.Equal([2], Ids(context, "SELECT Id FROM T WHERE Code = 102.0"));    // 102.0 = INT 102
            Assert.Empty(Ids(context, "SELECT Id FROM T WHERE Code = 102.5"));        // equals no INT
            Assert.Equal([2], Ids(context, "SELECT Id FROM T WHERE Code = '102'"));    // string literal: scan coercion

            Assert.Equal([3], Ids(context, "SELECT Id FROM T WHERE Score = 1.25"));
            Assert.Equal([1], Ids(context, "SELECT Id FROM T WHERE Score = 2"));       // INT literal on FLOAT
            Assert.Empty(Ids(context, "SELECT Id FROM T WHERE Score = 7.75"));

            Assert.Equal([1], Ids(context, "SELECT Id FROM T WHERE Name = 'alpha'"));
            Assert.Empty(Ids(context, "SELECT Id FROM T WHERE Name = 'ALPHA'"));      // ordinal, like the scan
            Assert.Empty(Ids(context, "SELECT Id FROM T WHERE Name = 'nobody'"));
            Assert.Equal([4], Ids(context, "SELECT Id FROM T WHERE Name = '007'"));    // numeric text keeps the scan

            Assert.Equal([3], Ids(context, "SELECT Id FROM T WHERE Id = 3"));
            Assert.Empty(Ids(context, "SELECT Id FROM T WHERE Id = 3000"));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CompositeIndex_QueriedByOneColumn_StillFindsRows(StorageMode mode)
    {
        WithTable(mode, context =>
        {
            Assert.False(context.Execute("CREATE INDEX ix_pair ON T (GroupId, Code)")[0].IsError);

            Assert.Equal([1, 2], Ids(context, "SELECT Id FROM T WHERE GroupId = 10"));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void IndexedLookup_SeesUpdatesAndDeletes(StorageMode mode)
    {
        WithTable(mode, context =>
        {
            Assert.False(context.Execute("UPDATE T SET Code = 500 WHERE Id = 2")[0].IsError);
            Assert.Empty(Ids(context, "SELECT Id FROM T WHERE Code = 102"));
            Assert.Equal([2], Ids(context, "SELECT Id FROM T WHERE Code = 500"));

            Assert.False(context.Execute("DELETE FROM T WHERE Id = 2")[0].IsError);
            Assert.Empty(Ids(context, "SELECT Id FROM T WHERE Code = 500"));
        });
    }

    private static void WithTable(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_index_lookup") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE Lookup");
        Execute(context, "USE Lookup");
        Execute(context, "CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(20), Code INT, Score FLOAT, GroupId INT)");
        Execute(context, "CREATE INDEX ix_code ON T (Code)");
        Execute(context, "CREATE INDEX ix_name ON T (Name)");
        Execute(context, "CREATE INDEX ix_score ON T (Score)");
        Execute(context, "INSERT INTO T (Id, Name, Code, Score, GroupId) VALUES (1, 'alpha', 101, 2.0, 10), (2, 'beta', 102, 3.5, 10), (3, 'gamma', 103, 1.25, 20), (4, '007', 104, 9.0, 20)");
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        var result = context.Execute(sql);
        Assert.False(result[0].IsError, string.Join(" | ", result[0].Messages));
    }

    private static List<int> Ids(DataVoContext context, string sql)
    {
        var result = context.Execute(sql);
        Assert.False(result[0].IsError, $"{sql}: {string.Join(" | ", result[0].Messages)}");
        return [.. result[0].Data.Select(row => Convert.ToInt32(row["Id"])).Order()];
    }
}
