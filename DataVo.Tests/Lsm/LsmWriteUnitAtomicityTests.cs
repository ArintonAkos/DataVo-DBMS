using DataVo.Core;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Every write API must commit a row and its persisted index entries in one commit-log frame. Only SQL
/// statements used to open a write batch; the typed APIs committed each storage call on its own, which
/// split a row from its index entries across frames (a crash in between kept a row without its index
/// entries) and made typed inserts 18x slower once indexes were persisted.
/// </summary>
public sealed class LsmWriteUnitAtomicityTests : IDisposable
{
    private readonly string _root = TestStorageDirectories.Create("lsm_write_units");

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public void TypedBatchInsert_IsOneCommit()
    {
        using DataVoContext context = OpenWithTable();
        int commits = CountCommits(() => context.InsertTypedBatch(
            "Items",
            new ReactiveRowSchema("Id", "Code"),
            [[CellValue.From(1), CellValue.From(10)], [CellValue.From(2), CellValue.From(20)], [CellValue.From(3), CellValue.From(10)]]));

        Assert.Equal(1, commits);
        Assert.Equal([1, 3], Ids(context, "SELECT Id FROM Items WHERE Code = 10"));
    }

    [Fact]
    public void TypedSingleInsert_IsOneCommit()
    {
        using DataVoContext context = OpenWithTable();
        int commits = CountCommits(() => context.InsertTyped("Items", new ReactiveRowSchema("Id", "Code"), [CellValue.From(7), CellValue.From(70)]));

        Assert.Equal(1, commits);
        Assert.Equal([7], Ids(context, "SELECT Id FROM Items WHERE Code = 70"));
    }

    [Fact]
    public void DictionaryBulkInsert_IsOneCommit()
    {
        using DataVoContext context = OpenWithTable();
        int commits = CountCommits(() => context.BulkInsert(
            "Items",
            [new Dictionary<string, object?> { ["Id"] = 1, ["Code"] = 5 }, new Dictionary<string, object?> { ["Id"] = 2, ["Code"] = 5 }]));

        Assert.Equal(1, commits);
        Assert.Equal([1, 2], Ids(context, "SELECT Id FROM Items WHERE Code = 5"));
    }

    private DataVoContext OpenWithTable()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _root, LsmStrictFsync = false });
        Assert.False(context.Execute("CREATE DATABASE Shop")[0].IsError);
        Assert.False(context.Execute("USE Shop")[0].IsError);
        Assert.False(context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Code INT)")[0].IsError);
        Assert.False(context.Execute("CREATE INDEX ix_code ON Items (Code)")[0].IsError);
        return context;
    }

    private static int CountCommits(Action write)
    {
        var engine = DataVoEngine.Current().StorageContext.LsmEngine!;
        int commits = 0;
        engine.BeforePublishForTesting = _ => Interlocked.Increment(ref commits);
        try
        {
            write();
        }
        finally
        {
            engine.BeforePublishForTesting = null;
        }

        return commits;
    }

    private static List<int> Ids(DataVoContext context, string sql)
    {
        var result = context.Execute(sql);
        Assert.False(result[0].IsError, string.Join(" | ", result[0].Messages));
        return [.. result[0].Data.Select(row => Convert.ToInt32(row["Id"])).Order()];
    }
}
