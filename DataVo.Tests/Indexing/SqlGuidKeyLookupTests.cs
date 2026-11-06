using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Equality on a GUID primary key is answered by the index, including a miss. A miss used to fall back
/// to a full table scan to confirm it ("does this id exist?" read every row), and an upper-case literal
/// matched or not depending on whether the index or the scan answered.
/// </summary>
public sealed class SqlGuidKeyLookupTests : IDisposable
{
    private const int Rows = 20_000;
    private readonly string _root = TestStorageDirectories.Create("sql_guid_lookup");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

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

    [Theory]
    [MemberData(nameof(Modes))]
    public void GuidEquality_FindsRows_AndAnswersMissesWithoutScanning(StorageMode mode)
    {
        using DataVoContext context = mode == StorageMode.Lsm
            ? new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _root, LsmStrictFsync = false })
            : new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Run(context, "CREATE DATABASE G");
        Run(context, "USE G");
        Run(context, "CREATE TABLE Items (Id GUID PRIMARY KEY, Name VARCHAR(20))");
        Guid[] keys = [.. Enumerable.Range(0, Rows).Select(_ => Guid.NewGuid())];
        context.InsertTypedBatch("Items", new ReactiveRowSchema("Id", "Name"), [.. keys.Select(key => new[] { CellValue.From(key), CellValue.From("x") })]);

        Assert.Single(Select(context, $"SELECT Name FROM Items WHERE Id = '{keys[42]}'"));
        Assert.Single(Select(context, $"SELECT Name FROM Items WHERE Id = '{keys[42].ToString().ToUpperInvariant()}'"));
        Assert.Empty(Select(context, "SELECT Name FROM Items WHERE Id = 'not-a-guid'"));

        string missing = $"SELECT Name FROM Items WHERE Id = '{Guid.NewGuid()}'";
        for (int i = 0; i < 50; i++)
        {
            Assert.Empty(Select(context, missing)); // warm up
        }

        // A scan of 20,000 rows allocates megabytes; an index miss allocates a few kilobytes.
        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Empty(Select(context, missing));
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 256 * 1024, $"a missing GUID lookup allocated {allocated:N0} B: it scanned the table");
    }

    private static void Run(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[0];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[0];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return result.Data;
    }
}
