using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

/// <summary>
/// The typed batch insert API with a GUID primary key. It treated every single-column key as an
/// integer and rejected GUID keys with "must be INT or BIGINT", so GUID tables lost the fast path.
/// </summary>
public sealed class TypedGuidPrimaryKeyInsertTests : IDisposable
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name");
    private readonly string _root = TestStorageDirectories.Create("typed_guid_pk");

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
    public void TypedBatch_InsertsGuidKeys_AndRejectsDuplicates(StorageMode mode)
    {
        Guid[] keys = [.. Enumerable.Range(0, 500).Select(_ => Guid.CreateVersion7())];
        using DataVoContext context = Open(mode);
        Run(context, "CREATE DATABASE G");
        Run(context, "USE G");
        Run(context, "CREATE TABLE Items (Id GUID PRIMARY KEY, Name VARCHAR(20))");

        context.InsertTypedBatch("Items", Schema, [.. keys.Select((key, i) => new[] { CellValue.From(key), CellValue.From($"n{i}") })]);

        Assert.Equal(500, Count(context, "SELECT Id FROM Items"));
        Assert.Equal(1, Count(context, $"SELECT Name FROM Items WHERE Id = '{keys[123]}'"));

        // A duplicate of an existing key, and a duplicate inside one batch, are both rejected whole.
        Assert.ThrowsAny<Exception>(() => context.InsertTypedBatch("Items", Schema, [[CellValue.From(keys[7]), CellValue.From("dup")]]));
        Guid fresh = Guid.CreateVersion7();
        Assert.ThrowsAny<Exception>(() => context.InsertTypedBatch("Items", Schema, [[CellValue.From(fresh), CellValue.From("a")], [CellValue.From(fresh), CellValue.From("b")]]));
        Assert.Equal(500, Count(context, "SELECT Id FROM Items"));
    }

    [Fact]
    public void TypedGuidKeys_AreServedByThePersistedIndexAfterReopen()
    {
        Guid key = Guid.NewGuid();
        using (DataVoContext context = Open(StorageMode.Lsm))
        {
            Run(context, "CREATE DATABASE G");
            Run(context, "USE G");
            Run(context, "CREATE TABLE Items (Id GUID PRIMARY KEY, Name VARCHAR(20))");
            context.InsertTypedBatch("Items", Schema, [[CellValue.From(key), CellValue.From("kept")]]);
        }

        using DataVoContext reopened = Open(StorageMode.Lsm);
        Run(reopened, "USE G");
        var rows = reopened.Execute($"SELECT Name FROM Items WHERE Id = '{key}'")[0].Data;
        Assert.Equal("kept", Assert.Single(rows)["Name"]?.ToString());
        Assert.ThrowsAny<Exception>(() => reopened.InsertTypedBatch("Items", Schema, [[CellValue.From(key), CellValue.From("dup")]]));
    }

    private DataVoContext Open(StorageMode mode) => mode == StorageMode.Lsm
        ? new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _root, LsmStrictFsync = false })
        : new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

    private static void Run(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[0];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }

    private static int Count(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[0];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return result.Data.Count;
    }
}
