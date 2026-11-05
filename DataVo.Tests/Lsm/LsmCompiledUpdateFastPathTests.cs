using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Lsm;

/// <summary>
/// The compiled fixed-width UPDATE patches the row in place in LSM mode (same row id, no index change).
/// Once indexes were persisted the fast path's gate (a heap primary-key lane) never passed in LSM mode,
/// so every compiled UPDATE silently took the generic delete-and-reinsert path.
/// </summary>
public sealed class LsmCompiledUpdateFastPathTests : IDisposable
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value");
    private static readonly DataVoCompiledQueryPlan Plan = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string> { ["Value"] = "value" },
        whereColumn: "Id",
        whereParameterName: "id");

    private readonly string _root = TestStorageDirectories.Create("lsm_compiled_update");

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
    public void FixedWidthUpdate_PatchesInPlace_AndAllocatesLittle()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _root, LsmStrictFsync = false });
        Assert.False(context.Execute("CREATE DATABASE Z")[0].IsError);
        Assert.False(context.Execute("USE Z")[0].IsError);
        Assert.False(context.Execute("CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(20), Value INT)")[0].IsError);
        context.InsertTypedBatch("Records", Schema, [.. Enumerable.Range(1, 100).Select(i => new[] { CellValue.From(i), CellValue.From($"n{i}"), CellValue.From(0) })]);
        long rowIdBefore = RowIdOf(context, 42);

        for (int i = 0; i < 200; i++)
        {
            Assert.Equal(1, Update(context, (i % 100) + 1, i));
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
        {
            Update(context, (i % 100) + 1, i);
        }

        long perUpdate = (GC.GetAllocatedBytesForCurrentThread() - before) / 1_000;

        Assert.Equal(941, ValueOf(context, 42)); // Id 42 is written when i % 100 == 41; the last such i is 941
        Assert.Equal(rowIdBefore, RowIdOf(context, 42)); // patched in place, not re-inserted
        // Coarse backstop (measured ~1.2 KB: primary-key index lookup, row copy-on-write, commit frame);
        // the row-id check above is what tells the in-place patch from the delete-and-reinsert path.
        Assert.True(perUpdate < 2_048, $"compiled fixed-width UPDATE allocated {perUpdate} B/update");
    }

    private static int Update(DataVoContext context, int id, int value) =>
        DataVoCompiledQuery.UpdateFixedWidthByPrimaryKey(context, Plan, DataVoFixedWidthValue.From(id), [DataVoFixedWidthValue.From(value)]);

    private static int ValueOf(DataVoContext context, int id) =>
        Convert.ToInt32(context.Execute($"SELECT Value FROM Records WHERE Id = {id}")[0].Data[0]["Value"]);

    private static long RowIdOf(DataVoContext context, int id) =>
        context.Engine.IndexManager.FilterUsingIndex($"[{id}]", "_PK_Records", "Records", "Z")[0];
}
