using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

/// <summary>
/// Coverage for the zero-allocation compiled UPDATE fast path: it byte-patches fixed-width columns,
/// emits a binary <see cref="WalFrameOperationType.Update"/> frame, survives a restart, and falls back
/// to the legacy dictionary path for shapes it cannot handle.
/// </summary>
public sealed class ZeroAllocUpdateTests : IDisposable
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");
    private static readonly DataVoCompiledQueryPlan UpdatePlan = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string> { ["Value"] = "value", ["Score"] = "score" },
        whereColumn: "Id",
        whereParameterName: "id");

    private static readonly DataVoCompiledQueryPlan NameUpdatePlan = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string> { ["Name"] = "name" },
        whereColumn: "Id",
        whereParameterName: "id");

    private readonly string _dir;
    private DataVoContext _context;

    public ZeroAllocUpdateTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "datavo-zeroalloc-update-" + Guid.NewGuid().ToString("N"));
        _context = NewContext();

        ExecOk("CREATE DATABASE Z");
        ExecOk("USE Z");
        ExecOk("CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT)");

        // Seed via the typed insert fast lane (as the benchmark does) so the integer primary-key fast lane
        // is populated — the same precondition the zero-alloc update path resolves through.
        _context.InsertTyped("Records", Schema,
            [CellValue.From(1), CellValue.From("a"), CellValue.From(10), CellValue.From(1.5d)]);
    }

    [Fact]
    public void CompiledUpdate_FixedWidth_EmitsBinaryUpdateFrame_AndAppliesChange()
    {
        int affected = RunUpdate(id: 1, value: 99, score: 2.5d);
        Assert.Equal(1, affected);

        string walPath = _context.Engine.Config.ResolveWalFilePath();
        var frames = new WalFileStore(walPath).ReadBinaryFrames();
        Assert.Contains(frames, frame => frame.Header.OpType == WalFrameOperationType.Update);

        Assert.Equal(99, SelectValue(id: 1));
    }

    [Fact]
    public void CompiledUpdate_PatchesBothFixedWidthColumns()
    {
        Assert.Equal(1, RunUpdate(id: 1, value: 77, score: 8.25d));

        var result = _context.Execute("SELECT Value, Score FROM Records WHERE Id = 1").Last();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(77, Convert.ToInt32(result.Data[0]["Value"]));
        Assert.Equal(8.25d, Convert.ToDouble(result.Data[0]["Score"]), 3);
    }

    [Fact]
    public void CompiledUpdate_FixedWidth_SurvivesGracefulRestart()
    {
        Assert.Equal(1, RunUpdate(id: 1, value: 123, score: 4.5d));

        _context.Dispose();
        _context = NewContext();
        ExecOk("USE Z");

        Assert.Equal(123, SelectValue(id: 1));
    }

    [Fact]
    public void Update_VariableWidthColumn_FallsBackToLegacy_AndIsCorrect()
    {
        int affected = DataVoCompiledQuery.Update(_context, NameUpdatePlan,
        [
            new DataVoCompiledQueryParameter("name", "patched-name"),
            new DataVoCompiledQueryParameter("id", 1),
        ]);
        Assert.Equal(1, affected);

        var result = _context.Execute("SELECT Name FROM Records WHERE Id = 1").Last();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal("patched-name", result.Data[0]["Name"]?.ToString());
    }

    private int RunUpdate(int id, int value, double score)
    {
        return DataVoCompiledQuery.Update(_context, UpdatePlan,
        [
            new DataVoCompiledQueryParameter("value", value),
            new DataVoCompiledQueryParameter("score", score),
            new DataVoCompiledQueryParameter("id", id),
        ]);
    }

    private int SelectValue(int id)
    {
        var result = _context.Execute($"SELECT Value FROM Records WHERE Id = {id}").Last();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        return Convert.ToInt32(result.Data[0]["Value"]);
    }

    private DataVoContext NewContext() => new(new DataVoConfig
    {
        StorageMode = StorageMode.Disk,
        DiskStoragePath = _dir,
        WalEnabled = true,
        WalFilePath = "datavo.walbin",
        IoSchedulerMode = IoSchedulerMode.GroupCommit,
        // Park the background checkpointer so a freshly written WAL frame stays put for inspection.
        WalCheckpointIntervalMs = 60_000,
    });

    private void ExecOk(string sql)
    {
        var result = _context.Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}: {string.Join(" | ", result.Messages)}");
        }
    }

    public void Dispose()
    {
        _context.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }
}
