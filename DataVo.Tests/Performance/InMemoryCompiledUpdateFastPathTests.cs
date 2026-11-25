using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Xunit.Abstractions;

namespace DataVo.Tests.Performance;

/// <summary>
/// The compiled fixed-width UPDATE has an allocation-free path in in-memory mode too. It used to run only in LSM
/// mode, so every in-memory compiled update materialized dictionaries for the old and new row while it held the
/// row's write lock and the table's intent lock.
/// </summary>
public sealed class InMemoryCompiledUpdateFastPathTests(ITestOutputHelper output)
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");
    private static readonly DataVoCompiledQueryPlan Plan = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string> { ["Value"] = "value", ["Score"] = "score" },
        whereColumn: "Id",
        whereParameterName: "id");

    [Fact]
    public void TypedRows_FastPath_AllocatesNothing_AndIsFarCheaperThanTheDictionaryPath()
    {
        (long fastBytes, double fastMicros) = MeasureUpdates(zeroAlloc: true);
        (long slowBytes, double slowMicros) = MeasureUpdates(zeroAlloc: false);
        output.WriteLine($"fast path {fastBytes} B/update {fastMicros:F2} us; dictionary path {slowBytes} B/update {slowMicros:F2} us");

        // Lock ids are structs and lock entries are pooled, the row is copied through a borrowed cell buffer and
        // the primary key is re-pointed in place, so nothing is allocated per update for the update itself. What
        // remains (measured 96 B) is the table's storage growing: in-memory rows are never patched in place, so each
        // update appends the row's new version to the packed store, whose chunks are allocated as it fills.
        Assert.True(fastBytes <= 128, $"in-memory fixed-width UPDATE allocated {fastBytes} B/update");
        Assert.True(slowBytes > 1_000, $"the dictionary path is expected to allocate per update, measured {slowBytes} B");
    }

    [Fact]
    public void TypedRows_FastPath_WritesTheValues_MovesTheRow_AndKeepsOneRowPerKey()
    {
        using DataVoContext context = CreateContext(zeroAlloc: true, rows: 100);
        long rowIdBefore = RowIdOf(context, 42);

        Assert.Equal(1, Update(context, 42, 7, 3.25));
        Assert.Equal(1, DataVoCompiledQuery.Update(context, Plan,
        [
            new DataVoCompiledQueryParameter("id", 43),
            new DataVoCompiledQueryParameter("value", 8),
            new DataVoCompiledQueryParameter("score", 4.5d),
        ]));
        Assert.Equal(1, DataVoCompiledQuery.UpdateFixedWidth(context, Plan,
        [
            new DataVoFixedWidthQueryParameter("id", DataVoFixedWidthValue.From(44)),
            new DataVoFixedWidthQueryParameter("value", DataVoFixedWidthValue.From(9)),
            new DataVoFixedWidthQueryParameter("score", DataVoFixedWidthValue.From(5.75d)),
        ]));
        Assert.Equal(0, Update(context, 1_000, 1, 1d)); // no such key

        Assert.NotEqual(rowIdBefore, RowIdOf(context, 42)); // out of place: in-memory rows are never patched in place
        AssertRow(context, 42, "n42", 7, 3.25);
        AssertRow(context, 43, "n43", 8, 4.5);
        AssertRow(context, 44, "n44", 9, 5.75);
        Assert.Equal(100L, Convert.ToInt64(context.Execute("SELECT COUNT(*) AS N FROM Records")[0].Data[0]["N"]));
        Assert.Single(context.Execute("SELECT Id FROM Records WHERE Value = 7")[0].Data);

        // The key moved with the row: it can be deleted and inserted again.
        Assert.False(context.Execute("DELETE FROM Records WHERE Id = 42")[0].IsError);
        Assert.False(context.Execute("INSERT INTO Records (Id, Name, Value, Score) VALUES (42, 'again', 1, 1.0)")[0].IsError);
        AssertRow(context, 42, "again", 1, 1d);
    }

    [Fact]
    public void SerializedRows_FastPath_WritesTheValues()
    {
        using DataVoContext context = CreateContext(zeroAlloc: true, rows: 0);
        for (int id = 1; id <= 20; id++)
        {
            Assert.False(context.Execute($"INSERT INTO Records (Id, Name, Value, Score) VALUES ({id}, 'n{id}', 0, 0.5)")[0].IsError);
        }

        for (int i = 0; i < 200; i++)
        {
            Assert.Equal(1, Update(context, (i % 20) + 1, i, i + 0.5));
        }

        AssertRow(context, 20, "n20", 199, 199.5);
        Assert.Equal(20L, Convert.ToInt64(context.Execute("SELECT COUNT(*) AS N FROM Records")[0].Data[0]["N"]));
    }

    [Fact]
    public void NullCell_FallsBackToTheDictionaryPath()
    {
        using DataVoContext context = CreateContext(zeroAlloc: true, rows: 0);
        context.InsertTyped("Records", Schema, [CellValue.From(1), CellValue.From("a"), CellValue.Null, CellValue.From(1.5)]);

        Assert.Equal(1, Update(context, 1, 5, 2.5));

        AssertRow(context, 1, "a", 5, 2.5);
    }

    [Fact]
    public void OtherFixedWidthTypes_ArePatchedWithTheirOwnTypes()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Execute(context, "CREATE DATABASE Z");
        Execute(context, "USE Z");
        Execute(context, "CREATE TABLE Flags (Id INT PRIMARY KEY, Total BIGINT, Active BIT)");
        context.InsertTyped("Flags", new ReactiveRowSchema("Id", "Total", "Active"), [CellValue.From(1), CellValue.From(10L), CellValue.From(false)]);
        DataVoCompiledQueryPlan plan = DataVoCompiledQueryPlan.Update(
            "Flags",
            new Dictionary<string, string> { ["Total"] = "total", ["Active"] = "active" },
            whereColumn: "Id",
            whereParameterName: "id");

        Assert.Equal(1, DataVoCompiledQuery.UpdateFixedWidthByPrimaryKey(context, plan, DataVoFixedWidthValue.From(1),
            [DataVoFixedWidthValue.From(5_000_000_000L), DataVoFixedWidthValue.From(true)]));

        Dictionary<string, object?> row = context.Execute("SELECT Total, Active FROM Flags WHERE Id = 1")[0].Data[0];
        Assert.Equal(5_000_000_000L, Convert.ToInt64(row["Total"]));
        Assert.True(Convert.ToBoolean(row["Active"]));
    }

    /// <summary>
    /// A point read takes the row id from the primary key without a lock. An update that moved the row meanwhile
    /// deletes the old row only after re-pointing the key, so the read must follow the key instead of reporting an
    /// existing row missing (both update paths moved rows this way; the read used to give up).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConcurrentUpdates_OfTheSameRows_NeverHideARowFromPointReads(bool zeroAlloc)
    {
        using DataVoContext context = CreateContext(zeroAlloc, rows: 16);
        DataVoPreparedSelectSingle<int> lookup = DataVoCompiledQuery.PrepareSelectSingleTyped(
            context,
            DataVoCompiledQueryPlan.SelectSingle("Records", ["Id", "Name", "Value", "Score"], whereColumn: "Id", parameterName: "id"),
            static row => row.GetInt32(0));
        int stop = 0;
        long misses = 0;

        Task[] writers = [.. Enumerable.Range(0, 4).Select(writer => Task.Run(() =>
        {
            for (int i = 0; i < 5_000; i++)
            {
                Update(context, (i % 16) + 1, writer * 100_000 + i, i);
            }
        }))];
        Task[] readers = [.. Enumerable.Range(0, 4).Select(reader => Task.Run(() =>
        {
            var random = new Random(reader);
            while (Volatile.Read(ref stop) == 0)
            {
                int id = random.Next(1, 17);
                if (lookup.Execute(id) != id) // a missing row reads as 0
                {
                    Interlocked.Increment(ref misses);
                }
            }
        }))];

        await Task.WhenAll(writers);
        Volatile.Write(ref stop, 1);
        await Task.WhenAll(readers);

        Assert.Equal(0, Interlocked.Read(ref misses));
        Assert.Equal(16L, Convert.ToInt64(context.Execute("SELECT COUNT(*) AS N FROM Records")[0].Data[0]["N"]));
    }

    private static (long BytesPerUpdate, double MicrosPerUpdate) MeasureUpdates(bool zeroAlloc)
    {
        const int Rows = 1_000;
        const int Measured = 20_000;
        using DataVoContext context = CreateContext(zeroAlloc, Rows);
        for (int i = 0; i < 2_000; i++)
        {
            Update(context, (i % Rows) + 1, i, i);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        long start = Stopwatch.GetTimestamp();
        for (int i = 0; i < Measured; i++)
        {
            Update(context, (i % Rows) + 1, i, i);
        }

        double micros = Stopwatch.GetElapsedTime(start).TotalMilliseconds * 1_000 / Measured;
        long bytes = (GC.GetAllocatedBytesForCurrentThread() - before) / Measured;
        AssertRow(context, Rows, $"n{Rows}", Measured - 1, Measured - 1);
        return (bytes, micros);
    }

    private static DataVoContext CreateContext(bool zeroAlloc, int rows)
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory, EnableZeroAllocCompiledUpdate = zeroAlloc });
        Execute(context, "CREATE DATABASE Z");
        Execute(context, "USE Z");
        Execute(context, "CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(20), Value INT, Score FLOAT)");
        if (rows > 0)
        {
            context.InsertTypedBatch("Records", Schema, [.. Enumerable.Range(1, rows).Select(i =>
                new[] { CellValue.From(i), CellValue.From($"n{i}"), CellValue.From(0), CellValue.From(0.5) })]);
        }

        return context;
    }

    private static int Update(DataVoContext context, int id, int value, double score) =>
        DataVoCompiledQuery.UpdateFixedWidthByPrimaryKey(context, Plan, DataVoFixedWidthValue.From(id),
            [DataVoFixedWidthValue.From(value), DataVoFixedWidthValue.From(score)]);

    private static void AssertRow(DataVoContext context, int id, string name, int value, double score)
    {
        List<Dictionary<string, object?>> rows = context.Execute($"SELECT Name, Value, Score FROM Records WHERE Id = {id}")[0].Data;
        Dictionary<string, object?> row = Assert.Single(rows);
        Assert.Equal(name, row["Name"]);
        Assert.Equal(value, Convert.ToInt32(row["Value"]));
        Assert.Equal(score, Convert.ToDouble(row["Score"]), 3);
    }

    private static long RowIdOf(DataVoContext context, int id) =>
        (long)context.Engine.IndexManager.FilterUsingIndex($"[{id}]", "_PK_Records", "Records", "Z")[0];

    private static void Execute(DataVoContext context, string sql) =>
        Assert.False(context.Execute(sql)[0].IsError, sql);
}
