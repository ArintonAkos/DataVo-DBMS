using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class InsertTypedTests
{
    private static readonly ReactiveRowSchema PlayerSchema = new("Id", "Name", "Level");

    [Fact]
    public void InsertTyped_InsertsStoredValuesAndReturnsRowIdLikeBulkInsert()
    {
        using DataVoContext typed = CreateContext();
        typed.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        long typedRowId = typed.InsertTyped("Players", PlayerSchema,
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)]);

        using DataVoContext dict = CreateContext();
        dict.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        IReadOnlyList<long> dictRowIds = dict.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 7 }
        ]);

        Assert.Equal(dictRowIds.Single(), typedRowId);
        Dictionary<string, object?> typedRow = Select(typed, "SELECT Id, Name, Level FROM Players").Single();
        Dictionary<string, object?> dictRow = Select(dict, "SELECT Id, Name, Level FROM Players").Single();
        Assert.Equal(dictRow["Id"], typedRow["Id"]);
        Assert.Equal(dictRow["Name"], typedRow["Name"]);
        Assert.Equal(dictRow["Level"], typedRow["Level"]);
    }

    [Fact]
    public void InsertTyped_WhenColumnCountDoesNotMatchRowLength_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            ctx.InsertTyped("Players", PlayerSchema, [CellValue.From(1), CellValue.From("Ada")]));

        Assert.Contains("schema", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_WhenCellTypeDoesNotExactlyMatchColumn_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() =>
            ctx.InsertTyped("Players", PlayerSchema,
                [CellValue.From(1L), CellValue.From("Ada"), CellValue.From(7)]));

        Assert.Contains("Id", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("dictionary", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_WhenSchemaIsNotFullCatalogOrder_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        var wrongOrder = new ReactiveRowSchema("Name", "Id", "Level");

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() =>
            ctx.InsertTyped("Players", wrongOrder,
                [CellValue.From("Ada"), CellValue.From(1), CellValue.From(7)]));

        Assert.Contains("catalog order", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_WhenVarcharWouldBeTruncated_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(3), Level INT)");

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() =>
            ctx.InsertTyped("Players", PlayerSchema,
                [CellValue.From(1), CellValue.From("Ada Lovelace"), CellValue.From(7)]));

        Assert.Contains("Name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_WhenUnsupportedColumnTypeHasNullCell_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Metrics (Id INT PRIMARY KEY, Reading FLOAT)");
        var schema = new ReactiveRowSchema("Id", "Reading");

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() =>
            ctx.InsertTyped("Metrics", schema, [CellValue.From(1), CellValue.Null]));

        Assert.Contains("Reading", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("dictionary", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_PopulatesTypedAfter_AndBulkInsertLeavesTypedAfterNull()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        ctx.Changes.Enabled = true;
        var captured = new List<RowChange>();
        ctx.Changes.Captured += set => captured.AddRange(set.Changes);

        long rowId = ctx.InsertTyped("Players", PlayerSchema,
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)]);
        ctx.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Bob", ["Level"] = 3 }
        ]);

        RowChange typedChange = captured.Single(c => c.RowId == rowId);
        Assert.NotNull(typedChange.TypedAfter);
        Assert.Equal(1, typedChange.TypedAfter.Value.AsRowRef()[0].AsInt32());
        Assert.Equal("Ada", typedChange.TypedAfter.Value.AsRowRef()[1].AsString());
        Assert.Equal(7, typedChange.After!["Level"]);

        RowChange dictChange = captured.Single(c => Equals(c.After!["Id"], 2));
        Assert.Null(dictChange.TypedAfter);
        Assert.Equal("Bob", dictChange.After!["Name"]);
    }

    [Fact]
    public void InsertTypedDispatchLoop_AllocatesMateriallyLessThanBulkInsertDispatchLoop()
    {
        long typed = MeasureOrderInsertLoop(useTypedInsert: true, startId: 1);
        long bulk = MeasureOrderInsertLoop(useTypedInsert: false, startId: 1);

        Assert.True(typed < bulk, $"typed={typed} bulk={bulk}");
        Assert.True(typed <= bulk * 90 / 100, $"typed={typed} bulk={bulk}");
    }

    [Fact]
    public void InsertTyped_WhenExistingValidationRejectsSingleRow_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        ctx.InsertTyped("Players", PlayerSchema,
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)]);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            ctx.InsertTyped("Players", PlayerSchema,
                [CellValue.From(1), CellValue.From("Duplicate"), CellValue.From(8)]));

        Assert.Contains("Primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static long MeasureOrderInsertLoop(bool useTypedInsert, int startId)
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Accounts (Id INT PRIMARY KEY, IsVip BIT)");
        ctx.Execute("CREATE TABLE Markets (Id INT PRIMARY KEY, Category VARCHAR(20))");
        ctx.Execute("CREATE TABLE Orders (Id INT, AccountId INT, MarketId INT, Stake INT)");
        ctx.Execute("INSERT INTO Accounts VALUES (1, true)");
        ctx.Execute("INSERT INTO Markets VALUES (1, 'sports')");
        using IDisposable sub = ctx.SubscribeZeroAlloc("""
            SELECT m.Category, SUM(o.Stake) AS TotalExposure
            FROM Orders o
            JOIN Accounts a ON o.AccountId = a.Id
            JOIN Markets m ON o.MarketId = m.Id
            WHERE a.IsVip = true
            GROUP BY m.Category
            """, (in QueryChangeRef _) => { });

        var schema = new ReactiveRowSchema("Id", "AccountId", "MarketId", "Stake");
        CellValue[] cells = new CellValue[4];

        for (int i = 0; i < 200; i++)
        {
            InsertOne(ctx, useTypedInsert, schema, cells, startId + i);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 2_000; i++)
        {
            InsertOne(ctx, useTypedInsert, schema, cells, startId + 200 + i);
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static void InsertOne(
        DataVoContext ctx,
        bool useTypedInsert,
        ReactiveRowSchema schema,
        CellValue[] cells,
        int id)
    {
        if (useTypedInsert)
        {
            cells[0] = CellValue.From(id);
            cells[1] = CellValue.From(1);
            cells[2] = CellValue.From(1);
            cells[3] = CellValue.From(25);
            ctx.InsertTyped("Orders", schema, cells);
        }
        else
        {
            ctx.BulkInsert("Orders",
            [
                new Dictionary<string, object?>
                {
                    ["Id"] = id,
                    ["AccountId"] = 1,
                    ["MarketId"] = 1,
                    ["Stake"] = 25
                }
            ]);
        }

        ctx.DispatchPendingNotifications();
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string dbName = $"TypedInsert_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {dbName}");
        context.Execute($"USE {dbName}");
        return context;
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Single();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result.Data;
    }
}
