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
