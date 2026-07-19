using System.Collections;
using DataVo.Core;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Verifies that <see cref="DataVoDate"/> is the canonical DATE value stored by <see cref="CellValue"/>,
/// while modern <see cref="DateOnly"/> values remain available through explicitly named adapters.
/// </summary>
public class CellValueDateTests
{
    [Fact]
    public void From_DataVoDate_RoundTripsViaAsDate()
    {
        DataVoDate d = DataVoDate.FromDayNumber(739_788);
        CellValue cell = CellValue.From(d);

        Assert.Equal(CellType.Date, cell.Type);
        Assert.False(cell.IsNull);
        Assert.Equal(d, cell.AsDate());
    }

    [Fact]
    public void From_DateOnly_RoundTripsViaExplicitModernAdapter()
    {
        var d = new DateOnly(2026, 6, 22);
        CellValue cell = CellValue.From(d);

        Assert.Equal(CellType.Date, cell.Type);
        Assert.Equal(DataVoDate.FromDateOnly(d), cell.AsDate());
        Assert.Equal(d, cell.AsDateOnly());
    }

    [Fact]
    public void FromObject_BoxedDateOnly_ProducesCanonicalDateCell()
    {
        object boxed = new DateOnly(2026, 1, 5);
        CellValue cell = CellValue.From(boxed);

        Assert.Equal(CellType.Date, cell.Type);
        Assert.Equal(DataVoDate.FromDateOnly((DateOnly)boxed), cell.AsDate());
        Assert.Equal((DateOnly)boxed, cell.AsDateOnly());
    }

    [Fact]
    public void ToObject_DateCell_ReturnsDataVoDate()
    {
        DataVoDate d = DataVoDate.FromDateTime(new DateTime(2025, 12, 31));
        object? obj = CellValue.From(d).ToObject();

        Assert.IsType<DataVoDate>(obj);
        Assert.Equal(d, (DataVoDate)obj!);
    }

    [Fact]
    public void Null_RemainsNull_AndIsNull()
    {
        Assert.True(CellValue.Null.IsNull);
        Assert.Null(CellValue.Null.ToObject());
    }

    [Fact]
    public void TypeMismatch_Throws_BothDirections()
    {
        CellValue date = CellValue.From(DataVoDate.FromDateTime(new DateTime(2026, 6, 22)));
        Assert.Throws<InvalidOperationException>(() => date.AsInt32()); // reading a Date cell as another type

        CellValue notDate = CellValue.From(42);
        Assert.Throws<InvalidOperationException>(() => notDate.AsDate()); // reading another cell as Date
    }

    [Fact]
    public void AggregateGroupKey_FormatsCanonicalDateInvariantly()
    {
        var select = (SelectStatement)ReactiveQueryParser.ParseSingleStatement(
            "SELECT Day, COUNT(*) AS Cnt FROM Events GROUP BY Day");
        using DataVoEngine engine = DataVoEngine.Initialize(
            new DataVoConfig { StorageMode = StorageMode.InMemory });
        var query = new AggregateReactiveQuery(select, engine, "DateDb");
        var builder = new QueryChangeBuilder(query.OutputSchema);
        var schema = new ReactiveRowSchema("Id", "Day");
        CellValue[] cells =
        [
            CellValue.From(1),
            CellValue.From(DataVoDate.FromDateTime(new DateTime(2026, 6, 22)))
        ];
        var change = new RowChange("Events", 1, TypedRow.FromOwnedCells(schema, cells));

        query.ApplyInto([change], builder);

        var groups = Assert.IsAssignableFrom<IDictionary>(
            typeof(AggregateReactiveQuery)
                .GetField("_groups", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
                .GetValue(query));
        Assert.True(groups.Contains("v:2026-06-22"));
    }
}
