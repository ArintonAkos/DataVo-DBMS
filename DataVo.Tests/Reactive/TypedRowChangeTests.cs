using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

public class TypedRowChangeTests
{
    [Fact]
    public void TypedRow_CopiesCellsAndExposesBorrowedRowRef()
    {
        var schema = new ReactiveRowSchema("Id", "Stake");
        CellValue[] source = [CellValue.From(1), CellValue.From(25)];

        var typed = new TypedRow(schema, source);
        source[1] = CellValue.From(999);

        RowRef row = typed.AsRowRef();
        Assert.Same(schema, typed.Schema);
        Assert.Equal(2, typed.Cells.Length);
        Assert.Equal(1, row[0].AsInt32());
        Assert.Equal(25, row[1].AsInt32());
    }

    [Fact]
    public void TypedRow_RejectsCellCountThatDoesNotMatchSchema()
    {
        var schema = new ReactiveRowSchema("Id", "Stake");

        var ex = Assert.Throws<ArgumentException>(() => new TypedRow(schema, [CellValue.From(1)]));

        Assert.Equal("cells", ex.ParamName);
    }

    [Fact]
    public void RowChange_DefaultsTypedAfterToNull_ForExistingConstructorCalls()
    {
        var after = new Dictionary<string, object?> { ["Id"] = 1 };

        var change = new RowChange("Orders", 7, ChangeKind.Insert, before: null, after: after);

        Assert.Null(change.TypedAfter);
        Assert.Same(after, change.After);
    }

    [Fact]
    public void RowChange_ExposesOriginalFiveParameterConstructor_ForBinaryCompatibility()
    {
        var constructor = typeof(RowChange).GetConstructor(
        [
            typeof(string),
            typeof(long),
            typeof(ChangeKind),
            typeof(IReadOnlyDictionary<string, object?>),
            typeof(IReadOnlyDictionary<string, object?>)
        ]);

        Assert.NotNull(constructor);
    }

    [Fact]
    public void RowChange_CanCarryTypedAfter_WithoutChangingAfterDictionary()
    {
        var schema = new ReactiveRowSchema("Id");
        var typed = new TypedRow(schema, [CellValue.From(1)]);
        var after = new Dictionary<string, object?> { ["Id"] = 1 };

        var change = new RowChange("Orders", 7, ChangeKind.Insert, before: null, after: after, typedAfter: typed);

        Assert.NotNull(change.TypedAfter);
        Assert.Equal(1, change.TypedAfter.Value.AsRowRef()[0].AsInt32());
        Assert.Same(after, change.After);
    }

    [Fact]
    public void ChangeRecorder_RecordTypedInsert_PublishesOwnedAfterAndTypedAfter()
    {
        using var engine = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
        engine.Changes.Enabled = true;

        ChangeSet? captured = null;
        engine.Changes.Captured += set => captured = set;

        var schema = new ReactiveRowSchema("Id");
        var typed = new TypedRow(schema, [CellValue.From(1)]);
        var ownedAfter = new Dictionary<string, object?> { ["Id"] = 1 };

        ChangeRecorder recorder = ChangeRecorder.TryCreate(engine, "GameDb")!;
        recorder.RecordTypedInsert("Orders", 7, ownedAfter, typed);
        recorder.Publish();

        RowChange change = Assert.Single(captured!.Changes);
        Assert.Same(ownedAfter, change.After);
        Assert.NotNull(change.TypedAfter);
        Assert.Equal(1, change.TypedAfter.Value.AsRowRef()[0].AsInt32());
    }
}
