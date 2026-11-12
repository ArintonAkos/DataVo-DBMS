using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Memory;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Memory;

/// <summary>
/// The in-memory engine keeps typed rows packed and serialized rows as bytes in one sequence of row ids, and
/// serves both through the interfaces it had when typed rows were objects.
/// </summary>
public class InMemoryTypedStorageTests
{
    private const string Db = "Shop";
    private const string Table = "Items";

    private static readonly Column[] Columns =
    [
        new Column { Name = "Id", Type = "INT" },
        new Column { Name = "Gold", Type = "BIGINT" },
        new Column { Name = "Name", Type = "VARCHAR" },
    ];

    private static readonly ReactiveRowSchema Schema = new("Id", "Gold", "Name");

    private static StoredRow Typed(int id) =>
        new(Schema, [CellValue.From(id), CellValue.From(id * 10L), CellValue.From("n" + id)]);

    /// <summary>A typed row whose INT column holds a BIGINT cell, so it cannot be packed.</summary>
    private static StoredRow Odd(int id) =>
        new(Schema, [CellValue.From((long)id), CellValue.From(id * 10L), CellValue.From("n" + id)]);

    private static byte[] Raw(int id) =>
        RowSerializer.SerializeCells(Columns, [CellValue.From(id), CellValue.From(id * 10L), CellValue.From("n" + id)]);

    private static ITypedRowStorageEngine TypedEngine(InMemoryStorageEngine engine) => engine;

    private static string? NameOf(InMemoryStorageEngine engine, long rowId)
    {
        string? name = null;
        bool found = TypedEngine(engine).TryVisitTypedCells(Db, Table, rowId, ref name,
            static (ref string? seen, long _, ReadOnlySpan<CellValue> cells) =>
            {
                seen = cells[2].AsString();
                return true;
            });
        return found ? name : null;
    }

    [Theory]
    [InlineData("INT", CellType.Int32)]
    [InlineData("bigint", CellType.Int64)]
    [InlineData("FLOAT", CellType.Double)]
    [InlineData("Double", CellType.Double)]
    [InlineData("BIT", CellType.Boolean)]
    [InlineData("DATE", CellType.Date)]
    [InlineData("GUID", CellType.Guid)]
    [InlineData("VARCHAR", CellType.String)]
    [InlineData("VECTOR", CellType.Vector)]
    public void ColumnCellTypes_MapEveryColumnTypeTypedRowsSupport(string columnType, CellType expected)
    {
        Assert.True(ColumnCellTypes.TryGet(columnType, out CellType type));
        Assert.Equal(expected, type);
        Assert.Equal(expected, RowLayout.Create(ColumnCellTypes.ForLayout([new Column { Name = "C", Type = columnType }]))[0].Type);
    }

    [Fact]
    public void ColumnCellTypes_UnsupportedColumnTypeIsRefused_AndLaidOutAsAReference()
    {
        Assert.False(ColumnCellTypes.TryGet("DECIMAL", out _));
        Assert.Equal([CellType.Int32, CellType.String], ColumnCellTypes.ForLayout(
            [new Column { Name = "A", Type = "INT" }, new Column { Name = "B", Type = "DECIMAL" }]));
    }

    [Fact]
    public void TableRows_TypedOnlyTable_HoldsNoSerializedRowStorage_AndSerializedOnlyTableHoldsNoLayout()
    {
        var typedOnly = new TableRows();
        var serializedOnly = new TableRows();
        for (int id = 1; id <= 5000; id++)
        {
            Assert.Equal(id - 1, typedOnly.AddTyped(Columns, Typed(id)));
            Assert.Equal(id - 1, serializedOnly.AddRaw(Raw(id)));
        }

        Assert.Null(typedOnly.RawAt(4999));
        Assert.Equal(16, typedOnly.Layout!.RecordBytes);
        Assert.Equal(0, typedOnly.KeptApartCount);
        Assert.Null(serializedOnly.Layout);
        Assert.Null(serializedOnly.Schema);
        Assert.False(serializedOnly.HasTypedRows());
        Assert.NotNull(serializedOnly.RawAt(4999));
    }

    [Fact]
    public void TableRows_SerializedAndTypedRowsShareOnePositionSequence()
    {
        var rows = new TableRows();
        Assert.Equal(0, rows.AddRaw(Raw(1)));          // before any typed row
        Assert.Equal(1, rows.AddTyped(Columns, Typed(2)));
        Assert.Equal(2, rows.AddRaw(Raw(3)));          // after the packed store began
        Assert.Equal(3, rows.AddTyped(Columns, Odd(4)));
        Assert.Equal(4, rows.AddTyped(Columns, Typed(5)));

        Assert.Equal(5, rows.Count);
        Assert.NotNull(rows.RawAt(0));
        Assert.False(rows.IsTypedLive(0));
        Assert.Null(rows.RawAt(1));
        Assert.True(rows.IsTypedLive(1));
        Assert.NotNull(rows.RawAt(2));
        Assert.False(rows.IsTypedLive(2));
        Assert.True(rows.TryCopyTyped(3, out StoredRow? odd));
        Assert.Equal(CellType.Int64, odd![0].Type);
        Assert.True(rows.TryCopyTyped(4, out StoredRow? last));
        Assert.Equal(5, last![0].AsInt32());
        Assert.Same(Schema, last.Schema);

        Assert.True(rows.Remove(0));
        Assert.True(rows.Remove(3));
        Assert.False(rows.Remove(3));
        Assert.False(rows.IsLive(0));
        Assert.False(rows.IsLive(3));
        Assert.Equal(0, rows.KeptApartCount);
        Assert.True(rows.IsLive(2));
        Assert.True(rows.IsLive(4));
    }

    [Fact]
    public void TableRows_Clone_KeepsBothKindsAndGoesItsOwnWay()
    {
        var rows = new TableRows();
        rows.AddRaw(Raw(1));
        rows.AddTyped(Columns, Typed(2));
        rows.AddTyped(Columns, Odd(3));
        rows.Remove(1);

        TableRows copy = rows.Clone();
        rows.Remove(0);
        rows.Remove(2);
        byte[] original = Raw(1);

        Assert.Equal(3, copy.Count);
        Assert.Equal(original, copy.RawAt(0));
        Assert.False(copy.IsLive(1));
        Assert.True(copy.TryCopyTyped(2, out StoredRow? odd));
        Assert.Equal(3L, odd![0].AsInt64());
        Assert.Equal(1, copy.KeptApartCount);
        Assert.Equal(3, copy.AddTyped(Columns, Typed(4)));
        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public void Engine_NumbersSerializedAndTypedRowsTogether_AndReadsEachItsOwnWay()
    {
        var engine = new InMemoryStorageEngine();
        Assert.Equal(1, engine.InsertRow(Db, Table, Raw(1)));
        Assert.Equal(2, TypedEngine(engine).InsertTypedRow(Db, Table, Columns, Typed(2)));
        Assert.Equal([3L, 4L], engine.InsertRows(Db, Table, [Raw(3), Raw(4)]));
        Assert.Equal([5L, 6L], TypedEngine(engine).InsertTypedRows(Db, Table, Columns, [Typed(5), Odd(6)]));

        Assert.Equal(Raw(3), engine.ReadRow(Db, Table, 3));
        Assert.Throws<InvalidOperationException>(() => engine.ReadRow(Db, Table, 2));
        Assert.Throws<RowNotFoundException>(() => engine.ReadRow(Db, Table, 7));
        Assert.Equal([1L, 3L, 4L], engine.ReadAllRows(Db, Table).Select(row => row.RowId));
        Assert.Equal([2L, 5L, 6L], TypedEngine(engine).ReadAllTypedRows(Db, Table).Select(row => row.RowId));

        Assert.Equal("n5", NameOf(engine, 5));
        Assert.Equal("n6", NameOf(engine, 6));
        Assert.Null(NameOf(engine, 1));
        Assert.Null(NameOf(engine, 99));
        Assert.True(TypedEngine(engine).HasTypedRow(Db, Table, 2));
        Assert.False(TypedEngine(engine).HasTypedRow(Db, Table, 3));

        engine.DeleteRow(Db, Table, 2);
        engine.DeleteRow(Db, Table, 3);
        Assert.False(TypedEngine(engine).HasTypedRow(Db, Table, 2));
        Assert.False(TypedEngine(engine).TryReadTypedRow(Db, Table, 2, out _));
        Assert.Throws<RowDeletedException>(() => engine.ReadRow(Db, Table, 3));
        Assert.Throws<RowDeletedException>(() => engine.ReadRow(Db, Table, 2));
        Assert.Equal(7, TypedEngine(engine).InsertTypedRow(Db, Table, Columns, Typed(7)));
    }

    [Fact]
    public void Engine_PagedScan_VisitsBothKindsInRowIdOrder()
    {
        var engine = new InMemoryStorageEngine();
        engine.InsertRow(Db, Table, Raw(1));
        TypedEngine(engine).InsertTypedRows(Db, Table, Columns, [Typed(2), Odd(3), Typed(4)]);
        engine.InsertRow(Db, Table, Raw(5));
        engine.DeleteRow(Db, Table, 4);

        var seen = new List<string>();
        ((IPagedRowScan)engine).VisitPage(Db, Table, null, null, int.MaxValue, ref seen,
            static (ref List<string> list, long rowId, ReadOnlySpan<byte> raw, ReadOnlySpan<CellValue> cells) =>
                list.Add(cells.IsEmpty ? $"{rowId}:bytes" : $"{rowId}:{cells[2].AsString()}"));

        Assert.Equal(["1:bytes", "2:n2", "3:n3", "5:bytes"], seen);
    }

    [Fact]
    public void Engine_VisitTypedRows_StopsWhenTheVisitorSaysSo()
    {
        var engine = new InMemoryStorageEngine();
        TypedEngine(engine).InsertTypedRows(Db, Table, Columns, [Typed(1), Typed(2), Typed(3)]);

        var ids = new List<long>();
        TypedEngine(engine).VisitTypedRows(Db, Table, ref ids,
            static (ref List<long> list, long rowId, ReadOnlySpan<CellValue> _) =>
            {
                list.Add(rowId);
                return list.Count < 2;
            });

        Assert.Equal([1L, 2L], ids);
    }

    [Fact]
    public void Engine_AVisitorMayReadOtherRowsOnTheSameThread_WithoutLosingTheCellsItHolds()
    {
        var engine = new InMemoryStorageEngine();
        TypedEngine(engine).InsertTypedRows(Db, Table, Columns, [Typed(1), Typed(2), Typed(3)]);

        var state = (Engine: engine, Seen: new List<string>());
        TypedEngine(engine).VisitTypedRows(Db, Table, ref state,
            static (ref (InMemoryStorageEngine Engine, List<string> Seen) s, long rowId, ReadOnlySpan<CellValue> outer) =>
            {
                // Reads of other rows, one nested inside the other, while the outer row's cells are still in use.
                string? next = NameOf(s.Engine, rowId % 3 + 1);
                var nested = (s.Engine, Inner: (string?)null);
                TypedEngine(s.Engine).TryVisitTypedCells(Db, Table, (rowId + 1) % 3 + 1, ref nested,
                    static (ref (InMemoryStorageEngine Engine, string? Inner) n, long id, ReadOnlySpan<CellValue> cells) =>
                    {
                        n.Inner = NameOf(n.Engine, id) + "/" + cells[2].AsString();
                        return true;
                    });
                s.Seen.Add($"{outer[0].AsInt32()}:{outer[2].AsString()}>{next}>{nested.Inner}");
                return true;
            });

        Assert.Equal(["1:n1>n2>n3/n3", "2:n2>n3>n1/n1", "3:n3>n1>n2/n2"], state.Seen);
    }

    [Fact]
    public void CellScratch_LendsADifferentBufferAtEachDepth_AndClearsWhatItGetsBack()
    {
        CellValue[] outer = CellScratch.Rent(3);
        outer[0] = CellValue.From("held");
        CellValue[] inner = CellScratch.Rent(3);
        Assert.NotSame(outer, inner);
        inner[0] = CellValue.From("inner");
        CellScratch.Return(inner, 3);

        Assert.Equal("held", outer[0].AsString());
        CellScratch.Return(outer, 3);

        CellValue[] again = CellScratch.Rent(3);
        Assert.Same(outer, again);
        Assert.True(again[0].IsNull);
        CellScratch.Return(again, 3);
    }

    [Fact]
    public void Engine_SnapshotAndRestore_BringBackBothKinds_RowsKeptApartIncluded()
    {
        var engine = new InMemoryStorageEngine();
        IInMemoryStorageSnapshotProvider snapshots = engine;
        engine.InsertRow(Db, Table, Raw(1));
        TypedEngine(engine).InsertTypedRows(Db, Table, Columns, [Typed(2), Odd(3)]);
        InMemoryStorageSnapshot snapshot = snapshots.CreateSnapshot();

        engine.DeleteRow(Db, Table, 1);
        engine.DeleteRow(Db, Table, 3);
        TypedEngine(engine).InsertTypedRow(Db, Table, Columns, Typed(4));

        for (int round = 0; round < 2; round++) // a snapshot can be restored more than once
        {
            snapshots.RestoreSnapshot(snapshot);
            Assert.Equal(Raw(1), engine.ReadRow(Db, Table, 1));
            Assert.Equal("n2", NameOf(engine, 2));
            Assert.True(TypedEngine(engine).TryReadTypedRow(Db, Table, 3, out StoredRow? odd));
            Assert.Equal(3L, odd![0].AsInt64());
            Assert.Null(NameOf(engine, 4));
            engine.DeleteRow(Db, Table, 2);
            Assert.Equal(4, TypedEngine(engine).InsertTypedRow(Db, Table, Columns, Typed(9)));
        }
    }

    [Fact]
    public void Engine_DropTable_ForgetsTheLayout_SoTheTableCanComeBackWithOtherColumns()
    {
        var engine = new InMemoryStorageEngine();
        TypedEngine(engine).InsertTypedRows(Db, Table, Columns, [Typed(1), Odd(2)]);
        engine.DropTable(Db, Table);

        Column[] wider = [.. Columns, new Column { Name = "Active", Type = "BIT" }];
        var schema = new ReactiveRowSchema("Id", "Gold", "Name", "Active");
        var row = new StoredRow(schema, [CellValue.From(1), CellValue.From(5L), CellValue.From("x"), CellValue.From(true)]);
        Assert.Equal(1, TypedEngine(engine).InsertTypedRow(Db, Table, wider, row));

        Assert.True(TypedEngine(engine).TryReadTypedRow(Db, Table, 1, out StoredRow? read));
        Assert.Equal(4, read!.Count);
        Assert.True(read[3].AsBoolean());
        Assert.False(TypedEngine(engine).HasTypedRow(Db, Table, 2));
    }

    [Fact]
    public void Engine_Compaction_RenumbersSerializedRows_AndRefusesWhileTypedRowsRemain()
    {
        var engine = new InMemoryStorageEngine();
        engine.InsertRows(Db, Table, [Raw(1), Raw(2), Raw(3)]);
        TypedEngine(engine).InsertTypedRow(Db, Table, Columns, Typed(4));
        engine.DeleteRow(Db, Table, 2);

        Assert.Throws<InvalidOperationException>(() => engine.CompactTable(Db, Table));

        engine.DeleteRow(Db, Table, 4);
        List<(long NewRowId, byte[] RawRow)> compacted = engine.CompactTable(Db, Table);
        Assert.Equal([1L, 2L], compacted.Select(row => row.NewRowId));
        Assert.Equal(Raw(3), engine.ReadRow(Db, Table, 2));
        Assert.Equal(3, TypedEngine(engine).InsertTypedRow(Db, Table, Columns, Typed(5)));
        Assert.Equal("n5", NameOf(engine, 3));
    }
}
