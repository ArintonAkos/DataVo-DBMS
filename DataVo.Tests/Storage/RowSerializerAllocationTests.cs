using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Storage;

public class RowSerializerAllocationTests
{
    [Fact]
    public void SerializeCells_WarmCalls_AllocateAtMostPayloadPlusSlack()
    {
        IReadOnlyList<Column> columns =
        [
            new Column { Name = "Id", Type = "INT" },
            new Column { Name = "AccountId", Type = "INT" },
            new Column { Name = "MarketId", Type = "INT" },
            new Column { Name = "Stake", Type = "INT" },
        ];
        CellValue[] cells = [CellValue.From(1), CellValue.From(2), CellValue.From(3), CellValue.From(4)];

        for (int i = 0; i < 200; i++) _ = RowSerializer.SerializeCells(columns, cells);

        const int n = 5_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++) _ = RowSerializer.SerializeCells(columns, cells);
        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        // Payload is ~1 byte null-flag + 4 bytes per INT over 4 cols (~20B) + the byte[] header.
        Assert.True(perCall <= 96, $"SerializeCells per-call {perCall} B exceeds 96 B (stream churn not eliminated)");
    }

    [Fact]
    public void DecodeProjectedCells_WarmCalls_AllocateOnlyProjectedStrings()
    {
        // Wide row (8 cols), narrow projection (Id, Tag, Score). The only heap allocation a projected decode
        // should make is the projected string cell (Tag); Int/Float are value types and the skipped Varchars
        // (C1..C5) are advanced past without materializing. The per-column type dispatch must not allocate.
        // Column.Type uses the PascalCase casing the catalog produces (DataTypes enum ToString), so
        // column.Type.ToUpperInvariant() genuinely re-cases ("Int" -> "INT") and allocates — the bug under test.
        IReadOnlyList<Column> columns =
        [
            new Column { Name = "Id", Type = "Int" },
            new Column { Name = "Tag", Type = "Varchar", Length = 20 },
            new Column { Name = "Score", Type = "Float" },
            new Column { Name = "C1", Type = "Varchar", Length = 20 },
            new Column { Name = "C2", Type = "Varchar", Length = 20 },
            new Column { Name = "C3", Type = "Varchar", Length = 20 },
            new Column { Name = "C4", Type = "Varchar", Length = 20 },
            new Column { Name = "C5", Type = "Varchar", Length = 20 },
        ];
        CellValue[] cells =
        [
            CellValue.From(1), CellValue.From("m8"), CellValue.From(2.5),
            CellValue.From("c1"), CellValue.From("c2"), CellValue.From("c3"),
            CellValue.From("c4"), CellValue.From("c5"),
        ];
        byte[] data = RowSerializer.SerializeCells(columns, cells);
        bool[] isProjected = [true, true, true, false, false, false, false, false];
        var buffer = new CellValue[3];

        for (int i = 0; i < 200; i++) RowSerializer.DecodeProjectedCells(data, columns, isProjected, buffer);

        const int n = 5_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++) RowSerializer.DecodeProjectedCells(data, columns, isProjected, buffer);
        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        // Only the projected "Tag" string (~32 B) should be allocated. Per-column ToUpperInvariant() would add
        // a string per column (8 cols ≈ +300 B). Budget allows the projected string plus slack.
        Assert.True(perCall <= 96, $"DecodeProjectedCells per-call {perCall} B exceeds 96 B (per-column type-string churn not eliminated)");
    }

    [Fact]
    public void SerializeCells_PascalCaseColumnTypes_DoesNotChurnTypeStrings()
    {
        // Columns carry the catalog's PascalCase type names ("Int"), so the write path's per-column type
        // dispatch must not re-case/allocate. Payload is the byte[] + ~5 bytes/Int over 8 cols; per-column
        // ToUpperInvariant() would add a string per column (~256 B).
        IReadOnlyList<Column> columns =
        [
            new Column { Name = "A", Type = "Int" }, new Column { Name = "B", Type = "Int" },
            new Column { Name = "C", Type = "Int" }, new Column { Name = "D", Type = "Int" },
            new Column { Name = "E", Type = "Int" }, new Column { Name = "F", Type = "Int" },
            new Column { Name = "G", Type = "Int" }, new Column { Name = "H", Type = "Int" },
        ];
        CellValue[] cells =
        [
            CellValue.From(1), CellValue.From(2), CellValue.From(3), CellValue.From(4),
            CellValue.From(5), CellValue.From(6), CellValue.From(7), CellValue.From(8),
        ];

        for (int i = 0; i < 200; i++) _ = RowSerializer.SerializeCells(columns, cells);

        const int n = 5_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++) _ = RowSerializer.SerializeCells(columns, cells);
        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perCall <= 96, $"SerializeCells per-call {perCall} B exceeds 96 B (per-column type-string churn not eliminated)");
    }
}
