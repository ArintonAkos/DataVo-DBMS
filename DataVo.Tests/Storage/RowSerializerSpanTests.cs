using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Storage;

public class RowSerializerSpanTests
{
    private static List<Column> Schema() =>
    [
        new Column { Name = "Id", Type = "INT" },
        new Column { Name = "Name", Type = "VARCHAR" },
        new Column { Name = "Score", Type = "FLOAT" },
        new Column { Name = "Active", Type = "BIT" },
        new Column { Name = "Day", Type = "DATE" },
        new Column { Name = "Note", Type = "VARCHAR" },
    ];

    [Fact]
    public void DeserializeCells_RoundTripsEveryType_IncludingUtf8AndNull()
    {
        List<Column> columns = Schema();
        CellValue[] original =
        [
            CellValue.From(42),
            CellValue.From("héllo wörld"),     // multi-byte UTF8
            CellValue.From((double)1.5f),       // FLOAT stored as single-bits
            CellValue.From(true),
            CellValue.From(new DateOnly(2026, 6, 24)),
            CellValue.Null,                     // NULL string
        ];

        byte[] bytes = RowSerializer.SerializeCells(columns, original);
        CellValue[] decoded = RowSerializer.DeserializeCells(bytes, columns);

        Assert.Equal(42, decoded[0].AsInt32());
        Assert.Equal("héllo wörld", decoded[1].AsString());
        Assert.Equal(1.5, decoded[2].AsDouble(), 3);
        Assert.True(decoded[3].AsBoolean());
        Assert.Equal(new DateOnly(2026, 6, 24), decoded[4].AsDate());
        Assert.True(decoded[5].IsNull);
    }

    [Fact]
    public void DecodeProjectedCells_DecodesOnlyProjected_SkippingTheRest()
    {
        List<Column> columns = Schema(); // Id, Name, Score, Active, Day, Note
        CellValue[] full =
        [
            CellValue.From(7),
            CellValue.From("skip-me"),         // not projected (a string before a projected column)
            CellValue.From((double)2.5f),
            CellValue.From(false),             // not projected
            CellValue.From(new DateOnly(2026, 1, 2)),
            CellValue.From("note!"),
        ];
        byte[] bytes = RowSerializer.SerializeCells(columns, full);

        // Project Id, Score, Day, Note (skip Name and Active — including a skipped string).
        bool[] isProjected = [true, false, true, false, true, true];
        var dest = new CellValue[4];

        RowSerializer.DecodeProjectedCells(bytes, columns, isProjected, dest);

        Assert.Equal(7, dest[0].AsInt32());                       // Id
        Assert.Equal(2.5, dest[1].AsDouble(), 3);                 // Score
        Assert.Equal(new DateOnly(2026, 1, 2), dest[2].AsDate()); // Day
        Assert.Equal("note!", dest[3].AsString());                // Note
    }

    [Fact]
    public void DecodeProjectedCells_ProjectedNull_IsNull()
    {
        List<Column> columns = [new Column { Name = "Id", Type = "INT" }, new Column { Name = "Note", Type = "VARCHAR" }];
        byte[] bytes = RowSerializer.SerializeCells(columns, [CellValue.From(1), CellValue.Null]);

        bool[] isProjected = [false, true];
        var dest = new CellValue[1];
        RowSerializer.DecodeProjectedCells(bytes, columns, isProjected, dest);

        Assert.True(dest[0].IsNull);
    }
}
