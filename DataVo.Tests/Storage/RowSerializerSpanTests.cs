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
}
