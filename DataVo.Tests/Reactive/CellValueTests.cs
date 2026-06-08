using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public class CellValueTests
{
    [Fact]
    public void Scalars_RoundTrip_WithoutLoss()
    {
        Assert.True(CellValue.Null.IsNull);
        Assert.Equal(CellType.Null, CellValue.Null.Type);

        Assert.True(CellValue.From(true).AsBoolean());
        Assert.Equal(42, CellValue.From(42).AsInt32());
        Assert.Equal(9_000_000_000L, CellValue.From(9_000_000_000L).AsInt64());
        Assert.Equal(3.5d, CellValue.From(3.5d).AsDouble());
        Assert.Equal(123.45m, CellValue.From(123.45m).AsDecimal());
        Assert.Equal("hello", CellValue.From("hello").AsString());
    }

    [Fact]
    public void NullString_BecomesNullCell()
    {
        CellValue cv = CellValue.From((string?)null);
        Assert.True(cv.IsNull);
        Assert.Null(cv.AsString());
    }

    [Fact]
    public void TypeMismatchedRead_Throws()
    {
        CellValue cv = CellValue.From(7);
        Assert.Throws<InvalidOperationException>(() => cv.AsDecimal());
        Assert.Throws<InvalidOperationException>(() => cv.AsString());
    }

    [Fact]
    public void ToObject_And_FromObject_Bridge_CommonTypes()
    {
        Assert.Null(CellValue.From((object?)null).ToObject());
        Assert.Equal(7, CellValue.From((object?)7).ToObject());
        Assert.Equal(250L, CellValue.From((object?)250L).ToObject());
        Assert.Equal(1.5m, CellValue.From((object?)1.5m).ToObject());
        Assert.Equal("x", CellValue.From((object?)"x").ToObject());
        Assert.Throws<NotSupportedException>(() => CellValue.From((object?)new object()));
    }

    [Fact]
    public void BuildAndReadScalars_DoesNotAllocate()
    {
        // Warm up JIT + any one-time state.
        long warm = 0;
        for (int i = 0; i < 2_000; i++)
        {
            warm += CellValue.From(i).AsInt32();
            warm += CellValue.From((decimal)i).AsDecimal() == 0m ? 0 : 0;
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        long sink = warm;
        for (int i = 0; i < 5_000; i++)
        {
            sink += CellValue.From(i).AsInt32();
            sink += CellValue.From((long)i).AsInt64();
            CellValue d = CellValue.From((decimal)i);
            sink += d.AsDecimal() == 0m ? 0 : 0;
        }
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(sink != long.MinValue); // keep sink live
        Assert.Equal(0L, allocated);
    }
}
