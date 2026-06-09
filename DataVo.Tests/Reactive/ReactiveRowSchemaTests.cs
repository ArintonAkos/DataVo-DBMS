using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public class ReactiveRowSchemaTests
{
    [Fact]
    public void ExposesColumnsAndOrdinals_CaseInsensitive()
    {
        var schema = new ReactiveRowSchema("Category", "TotalExposure");

        Assert.Equal(2, schema.ColumnCount);
        Assert.Equal("Category", schema.ColumnAt(0));
        Assert.Equal("TotalExposure", schema.ColumnAt(1));

        Assert.True(schema.TryGetOrdinal("category", out int ordinal));
        Assert.Equal(0, ordinal);
        Assert.True(schema.TryGetOrdinal("TOTALEXPOSURE", out ordinal));
        Assert.Equal(1, ordinal);

        Assert.False(schema.TryGetOrdinal("Missing", out _));
    }

    [Fact]
    public void Columns_SpanMatchesColumnCount()
    {
        var schema = new ReactiveRowSchema("A", "B", "C");
        Assert.Equal(schema.ColumnCount, schema.Columns.Length);
        Assert.Equal("B", schema.Columns[1]);
    }
}
