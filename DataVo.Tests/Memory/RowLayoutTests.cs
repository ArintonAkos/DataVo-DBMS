using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Tests.Memory;

public class RowLayoutTests
{
    private static readonly CellType[] PlayerColumns =
        [CellType.Int32, CellType.Boolean, CellType.Int32, CellType.Int64, CellType.Double, CellType.String];

    [Fact]
    public void Create_PacksFixedColumnsWidestFirst_AndKeepsStringsAsReferences()
    {
        RowLayout layout = RowLayout.Create(PlayerColumns);

        Assert.Equal(6, layout.ColumnCount);
        Assert.Equal(0, layout[3].Position);  // Gold
        Assert.Equal(8, layout[4].Position);  // Ratio
        Assert.Equal(16, layout[0].Position); // Id
        Assert.Equal(20, layout[2].Position); // Level
        Assert.Equal(24, layout[1].Position); // Active
        Assert.Equal(25, layout.StatusOffset);
        Assert.Equal(32, layout.RecordBytes);

        Assert.True(layout[5].IsReference);
        Assert.Equal(0, layout[5].Position);
        Assert.Equal(1, layout.ReferenceCount);
    }

    [Fact]
    public void Create_PutsEveryValueOnAMultipleOfItsWidth_WithoutOverlap()
    {
        CellType[] columns =
        [
            CellType.Boolean, CellType.Guid, CellType.Date, CellType.Vector, CellType.Decimal, CellType.Int32,
            CellType.Boolean, CellType.Double, CellType.String, CellType.Int64, CellType.Boolean,
        ];
        RowLayout layout = RowLayout.Create(columns);
        var taken = new bool[layout.RecordBytes];

        for (int ordinal = 0; ordinal < columns.Length; ordinal++)
        {
            RowField field = layout[ordinal];
            Assert.Equal(columns[ordinal], field.Type);
            Assert.Equal(RowLayout.WidthOf(columns[ordinal]), field.Width);
            if (field.IsReference)
            {
                continue;
            }

            Assert.Equal(0, field.Position % field.Width);
            for (int at = field.Position; at < field.Position + field.Width; at++)
            {
                Assert.False(taken[at]);
                taken[at] = true;
            }
        }

        Assert.False(taken[layout.StatusOffset]);
        Assert.Equal(0, layout.RecordBytes % 8);
        Assert.Equal(2, layout.ReferenceCount);
        Assert.Equal(0, layout[3].Position); // the vector
        Assert.Equal(1, layout[8].Position); // the string
    }

    [Fact]
    public void Record_StartsEmpty_AndIsLiveOnlyBetweenMarkLiveAndMarkEmpty()
    {
        RowLayout layout = RowLayout.Create(PlayerColumns);
        Span<byte> record = stackalloc byte[layout.RecordBytes];

        Assert.False(layout.IsLive(record));
        layout.MarkLive(record);
        Assert.True(layout.IsLive(record));
        layout.MarkEmpty(record);
        Assert.False(layout.IsLive(record));
    }

    [Fact]
    public void NullBits_AreIndependentPerColumn_AndLeaveValuesAndStatusAlone()
    {
        CellType[] columns = [.. Enumerable.Repeat(CellType.Int32, 19)];
        RowLayout layout = RowLayout.Create(columns);
        Span<byte> record = stackalloc byte[layout.RecordBytes];
        int[] nulls = [0, 7, 8, 18];

        foreach (int ordinal in nulls)
        {
            layout.SetNull(record, ordinal);
        }

        for (int ordinal = 0; ordinal < columns.Length; ordinal++)
        {
            Assert.Equal(nulls.Contains(ordinal), layout.IsNull(record, ordinal));
        }

        Assert.False(layout.IsLive(record));
        Assert.True(record.Slice(0, layout.StatusOffset).IndexOfAnyExcept((byte)0) < 0);
        Assert.Equal(80, layout.RecordBytes); // 76 of values, the status byte and three bytes of null bits
    }

    [Fact]
    public void Create_RejectsNoColumnsAndTheNullType()
    {
        Assert.Throws<ArgumentException>(() => RowLayout.Create([]));
        Assert.Throws<ArgumentException>(() => RowLayout.Create([CellType.Int32, CellType.Null]));
    }
}
