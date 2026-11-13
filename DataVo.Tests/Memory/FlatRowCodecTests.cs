using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Tests.Memory;

public class FlatRowCodecTests
{
    private static readonly CellType[] EveryType =
    [
        CellType.Boolean, CellType.Int32, CellType.Int64, CellType.Double, CellType.Decimal,
        CellType.String, CellType.Date, CellType.Vector, CellType.Guid,
    ];

    private static readonly Guid SomeGuid = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff");

    private static CellValue[] EveryTypeRow() =>
    [
        CellValue.From(true), CellValue.From(-42), CellValue.From(long.MinValue + 7), CellValue.From(-0.1),
        CellValue.From(79228162514264337593543950335m), CellValue.From("ada"),
        CellValue.From(DataVoDate.FromDayNumber(739_000)), CellValue.From(new[] { 1f, -2.5f, float.Epsilon }),
        CellValue.From(SomeGuid),
    ];

    [Fact]
    public void EncodeThenDecode_ReturnsEveryColumnType()
    {
        RowLayout layout = RowLayout.Create(EveryType);
        CellValue[] row = EveryTypeRow();
        var record = new byte[layout.RecordBytes];
        var references = new object?[layout.ReferenceCount];

        Assert.True(FlatRowCodec.TryEncode(layout, row, record, references, 0));
        var decoded = new CellValue[row.Length];
        FlatRowCodec.Decode(layout, record, references, 0, decoded);

        AssertSameCells(row, decoded);
        Assert.False(layout.IsLive(record)); // marking the row live is the store's business
    }

    [Fact]
    public void EncodeThenDecode_KeepsNullsInEveryColumn()
    {
        RowLayout layout = RowLayout.Create(EveryType);
        var row = new CellValue[EveryType.Length];
        var record = new byte[layout.RecordBytes];
        var references = new object?[layout.ReferenceCount];

        Assert.True(FlatRowCodec.TryEncode(layout, row, record, references, 0));
        var decoded = new CellValue[row.Length];
        FlatRowCodec.Decode(layout, record, references, 0, decoded);

        Assert.All(decoded, cell => Assert.True(cell.IsNull));
        Assert.All(references, Assert.Null);
    }

    [Fact]
    public void ValueHelpers_ReadWhatARowEncodes()
    {
        RowLayout layout = RowLayout.Create(EveryType);
        var record = new byte[layout.RecordBytes];
        var references = new object?[layout.ReferenceCount];
        FlatRowCodec.TryEncode(layout, EveryTypeRow(), record, references, 0);

        Assert.True(FlatRowCodec.ReadBoolean(record, layout[0].Position));
        Assert.Equal(-42, FlatRowCodec.ReadInt32(record, layout[1].Position));
        Assert.Equal(long.MinValue + 7, FlatRowCodec.ReadInt64(record, layout[2].Position));
        Assert.Equal(-0.1, FlatRowCodec.ReadDouble(record, layout[3].Position));
        Assert.Equal(79228162514264337593543950335m, FlatRowCodec.ReadDecimal(record, layout[4].Position));
        Assert.Equal("ada", FlatRowCodec.ReadString(references, 0, layout[5].Position));
        Assert.Equal(739_000, FlatRowCodec.ReadDate(record, layout[6].Position).DayNumber);
        Assert.Equal([1f, -2.5f, float.Epsilon], FlatRowCodec.ReadVector(references, 0, layout[7].Position));
        Assert.Equal(SomeGuid, FlatRowCodec.ReadGuid(record, layout[8].Position));
    }

    [Fact]
    public void Rows_UseOnlyTheirOwnRunOfASharedReferenceArray()
    {
        RowLayout layout = RowLayout.Create([CellType.String, CellType.Int32, CellType.String]);
        var records = new byte[layout.RecordBytes * 3];
        var references = new object?[layout.ReferenceCount * 3];

        for (int row = 0; row < 3; row++)
        {
            CellValue[] cells = [CellValue.From("a" + row), CellValue.From(row), CellValue.From("b" + row)];
            Assert.True(FlatRowCodec.TryEncode(layout, cells, records.AsSpan(row * layout.RecordBytes), references, row * 2));
        }

        Assert.Equal(["a0", "b0", "a1", "b1", "a2", "b2"], references);
        CellValue middle = FlatRowCodec.ReadCell(layout, 2, records.AsSpan(layout.RecordBytes), references, 2);
        Assert.Equal("b1", middle.AsString());

        FlatRowCodec.ClearReferences(layout, references, 2);
        Assert.Equal(["a0", "b0", null, null, "a2", "b2"], references);
    }

    [Fact]
    public void TryEncode_RefusesACellOfAnotherType_AndLeavesNothingBehind()
    {
        RowLayout layout = RowLayout.Create([CellType.String, CellType.Int64, CellType.Int32]);
        var record = new byte[layout.RecordBytes];
        var references = new object?[layout.ReferenceCount];
        CellValue[] row = [CellValue.From("kept?"), CellValue.From(5L), CellValue.From(5L)];

        Assert.False(FlatRowCodec.TryEncode(layout, row, record, references, 0));

        Assert.True(record.AsSpan().IndexOfAnyExcept((byte)0) < 0);
        Assert.All(references, Assert.Null);
    }

    [Fact]
    public void TryEncode_RejectsTheWrongNumberOfCells()
    {
        RowLayout layout = RowLayout.Create([CellType.Int32, CellType.Int32]);
        var record = new byte[layout.RecordBytes];

        Assert.Throws<ArgumentException>(() => FlatRowCodec.TryEncode(layout, [CellValue.From(1)], record, [], 0));
    }

    [Fact]
    public void Vector_IsStoredWithoutCopying_AndReadBackAsTheSameArray()
    {
        RowLayout layout = RowLayout.Create([CellType.Vector]);
        var record = new byte[layout.RecordBytes];
        var references = new object?[1];
        CellValue cell = CellValue.From(new[] { 3f, 4f });

        FlatRowCodec.TryEncode(layout, [cell], record, references, 0);

        Assert.Same(cell.AsVectorShared(), FlatRowCodec.ReadVector(references, 0, 0));
        Assert.Same(cell.AsVectorShared(), FlatRowCodec.ReadCell(layout, 0, record, references, 0).AsVectorShared());
    }

    /// <summary>Asserts two rows hold the same cells: the same types and the same values.</summary>
    internal static void AssertSameCells(ReadOnlySpan<CellValue> expected, ReadOnlySpan<CellValue> actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Type, actual[i].Type);
            Assert.Equal(expected[i].ToObject(), actual[i].ToObject());
        }
    }
}
