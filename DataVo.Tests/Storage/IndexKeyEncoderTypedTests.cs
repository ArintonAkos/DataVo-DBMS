using DataVo.Core.BTree;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Storage;

/// <summary>
/// GC Reduction Slice 4, P3.1: the shared typed index-key extraction on <see cref="IndexKeyEncoder"/>
/// must produce byte-for-byte identical keys to the legacy dictionary
/// <see cref="IndexKeyEncoder.BuildKeyString(Dictionary{string, object?}, IEnumerable{string})"/> for
/// every catalog type. The dictionary path is the parity oracle.
/// </summary>
public class IndexKeyEncoderTypedTests
{
    private static string TypedKey(ReactiveRowSchema schema, CellValue[] cells, params string[] attributes)
        => IndexKeyEncoder.BuildKeyString(schema, cells, attributes);

    private static string DictKey(Dictionary<string, object?> row, params string[] attributes)
        => IndexKeyEncoder.BuildKeyString(row, attributes);

    [Fact]
    public void IntKey_MatchesDictionary()
    {
        var schema = new ReactiveRowSchema("Id");
        CellValue[] cells = [CellValue.From(-5)];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { ["Id"] = -5 };

        Assert.Equal(DictKey(dict, "Id"), TypedKey(schema, cells, "Id"));
    }

    [Fact]
    public void IntKey_WarmTypedExtraction_DoesNotBoxOrAllocateMaterially()
    {
        var schema = new ReactiveRowSchema("Id");
        CellValue[] cells = [CellValue.From(42)];
        string[] attributes = ["Id"];

        for (int i = 0; i < 1_000; i++)
        {
            _ = IndexKeyEncoder.BuildKeyString(schema, cells, attributes);
        }

        const int measured = 10_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < measured; i++)
        {
            _ = IndexKeyEncoder.BuildKeyString(schema, cells, attributes);
        }

        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / measured;
        Assert.True(perCall <= 96, $"typed INT key allocated {perCall} B per call");
    }

    [Fact]
    public void FloatKey_MatchesDictionary()
    {
        var schema = new ReactiveRowSchema("Score");
        CellValue[] cells = [CellValue.From(1.5d)];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { ["Score"] = 1.5d };

        Assert.Equal(DictKey(dict, "Score"), TypedKey(schema, cells, "Score"));
    }

    [Fact]
    public void BitKey_MatchesDictionary()
    {
        var schema = new ReactiveRowSchema("Active");
        CellValue[] cells = [CellValue.From(true)];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { ["Active"] = true };

        Assert.Equal(DictKey(dict, "Active"), TypedKey(schema, cells, "Active"));
    }

    [Fact]
    public void DateKey_MatchesDictionary()
    {
        var schema = new ReactiveRowSchema("Born");
        var date = new DateOnly(2026, 6, 22);
        CellValue[] cells = [CellValue.From(date)];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { ["Born"] = date };

        Assert.Equal(DictKey(dict, "Born"), TypedKey(schema, cells, "Born"));
    }

    [Fact]
    public void VarcharKey_MatchesDictionary()
    {
        var schema = new ReactiveRowSchema("Name");
        CellValue[] cells = [CellValue.From("Ada")];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { ["Name"] = "Ada" };

        Assert.Equal(DictKey(dict, "Name"), TypedKey(schema, cells, "Name"));
    }

    [Fact]
    public void VectorKey_MatchesDictionary()
    {
        var schema = new ReactiveRowSchema("Embedding");
        float[] vector = [1.0f, 2.5f, -3.0f];
        CellValue[] cells = [CellValue.From(vector)];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { ["Embedding"] = vector };

        Assert.Equal(DictKey(dict, "Embedding"), TypedKey(schema, cells, "Embedding"));
    }

    [Fact]
    public void NullKey_MatchesDictionary()
    {
        var schema = new ReactiveRowSchema("Name");
        CellValue[] cells = [CellValue.Null];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase) { ["Name"] = null };

        Assert.Equal(DictKey(dict, "Name"), TypedKey(schema, cells, "Name"));
    }

    [Fact]
    public void CompositeKey_MatchesDictionary()
    {
        var schema = new ReactiveRowSchema("Id", "Name", "Score");
        CellValue[] cells = [CellValue.From(7), CellValue.From("Ada"), CellValue.From(9.5d)];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["Id"] = 7,
            ["Name"] = "Ada",
            ["Score"] = 9.5d,
        };

        Assert.Equal(DictKey(dict, "Id", "Name"), TypedKey(schema, cells, "Id", "Name"));
        Assert.Equal(DictKey(dict, "Id", "Name", "Score"), TypedKey(schema, cells, "Id", "Name", "Score"));
    }

    [Fact]
    public void EncodedBytes_MatchDictionaryEncoding()
    {
        var schema = new ReactiveRowSchema("Id", "Name");
        CellValue[] cells = [CellValue.From(42), CellValue.From("Bob")];
        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["Id"] = 42,
            ["Name"] = "Bob",
        };

        byte[] dictEncoded = IndexKeyEncoder.Encode(DictKey(dict, "Id", "Name"));
        byte[] typedEncoded = IndexKeyEncoder.Encode(TypedKey(schema, cells, "Id", "Name"));

        Assert.Equal(dictEncoded, typedEncoded);
    }
}
