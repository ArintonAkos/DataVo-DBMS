using System.Text;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

public sealed class SsTableKeyedTests : IDisposable
{
    private readonly string _directory = TestStorageDirectories.Create("sstable_keyed");

    public static TheoryData<LsmReadMode> Modes => new() { LsmReadMode.MemoryMapped, LsmReadMode.PositionalRead };

    public void Dispose()
    {
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public void KeyCodec_MapsIntegerAndGuidStringsToTheirNativeEncodings()
    {
        Assert.Equal(LsmIndexKeyCodec.EncodeInteger(42), LsmIndexKeyCodec.EncodeString("[42]"));
        Assert.Equal(LsmIndexKeyCodec.EncodeInteger(-7), LsmIndexKeyCodec.EncodeString("-7"));
        Guid guid = Guid.NewGuid();
        Assert.Equal(LsmIndexKeyCodec.EncodeGuid(guid), LsmIndexKeyCodec.EncodeString(guid.ToString("D")));

        // Non-canonical number text stays text.
        Assert.Equal(LsmIndexKeyCodec.StringTag, LsmIndexKeyCodec.EncodeString("007")[0]);
        Assert.Equal(LsmIndexKeyCodec.StringTag, LsmIndexKeyCodec.EncodeString("[1.25]")[0]);

        byte[] userKey = LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeString("abc"), 123);
        Assert.Equal(123, LsmIndexKeyCodec.RowIdOf(userKey));
        Assert.Equal(LsmIndexKeyCodec.KeyPrefix(LsmIndexKeyCodec.EncodeString("abc")), LsmIndexKeyCodec.PrefixOf(userKey).ToArray());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void PrefixLookup_ReturnsEveryRowOfAKey_AcrossBlocks(LsmReadMode mode)
    {
        // Key "hot" has 3,000 rows (several blocks); "a" and "hotter" share leading bytes with it.
        var entries = new List<(byte[] UserKey, ulong Sequence, LsmValueType Type)>();
        foreach (string key in new[] { "a", "hot", "hotter", "z" })
        {
            int rows = key == "hot" ? 3_000 : 10;
            for (long rowId = 1; rowId <= rows; rowId++)
            {
                entries.Add((LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeString(key), rowId), 1, LsmValueType.Put));
            }
        }

        string path = Write(entries);
        using SsTableKeyedReader reader = SsTableKeyedReader.Open(path, mode);
        Assert.True(reader.BlockCount > 2);

        Assert.Equal(Enumerable.Range(1, 3_000).Select(i => (long)i), Rows(reader, "hot"));
        Assert.Equal(Enumerable.Range(1, 10).Select(i => (long)i), Rows(reader, "hotter"));
        Assert.Equal(Enumerable.Range(1, 10).Select(i => (long)i), Rows(reader, "a"));
        Assert.Empty(Rows(reader, "ho"));
        Assert.Empty(Rows(reader, "missing"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NewestVersionPerRow_DecidesPresence(LsmReadMode mode)
    {
        byte[] key = LsmIndexKeyCodec.EncodeInteger(5);
        string path = Write(
        [
            (LsmIndexKeyCodec.UserKey(key, 1), 9, LsmValueType.Deletion),
            (LsmIndexKeyCodec.UserKey(key, 1), 3, LsmValueType.Put),
            (LsmIndexKeyCodec.UserKey(key, 2), 4, LsmValueType.Put),
        ]);

        using SsTableKeyedReader reader = SsTableKeyedReader.Open(path, mode);
        var seen = new List<(long RowId, LsmValueType Type)>();
        reader.ForEachWithPrefix(LsmIndexKeyCodec.KeyPrefix(key), seen, static (userKey, _, type, _, list) => list.Add((LsmIndexKeyCodec.RowIdOf(userKey), type)));
        Assert.Equal([(1L, LsmValueType.Deletion), (2L, LsmValueType.Put)], seen);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CorruptBlock_IsDetected(LsmReadMode mode)
    {
        var entries = Enumerable.Range(1, 500)
            .Select(i => (LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeInteger(1), i), 1UL, LsmValueType.Put))
            .ToList();
        string path = Write(entries);
        byte[] bytes = File.ReadAllBytes(path);
        bytes[100] ^= 0x40; // inside the first block
        File.WriteAllBytes(path, bytes);

        using SsTableKeyedReader reader = SsTableKeyedReader.Open(path, mode);
        Assert.Throws<InvalidDataException>(() => Rows(reader, 1));
    }

    [Fact]
    public void Writer_RejectsOutOfOrderKeys()
    {
        using var stream = new MemoryStream();
        using var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.RowIdSize);
        writer.Add(LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeString("b"), 1), 1, LsmValueType.Put, []);
        Assert.Throws<InvalidOperationException>(() =>
            writer.Add(LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeString("a"), 1), 1, LsmValueType.Put, []));
    }

    private string Write(List<(byte[] UserKey, ulong Sequence, LsmValueType Type)> entries)
    {
        entries.Sort((x, y) =>
        {
            int order = x.UserKey.AsSpan().SequenceCompareTo(y.UserKey);
            return order != 0 ? order : y.Sequence.CompareTo(x.Sequence);
        });

        string path = Path.Combine(_directory, $"{Guid.NewGuid():N}.sst");
        using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write);
        using var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.RowIdSize);
        foreach ((byte[] userKey, ulong sequence, LsmValueType type) in entries)
        {
            writer.Add(userKey, sequence, type, []);
        }

        writer.Finish();
        return path;
    }

    private static List<long> Rows(SsTableKeyedReader reader, string key) =>
        Rows(reader, LsmIndexKeyCodec.EncodeString(key));

    private static List<long> Rows(SsTableKeyedReader reader, long key) =>
        Rows(reader, LsmIndexKeyCodec.EncodeInteger(key));

    private static List<long> Rows(SsTableKeyedReader reader, byte[] encodedKey)
    {
        var rows = new List<long>();
        reader.ForEachWithPrefix(LsmIndexKeyCodec.KeyPrefix(encodedKey), rows, static (userKey, _, type, _, list) =>
        {
            if (type == LsmValueType.Put)
            {
                list.Add(LsmIndexKeyCodec.RowIdOf(userKey));
            }
        });
        return rows;
    }
}
