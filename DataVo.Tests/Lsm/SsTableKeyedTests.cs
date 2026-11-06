using DataVo.Core;
using System.Text;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>Version 3 blocks (full keys): files written before prefix compression.</summary>
public sealed class SsTableKeyedV3Tests : SsTableKeyedTests
{
    protected override bool Compact => false;
}

/// <summary>Prefix-compressed blocks with restart points (version 5), the default for new files.</summary>
public sealed class SsTableKeyedV5CompactTests : SsTableKeyedTests
{
    protected override bool Compact => true;

    [Theory]
    [MemberData(nameof(Modes))]
    public void ManyVersionsOfOneEntry_AcrossRestartsAndBlocks_AreVisitedOnce(LsmReadMode mode)
    {
        // Decoded keys live in a reused buffer: the "already visited" check must compare against a copy.
        byte[] key = LsmIndexKeyCodec.EncodeInteger(7);
        var entries = new List<(byte[] UserKey, ulong Sequence, LsmValueType Type)>();
        for (ulong sequence = 1; sequence <= 600; sequence++)
        {
            entries.Add((LsmIndexKeyCodec.UserKey(key, 1), sequence, LsmValueType.Put));
        }

        entries.Add((LsmIndexKeyCodec.UserKey(key, 2), 1, LsmValueType.Put));
        string path = Write(entries);

        using SsTableKeyedReader reader = SsTableKeyedReader.Open(path, mode);
        var seen = new List<(long RowId, ulong Sequence)>();
        reader.ForEachWithPrefix(LsmIndexKeyCodec.KeyPrefix(key), seen, static (userKey, sequence, _, _, list) => list.Add(((long)LsmIndexKeyCodec.RowKeyOf(userKey), sequence)));
        Assert.Equal([(1L, 600UL), (2L, 1UL)], seen);
    }

    [Fact]
    public void ValuesAndKeys_RoundTrip_ThroughForEach()
    {
        var expected = new List<(string Key, ulong Sequence, string Value)>();
        string path = Path.Combine(Directory, $"{Guid.NewGuid():N}.sst");
        using (var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write))
        using (var writer = new SsTableKeyedWriter(stream, bloomSuffixLength: 0, compact: true))
        {
            for (int i = 0; i < 2_000; i++)
            {
                string key = $"user-{i:D6}";
                string value = new('v', i % 50);
                writer.Add(Encoding.UTF8.GetBytes(key), (ulong)(i + 1), LsmValueType.Put, Encoding.UTF8.GetBytes(value));
                expected.Add((key, (ulong)(i + 1), value));
            }

            writer.Finish();
        }

        using SsTableKeyedReader reader = SsTableKeyedReader.Open(path, LsmReadMode.PositionalRead);
        var actual = new List<(string Key, ulong Sequence, string Value)>();
        reader.ForEach(actual, static (key, sequence, _, value, list) => list.Add((Encoding.UTF8.GetString(key), sequence, Encoding.UTF8.GetString(value))));
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void CompactIndexFiles_AreMuchSmaller()
    {
        var entries = Enumerable.Range(1, 20_000)
            .Select(i => (LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeInteger(i), i), 5UL, LsmValueType.Put))
            .ToList();
        long compact = new FileInfo(WriteWith(entries, compact: true)).Length;
        long full = new FileInfo(WriteWith(entries, compact: false)).Length;
        Assert.True(compact < full / 2, $"compact {compact} B vs full {full} B");
    }
}

public abstract class SsTableKeyedTests : IDisposable
{
    private readonly string _directory = TestStorageDirectories.Create("sstable_keyed");

    protected abstract bool Compact { get; }

    protected string Directory => _directory;

    public static TheoryData<LsmReadMode> Modes => new() { LsmReadMode.MemoryMapped, LsmReadMode.PositionalRead };

    public void Dispose()
    {
        try
        {
            System.IO.Directory.Delete(_directory, recursive: true);
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
        Assert.Equal(123, (long)LsmIndexKeyCodec.RowKeyOf(userKey));
        Assert.Equal(LsmIndexKeyCodec.KeyPrefix(LsmIndexKeyCodec.EncodeString("abc")), LsmIndexKeyCodec.PrefixOf(userKey).ToArray());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void PrefixLookup_ReturnsEveryRowOfAKey_AcrossBlocks(LsmReadMode mode)
    {
        // Key "hot" has 30,000 rows (several blocks in either format); "a" and "hotter" share leading bytes with it.
        var entries = new List<(byte[] UserKey, ulong Sequence, LsmValueType Type)>();
        foreach (string key in new[] { "a", "hot", "hotter", "z" })
        {
            int rows = key == "hot" ? 30_000 : 10;
            for (long rowId = 1; rowId <= rows; rowId++)
            {
                entries.Add((LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeString(key), rowId), 1, LsmValueType.Put));
            }
        }

        string path = Write(entries);
        using SsTableKeyedReader reader = SsTableKeyedReader.Open(path, mode);
        Assert.True(reader.BlockCount > 2);

        Assert.Equal(Enumerable.Range(1, 30_000).Select(i => (long)i), Rows(reader, "hot"));
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
        reader.ForEachWithPrefix(LsmIndexKeyCodec.KeyPrefix(key), seen, static (userKey, _, type, _, list) => list.Add(((long)LsmIndexKeyCodec.RowKeyOf(userKey), type)));
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
        using var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.RowKeySize);
        writer.Add(LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeString("b"), 1), 1, LsmValueType.Put, []);
        Assert.Throws<InvalidOperationException>(() =>
            writer.Add(LsmIndexKeyCodec.UserKey(LsmIndexKeyCodec.EncodeString("a"), 1), 1, LsmValueType.Put, []));
    }

    private protected string Write(List<(byte[] UserKey, ulong Sequence, LsmValueType Type)> entries) =>
        WriteWith(entries, Compact);

    private protected string WriteWith(List<(byte[] UserKey, ulong Sequence, LsmValueType Type)> entries, bool compact)
    {
        entries.Sort((x, y) =>
        {
            int order = x.UserKey.AsSpan().SequenceCompareTo(y.UserKey);
            return order != 0 ? order : y.Sequence.CompareTo(x.Sequence);
        });

        string path = Path.Combine(_directory, $"{Guid.NewGuid():N}.sst");
        using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write);
        using var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.RowKeySize, compact: compact);
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
                list.Add((long)LsmIndexKeyCodec.RowKeyOf(userKey));
            }
        });
        return rows;
    }
}
