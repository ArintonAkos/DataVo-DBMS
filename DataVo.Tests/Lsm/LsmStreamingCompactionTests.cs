using System.Buffers.Binary;
using DataVo.Core;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Core.StorageEngine.Serialization;
using Xunit.Abstractions;

namespace DataVo.Tests.Lsm;

/// <summary>
/// The streaming compactor writes, byte for byte, the file the earlier compactor wrote after merging every entry
/// in memory (kept here as <see cref="ReferenceCompaction"/>), for row-keyed files with and without a columnar
/// segment, legacy v1 row files, keyed (v3) files and legacy outputs.
/// </summary>
public sealed class LsmStreamingCompactionTests : IDisposable
{
    private static readonly Column[] Columns =
    [
        new() { Name = "Level", Type = "INT" },
        new() { Name = "Gold", Type = "BIGINT" },
        new() { Name = "Ratio", Type = "DOUBLE" },
        new() { Name = "Name", Type = "VARCHAR" },
    ];

    private static readonly ColumnarSpec Spec = new([.. Columns.Select(static c => RowSerializer.ClassifyColumnType(c.Type))], [0, 1, 2], keyOrdinal: -1);

    private readonly string _root = TestStorageDirectories.Create("lsm_streaming_compaction");
    private readonly ITestOutputHelper _output;

    public LsmStreamingCompactionTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    public static TheoryData<int> Seeds() => [.. Enumerable.Range(1, 40)];

    [Theory]
    [MemberData(nameof(Seeds))]
    public void RowKeyedCompaction_WritesTheSameBytesAsTheMaterializingCompactor(int seed)
    {
        var random = new Random(seed);
        bool columnar = seed % 4 != 0;
        bool wideKeys = seed % 3 == 0;
        bool drop = seed % 2 == 0;
        int keySpace = random.Next(1, 4) switch { 1 => 40, 2 => 3_000, _ => 20_000 };
        double deleteRate = seed % 5 == 0 ? 0.95 : 0.25;

        string directory = NewDirectory();
        var manifest = new LsmManifest(Path.Combine(directory, "MANIFEST"));
        int files = random.Next(1, 7);
        for (int i = 0; i < files; i++)
        {
            int level = i == 0 || i < files - 2 ? 0 : 1;
            if (seed % 7 == 0 && i == 0)
            {
                AddLegacyRowFile(directory, manifest, level, random, keySpace);
            }
            else
            {
                AddRowFile(directory, manifest, level, random, keySpace, wideKeys, deleteRate, columnar && random.Next(4) != 0 ? Spec : null);
            }
        }

        AssertSameOutput(directory, manifest, LsmSstableFormat.RowKeyed, columnar ? Spec : null, drop, spillLimit: (seed % 3) switch { 0 => 1, 1 => 70_000, _ => SsTableSpillOptions.DefaultMemoryLimit });
    }

    [Fact]
    public void RowKeyedCompaction_KeepsTheLastDroppedTombstone_WhenEveryRowWasDeleted()
    {
        string directory = NewDirectory();
        var manifest = new LsmManifest(Path.Combine(directory, "MANIFEST"));
        AddRowFile(directory, manifest, 0, [(5, 3, LsmValueType.Put), (9, 4, LsmValueType.Put)], metadataSequence: 4);
        AddRowFile(directory, manifest, 0, [(5, 7, LsmValueType.Deletion), (9, 8, LsmValueType.Deletion)], metadataSequence: 8);

        LsmTableFileMetadata output = AssertSameOutput(directory, manifest, LsmSstableFormat.RowKeyed, Spec, drop: true)!.Value;
        using SsTableV2Reader reader = SsTableV2Reader.Open(Path.Combine(directory, output.FileName));
        Assert.Equal(1, reader.Properties.EntryCount);
        Assert.Equal(1, reader.Properties.TombstoneCount);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void KeyedCompaction_WritesTheSameBytesAsTheMaterializingCompactor(int seed)
    {
        var random = new Random(seed);
        string directory = NewDirectory();
        var manifest = new LsmManifest(Path.Combine(directory, "MANIFEST"));
        for (int i = 0; i < 4; i++)
        {
            AddKeyedFile(directory, manifest, level: i < 3 ? 0 : 1, random);
        }

        AssertSameOutput(directory, manifest, LsmSstableFormat.Keyed, columnar: null, drop: seed % 2 == 0);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public void LegacyCompaction_WritesTheSameBytesAsTheMaterializingCompactor(int seed)
    {
        var random = new Random(seed);
        string directory = NewDirectory();
        var manifest = new LsmManifest(Path.Combine(directory, "MANIFEST"));
        for (int i = 0; i < 3; i++)
        {
            AddLegacyRowFile(directory, manifest, level: 0, random, keySpace: 500);
        }

        AssertSameOutput(directory, manifest, LsmSstableFormat.Legacy, columnar: null, drop: seed == 2);
    }

    [Fact]
    public void Merge_RejectsAnInputThatGoesBackwards()
    {
        static byte[] Key(byte user, ulong sequence)
        {
            var key = new byte[1 + InternalKey.TagSize];
            InternalKey.Write(key, [user], sequence, LsmValueType.Put);
            return key;
        }

        var source = new LsmListMergeSource(0, [(Key(5, 1), []), (Key(3, 1), [])]);
        var merge = new LsmCompactionMerge([source], dropTombstones: false, keepLastRowTombstone: false);
        Assert.True(merge.MoveNext());
        Assert.Throws<InvalidDataException>(() => merge.MoveNext());
    }

    [Fact]
    public void Merge_KeepsTheNewestVersion_AndBreaksTiesByInputOrder()
    {
        static byte[] Key(byte user, ulong sequence)
        {
            var key = new byte[1 + InternalKey.TagSize];
            InternalKey.Write(key, [user], sequence, LsmValueType.Put);
            return key;
        }

        var older = new LsmListMergeSource(0, [(Key(1, 4), [1]), (Key(2, 9), [1])]);
        var newer = new LsmListMergeSource(1, [(Key(1, 7), [2]), (Key(2, 9), [2])]);
        var merge = new LsmCompactionMerge([older, newer], dropTombstones: false, keepLastRowTombstone: false);
        var emitted = new List<(byte User, ulong Sequence, byte Value)>();
        while (merge.MoveNext())
        {
            emitted.Add((merge.InternalKey[0], InternalKey.Sequence(merge.InternalKey), merge.Value[0]));
        }

        Assert.Equal([(1, 7UL, 2), (2, 9UL, 1)], emitted);
    }

    [Fact]
    public void OpeningATable_DeletesSpillFilesACrashLeftBehind()
    {
        string root = NewDirectory();
        string tableDirectory;
        using (var engine = new LsmStorageEngine(root, LsmWalDurabilityMode.RelaxedOsBuffer))
        {
            engine.InsertRow("db", "t", Row(new Random(1)));
            engine.FlushTableForTesting("db", "t");
            tableDirectory = Path.GetDirectoryName(Directory.GetFiles(root, "*.sst", SearchOption.AllDirectories).Single())!;
        }

        string leftover = Path.Combine(tableDirectory, Guid.NewGuid().ToString("N") + SsTableSpillOptions.Extension);
        File.WriteAllBytes(leftover, new byte[100]);
        using (var engine = new LsmStorageEngine(root, LsmWalDurabilityMode.RelaxedOsBuffer))
        {
            Assert.Single(engine.ReadAllRows("db", "t"));
        }

        Assert.False(File.Exists(leftover));
        Assert.Single(Directory.GetFiles(tableDirectory, "*.sst"));
    }

    [Fact]
    public void Compaction_AllocatesNoMemoryPerRow()
    {
        (long small, long smallReference) = MeasureCompaction(rows: 50_000);
        (long large, long largeReference) = MeasureCompaction(rows: 400_000);
        _output.WriteLine($"50K rows: {small:N0} B (reference {smallReference:N0} B); 400K rows: {large:N0} B (reference {largeReference:N0} B)");

        // What is left grows with the output's size but by less than a byte per row: the Bloom filter (10 bits
        // per key), the block index and the columnar directory. The in-memory merge allocated ~300 B per row.
        Assert.True(large - small < 350_000L, $"{large - small:N0} B more for 350K more rows");
        Assert.True(large * 10 < largeReference, $"{large:N0} B against {largeReference:N0} B for the in-memory merge");
    }

    private (long Streaming, long Reference) MeasureCompaction(int rows)
    {
        string directory = NewDirectory();
        var manifest = new LsmManifest(Path.Combine(directory, "MANIFEST"));
        var random = new Random(rows);
        byte[] row = Row(random);
        for (int file = 0; file < 2; file++)
        {
            // Two files with the same keys: the second replaces every row of the first.
            var entries = Enumerable.Range(0, rows / 2).Select(i => ((RowKey)i, (ulong)(file * rows) + (ulong)i + 1, LsmValueType.Put)).ToList();
            WriteRowFile(directory, manifest, 0, entries, entries.ToDictionary(static e => (e.Item1, e.Item2), _ => row), (ulong)((file + 1) * rows), Spec);
        }

        string copy = NewDirectory();
        foreach (string file in Directory.EnumerateFiles(directory, "*.sst"))
        {
            File.Copy(file, Path.Combine(copy, Path.GetFileName(file)));
        }

        var compactor = new LsmCompactor(directory, manifest) { Format = LsmSstableFormat.RowKeyed, Columnar = Spec, SpillMemoryLimit = 64 * 1024 };
        IReadOnlyList<LsmTableFileMetadata> inputs = manifest.GetLiveFiles(0);
        long before = GC.GetAllocatedBytesForCurrentThread();
        compactor.CompactLevel(0, 1);
        long streaming = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        ReferenceCompaction.Run(copy, inputs, LsmSstableFormat.RowKeyed, Spec, dropTombstones: false);
        return (streaming, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    // Runs the streaming compactor and, on a copy of the inputs made beforehand, the reference; both outputs
    // must be the same bytes (or both absent).
    private LsmTableFileMetadata? AssertSameOutput(string directory, LsmManifest manifest, LsmSstableFormat format, ColumnarSpec? columnar, bool drop, long spillLimit = SsTableSpillOptions.DefaultMemoryLimit)
    {
        string copy = NewDirectory();
        foreach (string file in Directory.EnumerateFiles(directory, "*.sst"))
        {
            File.Copy(file, Path.Combine(copy, Path.GetFileName(file)));
        }

        var compactor = new LsmCompactor(directory, manifest) { Format = format, Columnar = columnar, SpillMemoryLimit = spillLimit };
        LsmCompactionResult result = compactor.CompactLevel(0, 1, drop)!.Value;
        Assert.Empty(Directory.EnumerateFiles(directory, "*" + SsTableSpillOptions.Extension));

        byte[]? expected = ReferenceCompaction.Run(copy, result.DeletedFiles, format, columnar, drop);
        if (expected is null)
        {
            Assert.Null(result.OutputFile);
            return null;
        }

        LsmTableFileMetadata output = Assert.NotNull(result.OutputFile);
        Assert.Equal(expected, File.ReadAllBytes(Path.Combine(directory, output.FileName)));
        return output;
    }

    private string NewDirectory()
    {
        string path = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static byte[] Row(Random random)
    {
        CellValue ratio = random.Next(10) switch
        {
            0 => CellValue.Null,
            1 => CellValue.From(double.NaN),
            _ => CellValue.From(random.NextDouble() * 100),
        };
        return RowSerializer.SerializeCells(Columns, [
            random.Next(8) == 0 ? CellValue.Null : CellValue.From(random.Next(1000)),
            CellValue.From((long)random.Next()),
            ratio,
            CellValue.From("p" + random.Next(100)),
        ]);
    }

    private static void AddRowFile(string directory, LsmManifest manifest, int level, Random random, int keySpace, bool wideKeys, double deleteRate, ColumnarSpec? spec)
    {
        var entries = new List<(RowKey Key, ulong Sequence, LsmValueType Type)>();
        var values = new Dictionary<(RowKey, ulong), byte[]>();
        foreach (int id in Enumerable.Range(0, random.Next(1, Math.Min(keySpace, 9_000))).Select(_ => random.Next(keySpace)).Distinct().Order())
        {
            RowKey key = wideKeys && id % 3 == 0 ? new RowKey((ulong)(id % 7) + 1, (ulong)id) : (RowKey)id;
            int versions = random.Next(1, 4);
            ulong sequence = (ulong)random.Next(1, 60) * 3 + 2;
            for (int v = 0; v < versions; v++, sequence -= (ulong)random.Next(1, 3))
            {
                LsmValueType type = random.NextDouble() < deleteRate ? LsmValueType.Deletion : LsmValueType.Put;
                entries.Add((key, sequence, type));
                values[(key, sequence)] = type == LsmValueType.Put ? Row(random) : [];
            }
        }

        entries.Sort(static (x, y) => x.Key != y.Key ? x.Key.CompareTo(y.Key) : y.Sequence.CompareTo(x.Sequence));
        WriteRowFile(directory, manifest, level, entries, values, random.Next(3) == 0 ? null : (ulong)random.Next(1, 200), spec);
    }

    private static void AddRowFile(string directory, LsmManifest manifest, int level, (long Id, ulong Sequence, LsmValueType Type)[] rows, ulong metadataSequence)
    {
        var random = new Random(1);
        var values = rows.ToDictionary(static r => ((RowKey)r.Id, r.Sequence), r => r.Type == LsmValueType.Put ? Row(random) : []);
        WriteRowFile(directory, manifest, level, [.. rows.Select(static r => ((RowKey)r.Id, r.Sequence, r.Type))], values, metadataSequence, Spec);
    }

    private static void WriteRowFile(
        string directory,
        LsmManifest manifest,
        int level,
        List<(RowKey Key, ulong Sequence, LsmValueType Type)> entries,
        Dictionary<(RowKey, ulong), byte[]> values,
        ulong? metadataSequence,
        ColumnarSpec? spec)
    {
        long number = manifest.AllocateFileNumber();
        string name = $"{number:D6}.sst";
        long length;
        using (FileStream stream = File.Create(Path.Combine(directory, name)))
        using (var writer = new SsTableV2Writer(stream, columnar: spec))
        {
            foreach ((RowKey key, ulong sequence, LsmValueType type) in entries)
            {
                writer.Add(key, sequence, type, values[(key, sequence)]);
            }

            if (metadataSequence is { } sequenceOfMetadata)
            {
                writer.SetTableMetadata(1_000 + (long)sequenceOfMetadata, entries.Count, sequenceOfMetadata);
            }

            length = writer.Finish();
        }

        AddToManifest(manifest, number, level, name, length,
            LsmRowKeyCodec.RowInternalKey(entries[0].Key, entries[0].Sequence, entries[0].Type),
            LsmRowKeyCodec.RowInternalKey(entries[^1].Key, entries[^1].Sequence, entries[^1].Type));
    }

    private static void AddLegacyRowFile(string directory, LsmManifest manifest, int level, Random random, int keySpace)
    {
        var entries = new List<(byte[] InternalKey, byte[] Value)>();
        foreach (int id in Enumerable.Range(0, random.Next(1, 400)).Select(_ => random.Next(keySpace)).Distinct())
        {
            var userKey = new byte[8];
            InternalKey.EncodeInt64UserKey(userKey, id);
            bool put = random.Next(4) != 0;
            var internalKey = new byte[16];
            InternalKey.Write(internalKey, userKey, (ulong)random.Next(1, 300), put ? LsmValueType.Put : LsmValueType.Deletion);
            entries.Add((internalKey, put ? Row(random) : []));
        }

        var metadataKey = LsmRowKeyCodec.MetadataInternalKey((ulong)random.Next(1, 300));
        entries.Add((metadataKey, LsmRowKeyCodec.EncodeTableMetadata(5_000, entries.Count)));
        entries.Sort(static (x, y) => InternalKey.Compare(x.InternalKey, y.InternalKey));

        var writer = new SsTableWriter(entries.Count);
        foreach ((byte[] key, byte[] value) in entries)
        {
            writer.Add(key, value);
        }

        byte[] bytes = writer.Finish();
        long number = manifest.AllocateFileNumber();
        string name = $"{number:D6}.sst";
        File.WriteAllBytes(Path.Combine(directory, name), bytes);
        byte[][] rowKeys = [.. entries.Select(static e => e.InternalKey).Where(static k => LsmRowKeyCodec.IsRowKey(InternalKey.UserKey(k)))];
        AddToManifest(manifest, number, level, name, bytes.Length, rowKeys.Length > 0 ? rowKeys[0] : entries[0].InternalKey, rowKeys.Length > 0 ? rowKeys[^1] : entries[^1].InternalKey);
    }

    private static byte[] KeyedKey(int key, int suffix)
    {
        var bytes = new byte[2 + 4 + 8];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, 4);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(2), key);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(6), suffix);
        return bytes;
    }

    private static void AddKeyedFile(string directory, LsmManifest manifest, int level, Random random)
    {
        var entries = new List<(byte[] UserKey, ulong Sequence, LsmValueType Type, byte[] Value)>();
        foreach ((int key, int suffix) in Enumerable.Range(0, random.Next(1, 3_000)).Select(_ => (random.Next(800), random.Next(3))).Distinct())
        {
            bool put = random.Next(3) != 0;
            entries.Add((KeyedKey(key, suffix), (ulong)random.Next(1, 500), put ? LsmValueType.Put : LsmValueType.Deletion, put ? [(byte)key, (byte)suffix] : []));
        }

        entries.Sort(static (x, y) =>
        {
            int order = x.UserKey.AsSpan().SequenceCompareTo(y.UserKey);
            return order != 0 ? order : y.Sequence.CompareTo(x.Sequence);
        });

        long number = manifest.AllocateFileNumber();
        string name = $"{number:D6}.sst";
        long length;
        using (FileStream stream = File.Create(Path.Combine(directory, name)))
        using (var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.BloomSuffix))
        {
            foreach ((byte[] userKey, ulong sequence, LsmValueType type, byte[] value) in entries)
            {
                writer.Add(userKey, sequence, type, value);
            }

            length = writer.Finish();
        }

        byte[] Internal((byte[] UserKey, ulong Sequence, LsmValueType Type, byte[] Value) e)
        {
            var key = new byte[e.UserKey.Length + InternalKey.TagSize];
            InternalKey.Write(key, e.UserKey, e.Sequence, e.Type);
            return key;
        }

        AddToManifest(manifest, number, level, name, length, Internal(entries[0]), Internal(entries[^1]));
    }

    private static void AddToManifest(LsmManifest manifest, long number, int level, string name, long length, byte[] smallest, byte[] largest)
    {
        var edit = new LsmVersionEdit();
        edit.AddFile(new LsmTableFileMetadata(number, level, smallest, largest, length, name));
        manifest.ApplyEdit(edit);
    }
}
