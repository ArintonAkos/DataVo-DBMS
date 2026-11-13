using System.Buffers.Binary;
using System.Runtime.InteropServices;
using DataVo.Core;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Damage to a version 8 SSTable: bit flips and truncation in the footer, the columnar directory, its chunk
/// records, the chunk groups and the tombstone run, and in the <c>COLUMNAR</c> spec file. Every damaged file must
/// either be refused with <see cref="InvalidDataException"/> (when it is opened, or when the damaged part is read)
/// or read exactly as the undamaged file does. A segment that reads different values without an error would let a
/// columnar aggregate return a wrong answer, and is a failure.
/// </summary>
public sealed partial class SsTableColumnarSegmentTests
{
    /// <summary>What reading a damaged file led to.</summary>
    private enum Outcome
    {
        RefusedAtOpen,
        RefusedAtRead,
        Identical,
    }

    // Three chunk groups, NULLs and NaNs, a tombstone run, and keys with older versions in the same file.
    private byte[] WriteDamageTarget()
    {
        var random = new Random(41);
        string path = Write(Heap, writer =>
        {
            for (long id = 1; id <= 11_000; id++)
            {
                if (id % 23 == 0)
                {
                    writer.Add(id, 7, LsmValueType.Deletion, default);
                    writer.Add(id, 3, LsmValueType.Put, Bytes(0, RandomRow(random), clustered: false));
                    continue;
                }

                writer.Add(id, 7, LsmValueType.Put, Bytes(0, RandomRow(random), clustered: false));
                if (id % 31 == 0)
                {
                    writer.Add(id, 2, LsmValueType.Put, Bytes(0, RandomRow(random), clustered: false));
                }
            }
        });
        return File.ReadAllBytes(path);
    }

    [Fact]
    public void EveryBitFlip_InTheFooter_IsRefused_OrChangesNothing()
    {
        byte[] file = WriteDamageTarget();
        SsTableDamage.Layout layout = SsTableDamage.LayoutOf(file);
        byte[] image = Image(file);
        var outcomes = new Dictionary<Outcome, int>();
        for (long offset = layout.Footer; offset < file.Length; offset++)
        {
            for (int bit = 0; bit < 8; bit++)
            {
                Tally(outcomes, Read(SsTableDamage.Flip(file, offset, (byte)(1 << bit)), image, mapped: bit % 2 == 0, $"footer byte {offset - layout.Footer} bit {bit}"));
            }
        }

        // The footer is not checksummed, but every field of it is checked or leads to a checksummed region.
        Assert.True(outcomes.GetValueOrDefault(Outcome.RefusedAtOpen) > 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void EveryBitFlip_InTheDirectory_IsRefusedWhenTheFileIsOpened(bool mapped)
    {
        byte[] file = WriteDamageTarget();
        SsTableDamage.Layout layout = SsTableDamage.LayoutOf(file);
        byte[] image = Image(file);
        for (long offset = layout.Directory; offset < layout.Directory + layout.DirectoryLength; offset++)
        {
            for (int bit = 0; bit < 8; bit++)
            {
                // Header, spec, chunk records, column records (zone maps included) and the checksum itself.
                Outcome outcome = Read(SsTableDamage.Flip(file, offset, (byte)(1 << bit)), image, mapped, $"directory byte {offset - layout.Directory} bit {bit}");
                Assert.True(outcome == Outcome.RefusedAtOpen, $"directory byte {offset - layout.Directory} bit {bit}: {outcome}");
            }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void EveryByteFlip_InTheTombstoneRun_IsRefusedWhenTheRunIsRead(bool mapped)
    {
        byte[] file = WriteDamageTarget();
        SsTableDamage.Layout layout = SsTableDamage.LayoutOf(file);
        byte[] image = Image(file);
        Assert.True(layout.Directory - layout.Tombstones >= 400 * 8);
        for (long offset = layout.Tombstones; offset < layout.Directory; offset++)
        {
            Outcome outcome = Read(SsTableDamage.Flip(file, offset, (byte)(1 << (int)(offset % 8))), image, mapped, $"tombstone byte {offset - layout.Tombstones}");
            Assert.True(outcome == Outcome.RefusedAtRead, $"tombstone byte {offset - layout.Tombstones}: {outcome}");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ByteFlips_InChunkGroups_AreRefusedWhenTheGroupIsLoaded(bool mapped)
    {
        byte[] file = WriteDamageTarget();
        SsTableDamage.Layout layout = SsTableDamage.LayoutOf(file);
        byte[] image = Image(file);
        Assert.Equal(3, layout.Chunks.Length);

        // The parts of every group: its first and last key, the start and end of each column's values, and each
        // NULL mask; then random bytes anywhere in the groups (padding included).
        var offsets = new List<long>();
        foreach (SsTableColumnarChunk chunk in layout.Chunks)
        {
            offsets.AddRange([chunk.Offset, chunk.Offset + 7, chunk.Offset + (chunk.Rows * chunk.KeyWidth) - 1, chunk.Offset + chunk.Length - 1]);
        }

        foreach (SsTableColumnarColumn column in layout.Columns)
        {
            offsets.AddRange([column.ValuesOffset, column.ValuesOffset + 1, column.ValuesOffset + 63]);
            if (column.NullsOffset != 0)
            {
                offsets.AddRange([column.NullsOffset, column.NullsOffset + 9, column.NullsOffset + 511]);
            }
        }

        var random = new Random(mapped ? 7 : 8);
        SsTableColumnarChunk last = layout.Chunks[^1];
        for (int i = 0; i < 600; i++)
        {
            offsets.Add(random.NextInt64(layout.FirstChunk, last.Offset + last.Length));
        }

        foreach (long offset in offsets)
        {
            Outcome outcome = Read(SsTableDamage.Flip(file, offset, (byte)(1 << random.Next(8))), image, mapped, $"chunk byte {offset}");
            Assert.True(outcome == Outcome.RefusedAtRead, $"chunk byte {offset}: {outcome}");
        }
    }

    [Fact]
    public void EveryTruncation_OfTheSegmentAndWhatFollowsIt_IsRefused()
    {
        byte[] file = WriteDamageTarget();
        SsTableDamage.Layout layout = SsTableDamage.LayoutOf(file);
        byte[] image = Image(file);

        // Every length from the middle of the last chunk group to one byte short, then coarser steps down to empty.
        var lengths = new SortedSet<long>();
        for (long length = layout.Chunks[^1].Offset + 100; length < file.Length; length += length >= layout.Tombstones ? 1 : 97)
        {
            lengths.Add(length);
        }

        for (long length = 0; length < layout.FirstChunk; length += 4_099)
        {
            lengths.Add(length);
        }

        foreach (long length in lengths)
        {
            Outcome outcome = Read(file.AsSpan(0, (int)length).ToArray(), image, mapped: length % 2 == 0, $"truncated to {length} of {file.Length}");
            Assert.True(outcome == Outcome.RefusedAtOpen, $"truncated to {length} of {file.Length}: {outcome}");
        }
    }

    [Fact]
    public void ADirectoryDamagedConsistently_WithItsChecksumRecomputed_IsRefusedByItsStructure()
    {
        byte[] file = WriteDamageTarget();
        SsTableDamage.Layout layout = SsTableDamage.LayoutOf(file);
        int header = (int)layout.Directory;
        int records = header + SsTableColumnarFormat.HeaderSize + SsTableColumnarFormat.PaddedSpecLength(BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(header + 48)));
        int columnRecords = records + (layout.Chunks.Length * SsTableColumnarFormat.ChunkRecordSize);
        int goldRecord = columnRecords + SsTableColumnarFormat.ColumnRecordSize; // slot 1 of the first group

        // Each damage is self-consistent as far as the checksum goes; the directory's own rules must catch it.
        (string What, Action<byte[]> Damage)[] cases =
        [
            ("magic", bytes => bytes[header] ^= 1),
            ("directory version", bytes => WriteU16(bytes, header + 4, 2)),
            ("tombstone key width", bytes => WriteU16(bytes, header + 6, 12)),
            ("spec hash", bytes => WriteU32(bytes, header + 12, ReadU32(bytes, header + 12) ^ 1)),
            ("column count", bytes => WriteI32(bytes, header + 16, 5)),
            ("chunk count", bytes => WriteI32(bytes, header + 20, 2)),
            ("row count", bytes => WriteI64(bytes, header + 24, ReadI64(bytes, header + 24) + 1)),
            ("tombstone offset past the directory", bytes => WriteI64(bytes, header + 32, layout.Directory)),
            ("negative tombstone count", bytes => WriteI64(bytes, header + 40, -1)),
            ("spec length", bytes => WriteI32(bytes, header + 48, ReadI32(bytes, header + 48) - 4)),
            ("chunk keys out of order", bytes => bytes.AsSpan(records + SsTableColumnarFormat.ChunkRecordSize, 16).CopyTo(bytes.AsSpan(records))),
            ("chunk of no rows", bytes => WriteI32(bytes, records + 32, 0)),
            ("chunk of too many rows", bytes => WriteI32(bytes, records + 32, SsTableColumnarFormat.ChunkRows + 1)),
            ("chunk length too short for its keys", bytes => WriteI32(bytes, records + 36, 8)),
            ("chunk offset unaligned", bytes => WriteI64(bytes, records + 40, layout.Chunks[0].Offset + 8)),
            ("chunk past the tombstones", bytes => WriteI64(bytes, records + SsTableColumnarFormat.ChunkRecordSize + 40, layout.Tombstones)),
            ("chunks overlapping", bytes => WriteI64(bytes, records + SsTableColumnarFormat.ChunkRecordSize + 40, layout.Chunks[0].Offset)),
            ("chunk key width", bytes => WriteI32(bytes, records + 52, 12)),
            ("column values before their chunk", bytes => WriteI64(bytes, columnRecords + (6 * SsTableColumnarFormat.ColumnRecordSize), layout.Chunks[0].Offset)),
            ("column values unaligned", bytes => WriteI64(bytes, columnRecords, layout.Columns[0].ValuesOffset + 4)),
            ("column values past their chunk", bytes => WriteI64(bytes, columnRecords, layout.Chunks[0].Offset + layout.Chunks[0].Length - 64)),
            ("a NULL mask the flags deny", bytes => bytes[goldRecord + 32] = 0),
            ("NULL flag without a mask", bytes => WriteI64(bytes, goldRecord + 8, 0)),
        ];

        Assert.True(layout.Columns[1].NullsOffset != 0, "the Gold column of the first group must have a NULL mask for this test");
        foreach ((string what, Action<byte[]> damage) in cases)
        {
            byte[] copy = (byte[])file.Clone();
            damage(copy);
            Rechecksum(copy, layout);
            InvalidDataException error = Assert.Throws<InvalidDataException>(() => SsTableV2Reader.Open(new SsTableDamage.MemoryBlockSource(copy, mapped: false), ownsSource: true).Dispose());
            Assert.False(string.IsNullOrEmpty(error.Message), what);
        }

        // Undamaged, the rechecksummed file still opens: the cases above failed for what they changed.
        byte[] intact = (byte[])file.Clone();
        Rechecksum(intact, layout);
        SsTableV2Reader.Open(new SsTableDamage.MemoryBlockSource(intact, mapped: false), ownsSource: true).Dispose();
    }

    [Fact]
    public void RandomDamage_ToTheDirectory_WithItsChecksumRecomputed_NeverFailsOtherThanAsInvalidData()
    {
        // Past the checksum, a directory is trusted only as far as its structure is checked. Damage that the
        // structure cannot see (a zone map, a key) may read differently, but must never index outside the file or
        // throw anything but InvalidDataException.
        byte[] file = WriteDamageTarget();
        SsTableDamage.Layout layout = SsTableDamage.LayoutOf(file);
        var random = new Random(97);
        for (int i = 0; i < 3_000; i++)
        {
            byte[] copy = (byte[])file.Clone();
            for (int flips = 1 + random.Next(4); flips > 0; flips--)
            {
                copy[layout.Directory + random.Next(layout.DirectoryLength - sizeof(uint))] ^= (byte)(1 << random.Next(8));
            }

            Rechecksum(copy, layout);
            try
            {
                using SsTableV2Reader reader = SsTableV2Reader.Open(new SsTableDamage.MemoryBlockSource(copy, mapped: false), ownsSource: true);
                Image(reader);
            }
            catch (InvalidDataException)
            {
            }
        }
    }

    [Fact]
    public void TheSpecFile_DamagedOrTruncatedAnywhere_LoadsAsNoSpec()
    {
        string directory = Path.Combine(_directory, "spec");
        Directory.CreateDirectory(directory);
        LsmColumnarSpecFile.Save(directory, Clustered);
        string path = Path.Combine(directory, "COLUMNAR");
        byte[] intact = File.ReadAllBytes(path);
        Assert.True(LsmColumnarSpecFile.TryLoad(directory)!.Matches(Clustered));

        for (int offset = 0; offset < intact.Length; offset++)
        {
            for (int bit = 0; bit < 8; bit++)
            {
                File.WriteAllBytes(path, SsTableDamage.Flip(intact, offset, (byte)(1 << bit)));
                Assert.Null(LsmColumnarSpecFile.TryLoad(directory));
            }
        }

        for (int length = 0; length < intact.Length; length++)
        {
            File.WriteAllBytes(path, intact.AsSpan(0, length).ToArray());
            Assert.Null(LsmColumnarSpecFile.TryLoad(directory));
        }

        // A spec whose checksum matches but whose shape does not is refused too, without an exception.
        var random = new Random(5);
        int specLength = intact.Length - sizeof(uint);
        for (int i = 0; i < 2_000; i++)
        {
            byte[] copy = (byte[])intact.Clone();
            copy[random.Next(specLength)] ^= (byte)(1 << random.Next(8));
            BinaryPrimitives.WriteUInt32LittleEndian(copy.AsSpan(specLength), WalCrc32C.HashToUInt32(copy.AsSpan(0, specLength)));
            File.WriteAllBytes(path, copy);
            ColumnarSpec? loaded = LsmColumnarSpecFile.TryLoad(directory);
            Assert.True(loaded is null || !loaded.Matches(Clustered) || copy.AsSpan().SequenceEqual(intact));
        }

        File.WriteAllBytes(path, intact);
        Assert.True(LsmColumnarSpecFile.TryLoad(directory)!.Matches(Clustered));
    }

    // ---- helpers -----------------------------------------------------------------------------------------------

    private static void Tally(Dictionary<Outcome, int> outcomes, Outcome outcome) => outcomes[outcome] = outcomes.GetValueOrDefault(outcome) + 1;

    // Opens a damaged file and reads all of it; fails the test if it reads differently from the intact file.
    private static Outcome Read(byte[] damaged, byte[] intactImage, bool mapped, string what)
    {
        SsTableV2Reader reader;
        try
        {
            reader = SsTableV2Reader.Open(new SsTableDamage.MemoryBlockSource(damaged, mapped), ownsSource: true);
        }
        catch (InvalidDataException)
        {
            return Outcome.RefusedAtOpen;
        }

        using (reader)
        {
            byte[] image;
            try
            {
                image = Image(reader);
            }
            catch (InvalidDataException)
            {
                return Outcome.RefusedAtRead;
            }

            Assert.True(image.AsSpan().SequenceEqual(intactImage), $"{what}: the damaged file opened and read different values without an error");
            return Outcome.Identical;
        }
    }

    private static byte[] Image(byte[] file)
    {
        using SsTableV2Reader reader = SsTableV2Reader.Open(new SsTableDamage.MemoryBlockSource(file, mapped: false), ownsSource: true);
        return Image(reader);
    }

    /// <summary>
    /// Everything a scan can learn from a file, as bytes: the segment's directory (counts, spec, chunk and column
    /// records with their zone maps), every chunk group's keys, values and NULL masks, the tombstone run, and
    /// every row entry.
    /// </summary>
    private static byte[] Image(SsTableV2Reader reader)
    {
        var image = new List<byte>(1 << 20);
        void Add(long value)
        {
            Span<byte> bytes = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
            image.AddRange(bytes);
        }

        SsTableColumnarSegment segment = reader.Columnar ?? throw new InvalidDataException("No segment.");
        Add(segment.Usable ? 1 : 0);
        Add(segment.RowCount);
        Add(segment.ChunkCount);
        Add(segment.TombstoneCount);
        Add(segment.TombstoneKeyWidth);
        Add(segment.Spec.Hash);
        var chunk = new SsTableColumnarChunkReader();
        for (int c = 0; c < segment.ChunkCount; c++)
        {
            SsTableColumnarChunk record = segment.Chunk(c);
            Add((long)record.FirstKey.High);
            Add((long)record.FirstKey.Low);
            Add((long)record.LastKey.High);
            Add((long)record.LastKey.Low);
            Add(record.Rows);
            chunk.Load(segment, c);
            for (int row = 0; row < chunk.Rows; row++)
            {
                RowKey key = chunk.KeyAt(row);
                Add((long)key.High);
                Add((long)key.Low);
            }

            for (int slot = 0; slot < segment.Spec.ColumnCount; slot++)
            {
                SsTableColumnarColumn column = segment.Column(c, slot);
                Add(column.Min);
                Add(column.Max);
                Add(column.Flags);
                ReadOnlySpan<byte> values = segment.Spec.KindOf(slot) switch
                {
                    DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType.Bit => MemoryMarshal.AsBytes(chunk.Bits(slot)),
                    var kind when ColumnarCells.WidthOf(kind) == 8 => MemoryMarshal.AsBytes(chunk.Values<long>(slot)),
                    _ => MemoryMarshal.AsBytes(chunk.Values<int>(slot)),
                };
                image.AddRange(values);
                if (chunk.HasNulls(slot))
                {
                    image.AddRange(MemoryMarshal.AsBytes(chunk.Nulls(slot)));
                }
            }
        }

        image.AddRange(segment.Tombstones());
        SsTableV2Cursor cursor = reader.OpenCursor();
        while (cursor.MoveNext())
        {
            Add((long)cursor.Key.Low);
            Add((long)cursor.Sequence);
            Add((long)cursor.Type);
            image.AddRange(cursor.Value);
        }

        return [.. image];
    }

    // Writes the directory's checksum again, so a damage passes the checksum and meets the structural checks.
    private static void Rechecksum(byte[] file, SsTableDamage.Layout layout)
    {
        Span<byte> directory = file.AsSpan((int)layout.Directory, layout.DirectoryLength);
        BinaryPrimitives.WriteUInt32LittleEndian(directory[^sizeof(uint)..], WalCrc32C.HashToUInt32(directory[..^sizeof(uint)]));
    }

    private static void WriteU16(byte[] bytes, int at, ushort value) => BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(at), value);

    private static void WriteU32(byte[] bytes, int at, uint value) => BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(at), value);

    private static uint ReadU32(byte[] bytes, int at) => BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(at));

    private static void WriteI32(byte[] bytes, int at, int value) => BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(at), value);

    private static int ReadI32(byte[] bytes, int at) => BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(at));

    private static void WriteI64(byte[] bytes, int at, long value) => BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(at), value);

    private static long ReadI64(byte[] bytes, int at) => BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(at));
}
