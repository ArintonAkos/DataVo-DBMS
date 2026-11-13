using System.Buffers.Binary;
using DataVo.Core.Memory.Flat;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Byte layout of the columnar segment of a version 8 SSTable: some columns of the file's rows a second time,
/// as dense runs of one type, so a scan of a few columns reads them without decoding rows. All integers are
/// little-endian.
/// </summary>
/// <remarks>
/// <code>
/// segment   := chunk group* | tombstone keys | directory
/// group     := keys | (values | nulls?)[column]            every part starts at a multiple of 64 bytes
/// keys      := u64 low half [rows] (integer keys) or (u64 high | u64 low)[rows], ascending
/// values    := one value per row, padded with zeros to a multiple of 64 values; a BIT column is a bit mask
/// nulls     := a bit per row, set where the value is NULL; left out when the group has no NULL in the column
/// tombstone := keys deleted by this file, ascending, in the width the header gives
/// directory := header(64) | spec, padded to 8 | chunk(56)[chunks] | column(40)[chunks x columns] | u32 crc32c
/// </code>
/// A group holds up to 4,096 rows: the rows of the file whose newest version is a put, in key order. A row
/// whose newest version in the file is a deletion has no position; its key is in the tombstone run instead, so
/// a reader can tell which rows of older files this file replaces or removes without opening a row block.
/// Offsets are positions in the file.
/// </remarks>
internal static class SsTableColumnarFormat
{
    /// <summary>"DVCS".</summary>
    public const uint Magic = 0x5343_5644;

    public const ushort DirectoryVersion = 1;
    public const int HeaderSize = 64;
    public const int ChunkRecordSize = 56;
    public const int ColumnRecordSize = 40;

    /// <summary>Every part of a chunk group starts at a multiple of this, so vector loads are aligned.</summary>
    public const int Alignment = 64;

    /// <summary>The most rows in one chunk group.</summary>
    public const int ChunkRows = FlatChunk.Slots;

    /// <summary>Header flag: a row could not be represented, so the segment must not answer scans.</summary>
    public const uint UnusableFlag = 1;

    /// <summary>Column flag: the group holds a NULL in the column.</summary>
    public const byte AnyNull = 1;

    /// <summary>Column flag: every row of the group is NULL in the column; min and max mean nothing.</summary>
    public const byte AllNull = 2;

    /// <summary>Column flag: a FLOAT or DOUBLE column holds a NaN in the group; min and max leave NaNs out.</summary>
    public const byte AnyNaN = 4;

    /// <summary>Rounds a length up to a multiple of <see cref="Alignment"/>.</summary>
    public static int AlignUp(int length) => (length + Alignment - 1) & ~(Alignment - 1);

    /// <summary>The directory's length for a spec of <paramref name="specLength"/> bytes.</summary>
    public static long DirectoryLength(int specLength, int chunks, int columns) =>
        HeaderSize + PaddedSpecLength(specLength) + ((long)chunks * ChunkRecordSize) + ((long)chunks * columns * ColumnRecordSize) + sizeof(uint);

    /// <summary>The spec's length in the directory, padded so the records after it are 8-byte aligned.</summary>
    public static int PaddedSpecLength(int specLength) => (specLength + 7) & ~7;
}

/// <summary>The fixed start of a columnar segment's directory.</summary>
/// <param name="TombstoneKeyWidth">8 when every tombstone key is an integer key, otherwise 16.</param>
/// <param name="Flags"><see cref="SsTableColumnarFormat.UnusableFlag"/> or 0.</param>
/// <param name="SpecHash">The hash of the spec the segment was written for.</param>
/// <param name="ColumnCount">The number of columns in every chunk group.</param>
/// <param name="ChunkCount">The number of chunk groups.</param>
/// <param name="RowCount">The number of rows in all groups.</param>
/// <param name="TombstoneOffset">Where the tombstone keys start.</param>
/// <param name="TombstoneCount">The number of tombstone keys.</param>
/// <param name="SpecLength">The length of the encoded spec that follows the header.</param>
/// <param name="TombstoneCrc">CRC32C of the tombstone keys.</param>
internal readonly record struct SsTableColumnarHeader(
    ushort TombstoneKeyWidth,
    uint Flags,
    uint SpecHash,
    int ColumnCount,
    int ChunkCount,
    long RowCount,
    long TombstoneOffset,
    long TombstoneCount,
    int SpecLength,
    uint TombstoneCrc)
{
    public void WriteTo(Span<byte> destination)
    {
        destination[..SsTableColumnarFormat.HeaderSize].Clear();
        BinaryPrimitives.WriteUInt32LittleEndian(destination, SsTableColumnarFormat.Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[4..], SsTableColumnarFormat.DirectoryVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[6..], TombstoneKeyWidth);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[8..], Flags);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[12..], SpecHash);
        BinaryPrimitives.WriteInt32LittleEndian(destination[16..], ColumnCount);
        BinaryPrimitives.WriteInt32LittleEndian(destination[20..], ChunkCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[24..], RowCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[32..], TombstoneOffset);
        BinaryPrimitives.WriteInt64LittleEndian(destination[40..], TombstoneCount);
        BinaryPrimitives.WriteInt32LittleEndian(destination[48..], SpecLength);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[52..], TombstoneCrc);
    }

    /// <exception cref="InvalidDataException">The bytes are not a directory header this code reads.</exception>
    public static SsTableColumnarHeader ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < SsTableColumnarFormat.HeaderSize
            || BinaryPrimitives.ReadUInt32LittleEndian(source) != SsTableColumnarFormat.Magic
            || BinaryPrimitives.ReadUInt16LittleEndian(source[4..]) != SsTableColumnarFormat.DirectoryVersion)
        {
            throw new InvalidDataException("SSTable columnar directory header is missing or of an unknown version.");
        }

        return new(
            BinaryPrimitives.ReadUInt16LittleEndian(source[6..]),
            BinaryPrimitives.ReadUInt32LittleEndian(source[8..]),
            BinaryPrimitives.ReadUInt32LittleEndian(source[12..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[16..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[20..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[24..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[32..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[40..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[48..]),
            BinaryPrimitives.ReadUInt32LittleEndian(source[52..]));
    }
}

/// <summary>One chunk group of a columnar segment.</summary>
/// <param name="FirstKey">The key of the group's first row.</param>
/// <param name="LastKey">The key of the group's last row.</param>
/// <param name="Rows">The number of rows, from 1 to 4,096.</param>
/// <param name="Length">The group's length in bytes.</param>
/// <param name="Offset">Where the group, and its keys, start.</param>
/// <param name="Crc">CRC32C of the group's bytes.</param>
/// <param name="KeyWidth">8 when every key of the group is an integer key, otherwise 16.</param>
internal readonly record struct SsTableColumnarChunk(RowKey FirstKey, RowKey LastKey, int Rows, int Length, long Offset, uint Crc, int KeyWidth)
{
    public void WriteTo(Span<byte> destination)
    {
        SsTableV2Format.WriteKey(destination, FirstKey);
        SsTableV2Format.WriteKey(destination[16..], LastKey);
        BinaryPrimitives.WriteInt32LittleEndian(destination[32..], Rows);
        BinaryPrimitives.WriteInt32LittleEndian(destination[36..], Length);
        BinaryPrimitives.WriteInt64LittleEndian(destination[40..], Offset);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[48..], Crc);
        BinaryPrimitives.WriteInt32LittleEndian(destination[52..], KeyWidth);
    }

    public static SsTableColumnarChunk ReadFrom(ReadOnlySpan<byte> source) => new(
        SsTableV2Format.ReadKey(source),
        SsTableV2Format.ReadKey(source[16..]),
        BinaryPrimitives.ReadInt32LittleEndian(source[32..]),
        BinaryPrimitives.ReadInt32LittleEndian(source[36..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[40..]),
        BinaryPrimitives.ReadUInt32LittleEndian(source[48..]),
        BinaryPrimitives.ReadInt32LittleEndian(source[52..]));
}

/// <summary>One column of one chunk group.</summary>
/// <param name="ValuesOffset">Where the column's values start.</param>
/// <param name="NullsOffset">Where the column's NULL mask starts, or 0 when the group has no NULL in it.</param>
/// <param name="Min">The smallest value that is neither NULL nor NaN, as <see cref="Indexing.Columnar.ColumnarCells.ZoneBits"/> encodes it.</param>
/// <param name="Max">The largest such value, encoded the same way.</param>
/// <param name="Flags">Any of <see cref="SsTableColumnarFormat.AnyNull"/>, <see cref="SsTableColumnarFormat.AllNull"/> and <see cref="SsTableColumnarFormat.AnyNaN"/>.</param>
internal readonly record struct SsTableColumnarColumn(long ValuesOffset, long NullsOffset, long Min, long Max, byte Flags)
{
    public void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination, ValuesOffset);
        BinaryPrimitives.WriteInt64LittleEndian(destination[8..], NullsOffset);
        BinaryPrimitives.WriteInt64LittleEndian(destination[16..], Min);
        BinaryPrimitives.WriteInt64LittleEndian(destination[24..], Max);
        BinaryPrimitives.WriteInt64LittleEndian(destination[32..], Flags);
    }

    public static SsTableColumnarColumn ReadFrom(ReadOnlySpan<byte> source) => new(
        BinaryPrimitives.ReadInt64LittleEndian(source),
        BinaryPrimitives.ReadInt64LittleEndian(source[8..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[16..]),
        BinaryPrimitives.ReadInt64LittleEndian(source[24..]),
        source[32]);
}
