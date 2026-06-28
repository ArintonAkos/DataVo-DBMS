using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>A location and byte length for a block inside an SSTable file.</summary>
public readonly record struct SsTableBlockHandle(long Offset, int Length);

/// <summary>
/// Fixed SSTable v1 byte layout primitives.
/// File layout: <c>data blocks | index block | filter block | footer</c>.
/// Footer layout: <c>index handle | filter handle | uint16 version | uint32 magic</c>.
/// </summary>
internal static class SsTableFormat
{
    /// <summary>Magic trailer for DataVo LSM SSTables. Little-endian bytes are "SLVD".</summary>
    public const uint Magic = 0x44564C53U;

    /// <summary>Current SSTable format version.</summary>
    public const ushort Version = 1;

    /// <summary>Serialized block handle size: <c>int64 offset | int32 length</c>.</summary>
    public const int BlockHandleSize = 12;

    /// <summary>Serialized footer size in bytes.</summary>
    public const int FooterSize = (BlockHandleSize * 2) + sizeof(ushort) + sizeof(uint);

    private const int IndexHandleOffset = 0;
    private const int FilterHandleOffset = IndexHandleOffset + BlockHandleSize;
    private const int VersionOffset = FilterHandleOffset + BlockHandleSize;
    private const int MagicOffset = VersionOffset + sizeof(ushort);

    public static void WriteFooter(
        Span<byte> destination,
        SsTableBlockHandle indexBlock,
        SsTableBlockHandle filterBlock)
    {
        if (destination.Length < FooterSize)
        {
            throw new ArgumentException(
                $"Footer destination must be at least {FooterSize} bytes.",
                nameof(destination));
        }

        WriteBlockHandle(destination.Slice(IndexHandleOffset, BlockHandleSize), indexBlock);
        WriteBlockHandle(destination.Slice(FilterHandleOffset, BlockHandleSize), filterBlock);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(VersionOffset, sizeof(ushort)), Version);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(MagicOffset, sizeof(uint)), Magic);
    }

    public static bool TryReadFooter(
        ReadOnlySpan<byte> source,
        out SsTableBlockHandle indexBlock,
        out SsTableBlockHandle filterBlock)
    {
        indexBlock = default;
        filterBlock = default;

        if (source.Length < FooterSize)
        {
            return false;
        }

        ReadOnlySpan<byte> footer = source[^FooterSize..];
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(footer.Slice(MagicOffset, sizeof(uint)));
        if (magic != Magic)
        {
            return false;
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(footer.Slice(VersionOffset, sizeof(ushort)));
        if (version != Version)
        {
            return false;
        }

        SsTableBlockHandle candidateIndexBlock = ReadBlockHandle(footer.Slice(IndexHandleOffset, BlockHandleSize));
        SsTableBlockHandle candidateFilterBlock = ReadBlockHandle(footer.Slice(FilterHandleOffset, BlockHandleSize));
        long dataRegionLength = source.Length - FooterSize;
        if (!IsValidBlockHandle(candidateIndexBlock, dataRegionLength)
            || !IsValidBlockHandle(candidateFilterBlock, dataRegionLength))
        {
            return false;
        }

        indexBlock = candidateIndexBlock;
        filterBlock = candidateFilterBlock;
        return true;
    }

    private static void WriteBlockHandle(Span<byte> destination, SsTableBlockHandle handle)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination[..sizeof(long)], handle.Offset);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(sizeof(long), sizeof(int)), handle.Length);
    }

    private static SsTableBlockHandle ReadBlockHandle(ReadOnlySpan<byte> source)
    {
        long offset = BinaryPrimitives.ReadInt64LittleEndian(source[..sizeof(long)]);
        int length = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(sizeof(long), sizeof(int)));
        return new SsTableBlockHandle(offset, length);
    }

    private static bool IsValidBlockHandle(SsTableBlockHandle handle, long dataRegionLength)
    {
        return handle.Offset >= 0
            && handle.Length > 0
            && handle.Offset <= long.MaxValue - handle.Length
            && handle.Offset + handle.Length <= dataRegionLength;
    }
}
