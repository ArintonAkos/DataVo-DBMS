using System.Buffers.Binary;

namespace DataVo.Core.Transactions;

/// <summary>
/// Binary WAL operation code stored in each fixed-size frame header.
/// </summary>
internal enum WalFrameOperationType : byte
{
    /// <summary>An Lsm commit-log frame: every mutation of one statement or transaction.</summary>
    CommitBatch = 6,
}

/// <summary>
/// Decoded fixed-size WAL frame header.
/// </summary>
/// <param name="FrameLength">Bytes in the frame, header included.</param>
/// <param name="Crc32C">CRC32C over everything after the CRC field.</param>
/// <param name="Lsn">The frame's log sequence number.</param>
/// <param name="OpType">The frame's operation.</param>
/// <param name="OperationCount">How many operations the payload holds.</param>
internal readonly record struct WalFrameHeader(
    int FrameLength,
    uint Crc32C,
    long Lsn,
    WalFrameOperationType OpType,
    long OperationCount);

/// <summary>
/// The binary frame layout of the Lsm commit log: a 32-byte little-endian header followed by the payload.
/// </summary>
/// <remarks>
/// <code>
/// header := i32 frame length | u32 crc32c | i64 lsn | u8 operation | 3 reserved bytes | i64 operation count | 4 reserved bytes
/// </code>
/// The reserved bytes are written as zero.
/// </remarks>
internal static class WalFrameFormat
{
    /// <summary>Size of the fixed frame header in bytes.</summary>
    public const int FrameHeaderSize = 32;

    /// <summary>Decodes the header at the start of <paramref name="frame"/>; false when it is torn or not a frame.</summary>
    public static bool TryReadFrameHeader(ReadOnlySpan<byte> frame, out WalFrameHeader header)
    {
        header = default;
        if (frame.Length < FrameHeaderSize)
        {
            return false;
        }

        int frameLength = BinaryPrimitives.ReadInt32LittleEndian(frame[..4]);
        if (frameLength < FrameHeaderSize || frameLength > frame.Length)
        {
            return false;
        }

        byte opTypeByte = frame[16];
        if (!Enum.IsDefined(typeof(WalFrameOperationType), opTypeByte))
        {
            return false;
        }

        header = new WalFrameHeader(
            frameLength,
            BinaryPrimitives.ReadUInt32LittleEndian(frame.Slice(4, sizeof(uint))),
            BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(8, sizeof(long))),
            (WalFrameOperationType)opTypeByte,
            BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(20, sizeof(long))));

        return true;
    }

    /// <summary>Whether the frame's CRC32C matches its header.</summary>
    public static bool ValidateFrame(ReadOnlySpan<byte> frame, WalFrameHeader header)
    {
        if (header.FrameLength < FrameHeaderSize || header.FrameLength > frame.Length)
        {
            return false;
        }

        uint actual = WalCrc32C.HashToUInt32(frame.Slice(8, header.FrameLength - 8));
        return actual == header.Crc32C;
    }
}
