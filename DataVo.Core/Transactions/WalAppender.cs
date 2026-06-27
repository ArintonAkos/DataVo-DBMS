using System.Buffers.Binary;
using System.Numerics;

namespace DataVo.Core.Transactions;

/// <summary>
/// Binary WAL operation code stored in each fixed-size frame header.
/// </summary>
internal enum WalFrameOperationType : byte
{
    Insert = 1,
    Update = 2,
    Delete = 3,
    TxnCommit = 4,
    Checkpoint = 5,
}

/// <summary>
/// Coordinates for a committed WAL frame inside the appender ring buffer.
/// </summary>
internal readonly struct WalBufferRange
{
    public WalBufferRange(byte[] buffer, int offset, int length)
    {
        Buffer = buffer;
        Offset = offset;
        Length = length;
    }

    public byte[] Buffer { get; }

    public int Offset { get; }

    public int Length { get; }

    public Span<byte> Span => Buffer.AsSpan(Offset, Length);

    public ReadOnlySpan<byte> ReadOnlySpan => Buffer.AsSpan(Offset, Length);
}

/// <summary>
/// Decoded fixed-size WAL frame header.
/// </summary>
internal readonly record struct WalFrameHeader(
    int FrameLength,
    uint Crc32C,
    long Lsn,
    WalFrameOperationType OpType,
    byte Flags,
    long RowId,
    int TableId);

/// <summary>
/// Pre-allocated binary WAL frame appender. Callers serialize payload bytes directly into the
/// reservation slice, then commit to seal the header and CRC32C.
/// </summary>
internal sealed class WalAppender
{
    public const int FrameHeaderSize = 32;

    private readonly byte[] _buffer;
    private readonly object _gate = new();
    private readonly Queue<FrameSlot> _slots = new();
    private int _head;
    private int _tail;
    private long _nextLsn;

    public WalAppender(int capacityBytes)
    {
        if (capacityBytes < FrameHeaderSize)
        {
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), "WAL ring capacity must fit at least one frame header.");
        }

        _buffer = new byte[capacityBytes];
    }

    public WalFrameReservation Reserve(WalFrameOperationType opType, int tableId, long rowId, int payloadLength)
    {
        if (payloadLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(payloadLength), "Payload length cannot be negative.");
        }

        int frameLength = checked(FrameHeaderSize + payloadLength);
        if (frameLength > _buffer.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(payloadLength), "WAL frame cannot exceed ring capacity.");
        }

        lock (_gate)
        {
            int offset = ReserveContiguousSlot(frameLength);
            long lsn = checked(++_nextLsn);
            var slot = new FrameSlot(offset, frameLength, lsn);
            _slots.Enqueue(slot);

            return new WalFrameReservation(this, opType, tableId, rowId, payloadLength, offset, frameLength, lsn, slot);
        }
    }

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
            frame[17],
            BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(20, sizeof(long))),
            BinaryPrimitives.ReadInt32LittleEndian(frame.Slice(28, sizeof(int))));

        return true;
    }

    public static bool ValidateFrame(ReadOnlySpan<byte> frame, WalFrameHeader header)
    {
        if (header.FrameLength < FrameHeaderSize || header.FrameLength > frame.Length)
        {
            return false;
        }

        uint actual = WalCrc32C.HashToUInt32(frame.Slice(8, header.FrameLength - 8));
        return actual == header.Crc32C;
    }

    internal Span<byte> GetPayloadSpan(int offset, int payloadLength)
    {
        return _buffer.AsSpan(offset + FrameHeaderSize, payloadLength);
    }

    private int ReserveContiguousSlot(int length)
    {
        if (_slots.Count == 0)
        {
            if (_tail + length > _buffer.Length)
            {
                _head = 0;
                _tail = 0;
            }

            int emptyOffset = _tail;
            _tail += length;
            return emptyOffset;
        }

        if (_tail >= _head)
        {
            if (_tail + length <= _buffer.Length)
            {
                int offset = _tail;
                _tail += length;
                return offset;
            }

            if (length <= _head)
            {
                _tail = length;
                return 0;
            }

            throw new InvalidOperationException("The WAL ring buffer does not have enough contiguous capacity.");
        }

        if (_tail + length <= _head)
        {
            int offset = _tail;
            _tail += length;
            return offset;
        }

        throw new InvalidOperationException("The WAL ring buffer does not have enough contiguous capacity.");
    }

    internal WalFrame Commit(WalFrameReservation reservation)
    {
        Span<byte> frame = _buffer.AsSpan(reservation.Offset, reservation.FrameLength);
        frame[..FrameHeaderSize].Clear();

        BinaryPrimitives.WriteInt32LittleEndian(frame[..sizeof(int)], reservation.FrameLength);
        BinaryPrimitives.WriteInt64LittleEndian(frame.Slice(8, sizeof(long)), reservation.Lsn);
        frame[16] = (byte)reservation.OpType;
        frame[17] = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(frame.Slice(18, sizeof(ushort)), 0);
        BinaryPrimitives.WriteInt64LittleEndian(frame.Slice(20, sizeof(long)), reservation.RowId);
        BinaryPrimitives.WriteInt32LittleEndian(frame.Slice(28, sizeof(int)), reservation.TableId);

        uint checksum = WalCrc32C.HashToUInt32(frame[8..reservation.FrameLength]);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.Slice(4, sizeof(uint)), checksum);

        lock (_gate)
        {
            reservation.Slot.Committed = true;
        }

        return new WalFrame(this, reservation.Slot, new WalBufferRange(_buffer, reservation.Offset, reservation.FrameLength), reservation.Lsn);
    }

    internal void Release(FrameSlot slot)
    {
        lock (_gate)
        {
            if (_slots.Count == 0)
            {
                throw new InvalidOperationException("WAL frame has already been released.");
            }

            if (!slot.Committed)
            {
                throw new InvalidOperationException("Cannot release an uncommitted WAL frame.");
            }

            slot.Released = true;
            while (_slots.Count != 0 && _slots.Peek().Released)
            {
                _slots.Dequeue();
            }

            _head = _slots.Count == 0 ? _tail : _slots.Peek().Offset;
        }
    }

    internal sealed class FrameSlot
    {
        public FrameSlot(int offset, int length, long lsn)
        {
            Offset = offset;
            Length = length;
            Lsn = lsn;
        }

        public int Offset { get; }

        public int Length { get; }

        public long Lsn { get; }

        public bool Committed { get; set; }

        public bool Released { get; set; }
    }
}

internal sealed class WalFrameReservation
{
    private readonly WalAppender _owner;
    private bool _committed;

    internal WalFrameReservation(
        WalAppender owner,
        WalFrameOperationType opType,
        int tableId,
        long rowId,
        int payloadLength,
        int offset,
        int frameLength,
        long lsn,
        WalAppender.FrameSlot slot)
    {
        _owner = owner;
        OpType = opType;
        TableId = tableId;
        RowId = rowId;
        PayloadLength = payloadLength;
        Offset = offset;
        FrameLength = frameLength;
        Lsn = lsn;
        Slot = slot;
    }

    public Span<byte> PayloadSpan => _owner.GetPayloadSpan(Offset, PayloadLength);

    internal WalFrameOperationType OpType { get; }

    internal int TableId { get; }

    internal long RowId { get; }

    internal int PayloadLength { get; }

    internal int Offset { get; }

    internal int FrameLength { get; }

    internal long Lsn { get; }

    internal WalAppender.FrameSlot Slot { get; }

    public WalFrame Commit()
    {
        if (_committed)
        {
            throw new InvalidOperationException("WAL frame reservation has already been committed.");
        }

        _committed = true;
        return _owner.Commit(this);
    }
}

internal sealed class WalFrame : IDisposable
{
    private readonly WalAppender _owner;
    private readonly WalAppender.FrameSlot _slot;
    private bool _disposed;

    internal WalFrame(WalAppender owner, WalAppender.FrameSlot slot, WalBufferRange range, long lsn)
    {
        _owner = owner;
        _slot = slot;
        Range = range;
        Lsn = lsn;
    }

    public WalBufferRange Range { get; }

    public long Lsn { get; }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _owner.Release(_slot);
    }
}

internal static class WalCrc32C
{
    public static uint HashToUInt32(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;

        while (data.Length >= sizeof(ulong))
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data[..sizeof(ulong)]));
            data = data[sizeof(ulong)..];
        }

        if (data.Length >= sizeof(uint))
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt32LittleEndian(data[..sizeof(uint)]));
            data = data[sizeof(uint)..];
        }

        if (data.Length >= sizeof(ushort))
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt16LittleEndian(data[..sizeof(ushort)]));
            data = data[sizeof(ushort)..];
        }

        if (data.Length != 0)
        {
            crc = BitOperations.Crc32C(crc, data[0]);
        }

        return ~crc;
    }
}
