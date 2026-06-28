using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Writes LSM mutation payloads into binary WAL frames without allocating payload buffers.</summary>
internal sealed class LsmWalWriter
{
    private const int LsmTableId = 0;
    private const long UnknownRowId = 0;

    private readonly WalAppender _appender;
    private readonly WalFileStore _store;
    private readonly LsmWalDurabilityMode _durabilityMode;

    public LsmWalWriter(string walPath, LsmWalDurabilityMode durabilityMode, int capacityBytes = 1 << 20)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(walPath);
        _appender = new WalAppender(capacityBytes);
        _store = new WalFileStore(walPath);
        _durabilityMode = durabilityMode;
    }

    internal int DurableFlushCount => _store.DurableFlushCount;

    public void AppendMutation(
        ReadOnlySpan<byte> userKey,
        ulong seqno,
        LsmValueType valueType,
        ReadOnlySpan<byte> value)
    {
        ValidateMutation(seqno, valueType, value);

        int payloadLength = LsmWalRecordCodec.MeasureSize(userKey, value);
        WalFrameOperationType operationType = valueType == LsmValueType.Deletion
            ? WalFrameOperationType.Delete
            : WalFrameOperationType.Insert;

        WalFrameReservation reservation = _appender.Reserve(operationType, LsmTableId, UnknownRowId, payloadLength);
        LsmWalRecordCodec.Write(reservation.PayloadSpan, userKey, seqno, valueType, value);

        using WalFrame frame = reservation.Commit();
        _store.AppendFrame(frame, flushToDisk: _durabilityMode == LsmWalDurabilityMode.StrictFsync);
    }

    public void ReplayInto(MemTable memTable)
    {
        ArgumentNullException.ThrowIfNull(memTable);

        foreach (WalFrameRecord frame in _store.ReadBinaryFrames())
        {
            _ = LsmWalRecordCodec.Replay(frame.Payload, memTable);
        }
    }

    public void Clear()
    {
        _store.DeleteIfExists();
    }

    private static void ValidateMutation(ulong seqno, LsmValueType valueType, ReadOnlySpan<byte> value)
    {
        if (valueType is not (LsmValueType.Put or LsmValueType.Deletion))
        {
            throw new ArgumentOutOfRangeException(nameof(valueType));
        }

        if (seqno > InternalKey.MaxSequenceNumber)
        {
            throw new ArgumentOutOfRangeException(nameof(seqno));
        }

        if (valueType == LsmValueType.Deletion && !value.IsEmpty)
        {
            throw new ArgumentException("Deletion records must not carry a value.", nameof(value));
        }
    }
}
