using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using DataVo.Core.Serialization;
using DataVo.Core.StorageEngine.Disk;
using DataVo.Core.Utils;

namespace DataVo.Core.Transactions;

/// <summary>
/// Provides synchronized low-level access to the write-ahead log file.
/// </summary>
/// <remarks>
/// This class centralizes file locking, JSON line serialization, append operations,
/// and full-file rewrites so higher-level reader and writer components can stay focused
/// on WAL semantics instead of file-system details.
/// </remarks>
/// <example>
/// <code>
/// var store = new WalFileStore("./data/datavo.wal");
/// store.AppendEntry(new WalEntry { TransactionId = Guid.NewGuid() });
/// List&lt;WalEntry&gt; entries = store.ReadEntries();
/// </code>
/// </example>
/// <summary>
/// A decoded binary WAL frame: its fixed-size header plus the application payload bytes that follow it.
/// </summary>
internal readonly record struct WalFrameRecord(WalFrameHeader Header, byte[] Payload);

internal sealed class WalFileStore
{
    internal sealed class WalRecordEnvelope
    {
        public int Version { get; set; }
        public int PayloadLength { get; set; }
        public uint PayloadCrc32 { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    private static readonly ConcurrentDictionary<string, object> FileLocks =
        new(StringComparer.OrdinalIgnoreCase);

    private static readonly FileHandlePool BinaryFrameHandlePool = new(capacity: 128);

    private int _durableFlushCount;

    // Source-gen context built with the WAL object converter so the heterogeneous object? row values
    // serialize without reflection (Native-AOT safe).
    private static readonly DataVoJsonContext WalJson =
        new(new JsonSerializerOptions { Converters = { new WalObjectConverter() } });

    /// <summary>
    /// Initializes a new file store for the specified WAL path.
    /// </summary>
    /// <param name="filePath">The absolute or relative WAL file path.</param>
    public WalFileStore(string filePath)
    {
        FilePath = filePath;
    }

    /// <summary>
    /// Gets the path of the WAL file managed by this store.
    /// </summary>
    public string FilePath { get; }

    internal int DurableFlushCount => Volatile.Read(ref _durableFlushCount);

    /// <summary>
    /// Reads and deserializes every WAL entry currently present in the file.
    /// </summary>
    /// <returns>The ordered list of entries stored in the log file.</returns>
    public List<WalEntry> ReadEntries()
    {
        return ExecuteLocked(ReadEntriesCore);
    }

    /// <summary>
    /// Appends a single WAL entry to the end of the file and forces it to disk.
    /// </summary>
    /// <param name="entry">The entry to persist.</param>
    public void AppendEntry(WalEntry entry)
    {
        ExecuteLocked(() =>
        {
            EnsureDirectoryExists();

            using var stream = new FileStream(FilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            stream.Seek(0, SeekOrigin.End);

            using (var writer = new StreamWriter(stream, new UTF8Encoding(false), leaveOpen: true))
            {
                writer.WriteLine(SerializeWalEntry(entry));
                writer.Flush();
            }

            stream.Flush(true);
        });
    }

    /// <summary>
    /// Appends an already-framed binary WAL record and forces it to disk.
    /// </summary>
    /// <param name="frame">The committed binary frame to persist.</param>
    public void AppendFrame(WalFrame frame)
    {
        AppendSingleFrame(frame, flushToDisk: true);
    }

    internal void AppendFrame(WalFrame frame, bool flushToDisk)
    {
        AppendSingleFrame(frame, flushToDisk);
    }

    internal void AppendFrameBytes(ReadOnlySpan<byte> bytes, bool flushToDisk)
    {
        lock (GetLock())
        {
            EnsureDirectoryExists();
            AppendFrameBytesCore(bytes, flushToDisk);
        }
    }

    internal void FlushToDisk()
    {
        ExecuteLocked(() =>
        {
            EnsureDirectoryExists();
            using FileHandlePool.FileHandleLease lease = BinaryFrameHandlePool.Acquire(FilePath);
            RandomAccess.FlushToDisk(lease.Handle);
            Interlocked.Increment(ref _durableFlushCount);
        });
    }

    /// <summary>
    /// Forces buffered frames to disk WITHOUT holding the store lock across the device flush, so
    /// concurrent appends proceed while the fsync is in flight. This is what makes WAL group commit
    /// group: the leader's fsync no longer blocks follower appends, so the next batch accumulates
    /// during the current flush. Correctness is unaffected because the caller captures its durable
    /// watermark BEFORE invoking this — frames appended during the fsync are simply not yet claimed
    /// durable. The handle lease is acquired under the lock (so it matches the current file across
    /// delete/recreate cycles); a file deleted mid-flush by a completed generation flush is benign,
    /// because that generation's data is already durable in its SSTable.
    /// </summary>
    internal void FlushToDiskConcurrent()
    {
        FileHandlePool.FileHandleLease lease;
        lock (GetLock())
        {
            EnsureDirectoryExists();
            lease = BinaryFrameHandlePool.Acquire(FilePath);
        }

        using (lease)
        {
            try
            {
                RandomAccess.FlushToDisk(lease.Handle);
            }
            catch (ObjectDisposedException)
            {
                // The segment was deleted after its SSTable became durable; nothing left to flush.
                return;
            }

            Interlocked.Increment(ref _durableFlushCount);
        }
    }

    /// <summary>
    /// Appends committed binary WAL frames as one contiguous durable write.
    /// </summary>
    /// <param name="frames">The committed binary frames to persist in LSN order.</param>
    public void AppendFrames(IReadOnlyList<WalFrame> frames)
    {
        AppendFrames(frames, flushToDisk: true);
    }

    internal void AppendFrames(IReadOnlyList<WalFrame> frames, bool flushToDisk)
    {
        ExecuteLocked(() =>
        {
            if (frames.Count == 0)
            {
                return;
            }

            EnsureDirectoryExists();

            int totalLength = 0;
            foreach (WalFrame frame in frames)
            {
                totalLength = checked(totalLength + frame.Range.Length);
            }

            byte[] rented = ArrayPool<byte>.Shared.Rent(totalLength);
            try
            {
                int offset = 0;
                foreach (WalFrame frame in frames)
                {
                    frame.Range.ReadOnlySpan.CopyTo(rented.AsSpan(offset, frame.Range.Length));
                    offset += frame.Range.Length;
                }

                AppendFrameBytesCore(rented.AsSpan(0, totalLength), flushToDisk);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        });
    }

    private void AppendSingleFrame(WalFrame frame, bool flushToDisk)
    {
        ExecuteLocked(() =>
        {
            EnsureDirectoryExists();
            AppendFrameBytesCore(frame.Range.ReadOnlySpan, flushToDisk);
        });
    }

    private void AppendFrameBytesCore(ReadOnlySpan<byte> bytes, bool flushToDisk)
    {
        using FileHandlePool.FileHandleLease lease = BinaryFrameHandlePool.Acquire(FilePath);
        long offset = RandomAccess.GetLength(lease.Handle);
        RandomAccess.Write(lease.Handle, bytes, offset);
        if (flushToDisk)
        {
            RandomAccess.FlushToDisk(lease.Handle);
            Interlocked.Increment(ref _durableFlushCount);
        }
    }

    /// <summary>
    /// Sequentially scans the binary WAL, returning every frame that passes <c>FrameLen</c> and
    /// <c>Crc32C</c> validation. Scanning stops at the first torn tail — a partial header, a frame whose
    /// declared length runs past the file, or a checksum mismatch — so a crash mid-append is tolerated.
    /// </summary>
    /// <returns>The ordered, validated frames preceding the first torn tail.</returns>
    public List<WalFrameRecord> ReadBinaryFrames()
    {
        return ExecuteLocked(() =>
        {
            var records = new List<WalFrameRecord>();
            if (!File.Exists(FilePath))
            {
                return records;
            }

            byte[] bytes = File.ReadAllBytes(FilePath);
            int offset = 0;
            while (TryReadValidFrame(bytes, offset, out WalFrameHeader header))
            {
                ReadOnlySpan<byte> payload = bytes.AsSpan(
                    offset + WalAppender.FrameHeaderSize,
                    header.FrameLength - WalAppender.FrameHeaderSize);
                records.Add(new WalFrameRecord(header, payload.ToArray()));
                offset += header.FrameLength;
            }

            return records;
        });
    }

    /// <summary>
    /// Rewrites the binary WAL so only frames with an LSN greater than <paramref name="checkpointLsn"/>
    /// survive, discarding any torn tail. When no frames remain the file is removed entirely. The rewrite
    /// runs under the shared file lock, so it is atomic with respect to concurrent appends.
    /// </summary>
    /// <param name="checkpointLsn">The highest LSN whose effects are already durable in the data files.</param>
    public void TruncateThroughLsn(long checkpointLsn)
    {
        ExecuteLocked(() =>
        {
            if (!File.Exists(FilePath))
            {
                return;
            }

            byte[] bytes = File.ReadAllBytes(FilePath);
            int offset = 0;
            int keepStart = -1;
            int keepEnd = 0;
            while (TryReadValidFrame(bytes, offset, out WalFrameHeader header))
            {
                if (header.Lsn > checkpointLsn && keepStart < 0)
                {
                    keepStart = offset;
                }

                offset += header.FrameLength;
                keepEnd = offset;
            }

            BinaryFrameHandlePool.Remove(FilePath);

            if (keepStart < 0)
            {
                DeleteIfExistsCore();
                return;
            }

            byte[] retained = bytes.AsSpan(keepStart, keepEnd - keepStart).ToArray();

            string tmpPath = FilePath + ".tmp";
            using (var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                stream.Write(retained, 0, retained.Length);
                stream.Flush(true);
            }

            AtomicFileOperations.ReplaceFromTemp(tmpPath, FilePath);
        });
    }

    /// <summary>
    /// Attempts to decode and CRC-validate the frame starting at <paramref name="offset"/>.
    /// </summary>
    private static bool TryReadValidFrame(byte[] bytes, int offset, out WalFrameHeader header)
    {
        header = default;
        if (offset < 0 || offset + WalAppender.FrameHeaderSize > bytes.Length)
        {
            return false;
        }

        ReadOnlySpan<byte> remaining = bytes.AsSpan(offset);
        if (!WalAppender.TryReadFrameHeader(remaining, out header))
        {
            return false;
        }

        if (header.FrameLength < WalAppender.FrameHeaderSize || offset + header.FrameLength > bytes.Length)
        {
            return false;
        }

        ReadOnlySpan<byte> frame = bytes.AsSpan(offset, header.FrameLength);
        return WalAppender.ValidateFrame(frame, header);
    }

    /// <summary>
    /// Rewrites the entire WAL file with the supplied entries and forces the result to disk.
    /// </summary>
    /// <param name="entries">The entries that should remain in the file.</param>
    public void RewriteEntries(IEnumerable<WalEntry> entries)
    {
        ExecuteLocked(() => RewriteEntriesCore(entries.ToList()));
    }

    /// <summary>
    /// Deletes the WAL file if it exists.
    /// </summary>
    public void DeleteIfExists()
    {
        ExecuteLocked(DeleteIfExistsCore);
    }

    /// <summary>
    /// Executes an operation while holding the file-specific synchronization lock.
    /// </summary>
    /// <param name="action">The action to execute.</param>
    public void ExecuteLocked(Action action)
    {
        lock (GetLock())
        {
            action();
        }
    }

    /// <summary>
    /// Executes a function while holding the file-specific synchronization lock.
    /// </summary>
    /// <typeparam name="T">The function return type.</typeparam>
    /// <param name="func">The function to execute.</param>
    /// <returns>The function result.</returns>
    public T ExecuteLocked<T>(Func<T> func)
    {
        lock (GetLock())
        {
            return func();
        }
    }

    private List<WalEntry> ReadEntriesCore()
    {
        if (!File.Exists(FilePath))
        {
            return [];
        }

        var entries = new List<WalEntry>();
        int lineNumber = 0;
        foreach (var line in File.ReadLines(FilePath))
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            WalEntry entry = DeserializeWalEntry(line, lineNumber);
            if (entry != null)
            {
                entries.Add(entry);
            }
        }

        return entries;
    }

    private void RewriteEntriesCore(List<WalEntry> entries)
    {
        if (entries.Count == 0)
        {
            DeleteIfExistsCore();
            return;
        }

        EnsureDirectoryExists();

        string tmpPath = FilePath + ".tmp";
        using var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None);
        using (var writer = new StreamWriter(stream, new UTF8Encoding(false), leaveOpen: true))
        {
            foreach (var entry in entries)
            {
                writer.WriteLine(SerializeWalEntry(entry));
            }

            writer.Flush();
        }

        stream.Flush(true);

        BinaryFrameHandlePool.Remove(FilePath);
        AtomicFileOperations.ReplaceFromTemp(tmpPath, FilePath);
    }

    private void EnsureDirectoryExists()
    {
        string? directory = Path.GetDirectoryName(FilePath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private void DeleteIfExistsCore()
    {
        BinaryFrameHandlePool.Remove(FilePath);
        if (File.Exists(FilePath))
        {
            File.Delete(FilePath);
        }
    }

    private object GetLock()
    {
        return FileLocks.GetOrAdd(FilePath, _ => new object());
    }

    private static string SerializeWalEntry(WalEntry entry)
    {
        // Prepare the entry by wrapping vector arrays in envelopes before JSON serialization.
        WalEntry prepared = PrepareWalEntryForSerialization(entry);

        string payload = JsonSerializer.Serialize(prepared, WalJson.WalEntry);
        byte[] payloadBytes = Encoding.UTF8.GetBytes(payload);

        var envelope = new WalRecordEnvelope
        {
            Version = 1,
            PayloadLength = payloadBytes.Length,
            PayloadCrc32 = ComputeCrc32(payloadBytes),
            Payload = payload,
        };

        return JsonSerializer.Serialize(envelope, WalJson.WalRecordEnvelope);
    }

    internal static byte[] SerializeWalEntryPayload(WalEntry entry)
    {
        WalEntry prepared = PrepareWalEntryForSerialization(entry);
        return JsonSerializer.SerializeToUtf8Bytes(prepared, WalJson.WalEntry);
    }

    /// <summary>
    /// Decodes a binary WAL frame payload produced by <see cref="SerializeWalEntryPayload"/> back into a
    /// <see cref="WalEntry"/>. Vector envelopes are decoded lazily by <see cref="WalEntry.ToTransactionContext"/>.
    /// </summary>
    internal static WalEntry? DeserializeWalEntryPayload(byte[] payload)
    {
        return JsonSerializer.Deserialize(payload, WalJson.WalEntry);
    }

    private static WalEntry PrepareWalEntryForSerialization(WalEntry entry)
    {
        var prepared = new WalEntry
        {
            TransactionId = entry.TransactionId,
            MvccTransactionId = entry.MvccTransactionId,
            Timestamp = entry.Timestamp,
            DatabaseName = entry.DatabaseName,
            IsCheckpointed = entry.IsCheckpointed,
            Operations = entry.Operations.Select(op => new WalOperation
            {
                OperationType = op.OperationType,
                TableName = op.TableName,
                RowId = op.RowId,
                RowData = op.RowData != null ? PrepareRowDataForSerialization(op.RowData) : null,
                UpdatedColumns = op.UpdatedColumns != null ? PrepareRowDataForSerialization(op.UpdatedColumns) : null,
            }).ToList(),
        };

        return prepared;
    }

    private static Dictionary<string, object?> PrepareRowDataForSerialization(Dictionary<string, object?> row)
    {
        var prepared = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (key, value) in row)
        {
            prepared[key] = PrepareValueForWalSerialization(value);
        }

        return prepared;
    }

    private static object? PrepareValueForWalSerialization(object? value)
    {
        if (TryCoerceRuntimeVector(value, out float[] vector))
        {
            return CreateVectorEnvelope(vector);
        }

        return value;
    }

    private static bool TryCoerceRuntimeVector(object? value, out float[] vector)
    {
        vector = [];

        switch (value)
        {
            case null:
                return false;
            case float[] floatArray:
                vector = [.. floatArray];
                return vector.Length > 0;
            case double[] doubleArray:
                vector = doubleArray.Select(item => (float)item).ToArray();
                return vector.Length > 0;
            case IEnumerable<float> floatEnumerable:
                vector = floatEnumerable.ToArray();
                return vector.Length > 0;
            case IEnumerable<double> doubleEnumerable:
                vector = doubleEnumerable.Select(item => (float)item).ToArray();
                return vector.Length > 0;
            default:
                return false;
        }
    }

    private static Dictionary<string, object> CreateVectorEnvelope(float[] vector)
    {
        byte[] payload = new byte[vector.Length * sizeof(int)];
        for (int i = 0; i < vector.Length; i++)
        {
            int bits = BitConverter.SingleToInt32Bits(vector[i]);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(i * sizeof(int), sizeof(int)), bits);
        }

        return new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["__dvType"] = "vector-f32b64-v1",
            ["dims"] = vector.Length,
            ["data"] = Convert.ToBase64String(payload)
        };
    }

    private static WalEntry DeserializeWalEntry(string line, int lineNumber)
    {
        WalRecordEnvelope? envelope = null;
        try
        {
            envelope = JsonSerializer.Deserialize(line, WalJson.WalRecordEnvelope);
        }
        catch
        {
            // Legacy format fallback handled below.
        }

        if (envelope != null && !string.IsNullOrWhiteSpace(envelope.Payload) && envelope.Version > 0)
        {
            byte[] payloadBytes = Encoding.UTF8.GetBytes(envelope.Payload);
            if (payloadBytes.Length != envelope.PayloadLength)
            {
                throw new InvalidDataException($"WAL corruption at line {lineNumber}: payload length mismatch.");
            }

            uint actualChecksum = ComputeCrc32(payloadBytes);
            if (actualChecksum != envelope.PayloadCrc32)
            {
                throw new InvalidDataException($"WAL corruption at line {lineNumber}: checksum mismatch.");
            }

            WalEntry? entry = JsonSerializer.Deserialize(envelope.Payload, WalJson.WalEntry);
            if (entry == null)
            {
                throw new InvalidDataException($"WAL corruption at line {lineNumber}: invalid payload JSON.");
            }

            return entry;
        }

        WalEntry? legacyEntry = null;
        try
        {
            legacyEntry = JsonSerializer.Deserialize(line, WalJson.WalEntry);
        }
        catch
        {
            // Handled below.
        }

        if (legacyEntry != null)
        {
            return legacyEntry;
        }

        throw new InvalidDataException($"WAL corruption at line {lineNumber}: unrecognized record format.");
    }

    private static uint ComputeCrc32(ReadOnlySpan<byte> data)
    {
        const uint polynomial = 0xEDB88320u;
        uint crc = 0xFFFFFFFFu;

        for (int i = 0; i < data.Length; i++)
        {
            crc ^= data[i];
            for (int bit = 0; bit < 8; bit++)
            {
                uint mask = (uint)-(int)(crc & 1);
                crc = (crc >> 1) ^ (polynomial & mask);
            }
        }

        return ~crc;
    }
}
