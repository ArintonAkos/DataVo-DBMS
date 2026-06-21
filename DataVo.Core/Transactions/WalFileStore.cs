using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using DataVo.Core.Serialization;
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
