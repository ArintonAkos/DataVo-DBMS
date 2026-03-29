using System.Collections.Concurrent;
using System.Text;
using Newtonsoft.Json;

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
    private sealed class WalRecordEnvelope
    {
        public int Version { get; set; }
        public int PayloadLength { get; set; }
        public uint PayloadCrc32 { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    private static readonly ConcurrentDictionary<string, object> FileLocks =
        new(StringComparer.OrdinalIgnoreCase);

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

        if (File.Exists(FilePath))
        {
            File.Replace(tmpPath, FilePath, null);
        }
        else
        {
            File.Move(tmpPath, FilePath, overwrite: true);
        }
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
        string payload = JsonConvert.SerializeObject(entry);
        byte[] payloadBytes = Encoding.UTF8.GetBytes(payload);

        var envelope = new WalRecordEnvelope
        {
            Version = 1,
            PayloadLength = payloadBytes.Length,
            PayloadCrc32 = ComputeCrc32(payloadBytes),
            Payload = payload,
        };

        return JsonConvert.SerializeObject(envelope);
    }

    private static WalEntry DeserializeWalEntry(string line, int lineNumber)
    {
        WalRecordEnvelope? envelope = null;
        try
        {
            envelope = JsonConvert.DeserializeObject<WalRecordEnvelope>(line);
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

            WalEntry? entry = JsonConvert.DeserializeObject<WalEntry>(envelope.Payload);
            if (entry == null)
            {
                throw new InvalidDataException($"WAL corruption at line {lineNumber}: invalid payload JSON.");
            }

            return entry;
        }

        WalEntry? legacyEntry = null;
        try
        {
            legacyEntry = JsonConvert.DeserializeObject<WalEntry>(line);
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
