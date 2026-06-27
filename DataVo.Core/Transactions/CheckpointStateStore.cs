using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;

namespace DataVo.Core.Transactions;

/// <summary>
/// Persists and restores the binary WAL checkpoint LSN — the highest log sequence number whose effects
/// are guaranteed durable in the <c>.dat</c> files. The watermark is written through a temp file and
/// fsync'd, so the persisted value never claims durability that has not actually reached the device.
/// </summary>
internal sealed class CheckpointStateStore
{
    private static readonly ConcurrentDictionary<string, object> FileLocks =
        new(StringComparer.OrdinalIgnoreCase);

    public CheckpointStateStore(DataVoConfig config)
    {
        FilePath = config.ResolveCheckpointStateFilePath();
    }

    public string FilePath { get; }

    /// <summary>
    /// Reads the persisted checkpoint LSN, returning <c>0</c> when no checkpoint has been recorded yet.
    /// </summary>
    public long ReadCheckpointLsn()
    {
        return ExecuteLocked(() =>
        {
            if (!File.Exists(FilePath))
            {
                return 0L;
            }

            string content = File.ReadAllText(FilePath, Encoding.UTF8).Trim();
            if (string.IsNullOrWhiteSpace(content))
            {
                return 0L;
            }

            if (!long.TryParse(content, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value) || value < 0)
            {
                throw new InvalidDataException($"Invalid checkpoint LSN state payload in '{FilePath}'.");
            }

            return value;
        });
    }

    /// <summary>
    /// Durably persists a new checkpoint LSN.
    /// </summary>
    /// <param name="checkpointLsn">The checkpoint LSN to record. Must be non-negative.</param>
    public void Persist(long checkpointLsn)
    {
        if (checkpointLsn < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(checkpointLsn), "Checkpoint LSN must be non-negative.");
        }

        ExecuteLocked(() =>
        {
            EnsureDirectoryExists();

            string tmpPath = FilePath + ".tmp";
            string payload = checkpointLsn.ToString(CultureInfo.InvariantCulture);

            using (var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
            {
                writer.Write(payload);
                writer.Flush();
                stream.Flush(true);
            }

            AtomicFileOperations.ReplaceFromTemp(tmpPath, FilePath);
        });
    }

    private void EnsureDirectoryExists()
    {
        string? directory = Path.GetDirectoryName(FilePath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private void ExecuteLocked(Action action)
    {
        lock (GetLock())
        {
            action();
        }
    }

    private T ExecuteLocked<T>(Func<T> func)
    {
        lock (GetLock())
        {
            return func();
        }
    }

    private object GetLock()
    {
        return FileLocks.GetOrAdd(FilePath, _ => new object());
    }
}
