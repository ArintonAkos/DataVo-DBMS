using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;

namespace DataVo.Core.Transactions;

/// <summary>
/// Persists and restores the transaction ID allocator high-water mark.
/// </summary>
internal sealed class TransactionIdStateStore
{
    private static readonly ConcurrentDictionary<string, object> FileLocks =
        new(StringComparer.OrdinalIgnoreCase);
    private const long MinPersistDelta = 128;
    private static readonly long MinPersistIntervalTicks = TimeSpan.FromMilliseconds(250).Ticks;
    private long _lastPersistedHighWaterMark = -1;
    private long _lastPersistTicks;

    public TransactionIdStateStore(DataVoConfig config)
    {
        FilePath = config.ResolveTransactionIdStateFilePath();
    }

    public string FilePath { get; }

    public long? TryReadHighWaterMark()
    {
        return ExecuteLocked<long?>(() =>
        {
            if (!File.Exists(FilePath))
            {
                return null;
            }

            string content = File.ReadAllText(FilePath, Encoding.UTF8).Trim();
            if (string.IsNullOrWhiteSpace(content))
            {
                return null;
            }

            if (!long.TryParse(content, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value) || value < 0)
            {
                throw new InvalidDataException($"Invalid transaction ID state payload in '{FilePath}'.");
            }

            return value;
        });
    }

    public void PersistHighWaterMark(long highWaterMark)
    {
        if (highWaterMark < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(highWaterMark), "High-water mark must be non-negative.");
        }

        ExecuteLocked(() =>
        {
            long nowTicks = DateTime.UtcNow.Ticks;
            bool shouldPersistByDelta = highWaterMark - _lastPersistedHighWaterMark >= MinPersistDelta;
            bool shouldPersistByInterval = nowTicks - _lastPersistTicks >= MinPersistIntervalTicks;

            if (highWaterMark <= _lastPersistedHighWaterMark)
            {
                return;
            }

            if (!shouldPersistByDelta && !shouldPersistByInterval)
            {
                return;
            }

            EnsureDirectoryExists();

            string tmpPath = FilePath + ".tmp";
            string payload = highWaterMark.ToString(CultureInfo.InvariantCulture);

            using (var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
            {
                writer.Write(payload);
                writer.Flush();
                stream.Flush(true);
            }

            AtomicFileOperations.ReplaceFromTemp(tmpPath, FilePath);

            _lastPersistedHighWaterMark = highWaterMark;
            _lastPersistTicks = nowTicks;
        });
    }

    public void ForcePersistHighWaterMark(long highWaterMark)
    {
        if (highWaterMark < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(highWaterMark), "High-water mark must be non-negative.");
        }

        ExecuteLocked(() =>
        {
            EnsureDirectoryExists();

            string tmpPath = FilePath + ".tmp";
            string payload = highWaterMark.ToString(CultureInfo.InvariantCulture);

            using (var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
            {
                writer.Write(payload);
                writer.Flush();
                stream.Flush(true);
            }

            AtomicFileOperations.ReplaceFromTemp(tmpPath, FilePath);

            _lastPersistedHighWaterMark = highWaterMark;
            _lastPersistTicks = DateTime.UtcNow.Ticks;
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
