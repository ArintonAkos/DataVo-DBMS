using DataVo.Core.Logging;

namespace DataVo.Core.Transactions;

/// <summary>
/// Background worker that periodically checkpoints the binary WAL in
/// <see cref="DataVo.Core.StorageEngine.Config.IoSchedulerMode.GroupCommit"/>. A checkpoint makes the
/// data files durable, advances the persisted checkpoint LSN, and prunes the now-redundant WAL prefix.
/// </summary>
/// <remarks>
/// Crash-safe ordering is essential and is enforced in <see cref="CheckpointTo"/>:
/// <list type="number">
/// <item>fsync the <c>.dat</c> files, so their contents up to the target LSN are on the device;</item>
/// <item>persist the checkpoint LSN watermark (itself fsync'd);</item>
/// <item>truncate the WAL frames at or below the watermark.</item>
/// </list>
/// A crash between any two steps degrades to replaying slightly more of the WAL on the next startup,
/// never to losing committed data.
/// </remarks>
internal sealed class WalCheckpointer : IDisposable
{
    private readonly Func<long> _durableLsnProvider;
    private readonly Action _flushDataToDisk;
    private readonly CheckpointStateStore _stateStore;
    private readonly WalFileStore _walStore;
    private readonly int _intervalMs;
    private readonly object _checkpointGate = new();
    private readonly ManualResetEventSlim _wake = new(false);
    private long _checkpointLsn;
    private Thread? _thread;
    private volatile bool _disposed;

    public WalCheckpointer(
        Func<long> durableLsnProvider,
        Action flushDataToDisk,
        CheckpointStateStore stateStore,
        WalFileStore walStore,
        int intervalMs)
    {
        _durableLsnProvider = durableLsnProvider;
        _flushDataToDisk = flushDataToDisk;
        _stateStore = stateStore;
        _walStore = walStore;
        _intervalMs = intervalMs <= 0 ? 1000 : intervalMs;
        _checkpointLsn = stateStore.ReadCheckpointLsn();
    }

    /// <summary>
    /// Gets the highest LSN whose effects are guaranteed durable in the data files.
    /// </summary>
    public long CheckpointLsn => Volatile.Read(ref _checkpointLsn);

    /// <summary>
    /// Starts the periodic background checkpoint loop. Must be called after startup recovery completes.
    /// </summary>
    public void Start()
    {
        if (_thread != null)
        {
            return;
        }

        _thread = new Thread(WriterLoop)
        {
            IsBackground = true,
            Name = "DataVo WAL checkpointer",
        };
        _thread.Start();
    }

    /// <summary>
    /// Runs a checkpoint up to the current durable WAL LSN.
    /// </summary>
    /// <returns>The checkpoint LSN in effect after the call.</returns>
    public long Checkpoint()
    {
        return CheckpointTo(_durableLsnProvider());
    }

    /// <summary>
    /// Runs a checkpoint up to <paramref name="targetLsn"/>. No-ops when the target does not advance the
    /// watermark, which makes the call idempotent across repeated invocations and startup recovery.
    /// </summary>
    /// <param name="targetLsn">The LSN to checkpoint through.</param>
    /// <returns>The checkpoint LSN in effect after the call.</returns>
    public long CheckpointTo(long targetLsn)
    {
        lock (_checkpointGate)
        {
            long current = Volatile.Read(ref _checkpointLsn);
            if (targetLsn <= current)
            {
                return current;
            }

            _flushDataToDisk();
            _stateStore.Persist(targetLsn);
            Volatile.Write(ref _checkpointLsn, targetLsn);
            _walStore.TruncateThroughLsn(targetLsn);
            return targetLsn;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _wake.Set();
        _thread?.Join();
        _wake.Dispose();
    }

    private void WriterLoop()
    {
        while (!_disposed)
        {
            _wake.Wait(_intervalMs);
            if (_disposed)
            {
                return;
            }

            try
            {
                Checkpoint();
            }
            catch (Exception ex)
            {
                Logger.Error($"WAL checkpoint failed: {ex.Message}");
            }
        }
    }
}
