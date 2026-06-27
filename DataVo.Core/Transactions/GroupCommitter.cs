using System.Threading.Channels;

namespace DataVo.Core.Transactions;

/// <summary>
/// Dedicated WAL writer that batches committed binary frames behind one durable flush.
/// </summary>
internal sealed class GroupCommitter : IAsyncDisposable
{
    private readonly WalFileStore _fileStore;
    private readonly TimeSpan _batchDelay;
    private readonly Channel<PendingCommit> _channel;
    private readonly SortedDictionary<long, PendingCommit> _pendingByLsn = [];
    private readonly Thread _writerThread;
    private long _nextWriteLsn = 1;
    private long _durableLsn;
    private Exception? _writerException;
    private bool _disposed;

    public GroupCommitter(WalFileStore fileStore, TimeSpan? batchDelay = null)
    {
        _fileStore = fileStore;
        _batchDelay = batchDelay ?? TimeSpan.FromMilliseconds(1);
        _channel = Channel.CreateUnbounded<PendingCommit>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

        _writerThread = new Thread(WriterLoop)
        {
            IsBackground = true,
            Name = "DataVo WAL group commit writer",
        };
        _writerThread.Start();
    }

    public long DurableLsn => Volatile.Read(ref _durableLsn);

    public ValueTask CommitAsync(WalFrame frame, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (Volatile.Read(ref _writerException) is Exception writerException)
        {
            frame.Dispose();
            throw new InvalidOperationException("The WAL group committer writer has failed.", writerException);
        }

        var pending = new PendingCommit(frame);
        if (!_channel.Writer.TryWrite(pending))
        {
            frame.Dispose();
            throw new InvalidOperationException("The WAL group committer is closed.");
        }

        if (!cancellationToken.CanBeCanceled)
        {
            return new ValueTask(pending.Task);
        }

        return new ValueTask(pending.Task.WaitAsync(cancellationToken));
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _channel.Writer.TryComplete();
        await Task.Run(() => _writerThread.Join()).ConfigureAwait(false);
    }

    private void WriterLoop()
    {
        try
        {
            while (_channel.Reader.WaitToReadAsync().AsTask().GetAwaiter().GetResult())
            {
                DrainAvailableChannelItems();

                if (_batchDelay > TimeSpan.Zero)
                {
                    Thread.Sleep(_batchDelay);
                    DrainAvailableChannelItems();
                }

                FlushContiguousFrames();
            }

            DrainAvailableChannelItems();
            FlushContiguousFrames();

            if (_pendingByLsn.Count > 0)
            {
                FailPending(new InvalidOperationException("The WAL group committer stopped with a non-contiguous LSN gap."));
            }
        }
        catch (Exception ex)
        {
            Volatile.Write(ref _writerException, ex);
            _channel.Writer.TryComplete(ex);
            FailPending(ex);
        }
    }

    private void DrainAvailableChannelItems()
    {
        while (_channel.Reader.TryRead(out PendingCommit? pending))
        {
            long lsn = pending.Frame.Lsn;
            if (lsn < _nextWriteLsn || _pendingByLsn.ContainsKey(lsn))
            {
                pending.Fail(new InvalidOperationException($"Duplicate or stale WAL frame LSN {lsn}."));
                continue;
            }

            _pendingByLsn.Add(lsn, pending);
        }
    }

    private void FlushContiguousFrames()
    {
        List<PendingCommit>? batch = null;
        while (_pendingByLsn.TryGetValue(_nextWriteLsn, out PendingCommit? pending))
        {
            _pendingByLsn.Remove(_nextWriteLsn);
            batch ??= [];
            batch.Add(pending);
            _nextWriteLsn++;
        }

        if (batch == null || batch.Count == 0)
        {
            return;
        }

        try
        {
            _fileStore.AppendFrames(batch.Select(item => item.Frame).ToList());
            long durableLsn = batch[^1].Frame.Lsn;
            Volatile.Write(ref _durableLsn, durableLsn);

            foreach (PendingCommit pending in batch)
            {
                pending.Succeed();
            }
        }
        catch (Exception ex)
        {
            foreach (PendingCommit pending in batch)
            {
                pending.Fail(ex);
            }
        }
    }

    private void FailPending(Exception ex)
    {
        foreach (PendingCommit pending in _pendingByLsn.Values)
        {
            pending.Fail(ex);
        }

        _pendingByLsn.Clear();

        while (_channel.Reader.TryRead(out PendingCommit? pending))
        {
            pending.Fail(ex);
        }
    }

    private sealed class PendingCommit
    {
        private readonly TaskCompletionSource _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _completed;

        public PendingCommit(WalFrame frame)
        {
            Frame = frame;
        }

        public WalFrame Frame { get; }

        public Task Task => _completion.Task;

        public void Succeed()
        {
            if (Interlocked.Exchange(ref _completed, 1) != 0)
            {
                return;
            }

            try
            {
                _completion.SetResult();
            }
            finally
            {
                Frame.Dispose();
            }
        }

        public void Fail(Exception ex)
        {
            if (Interlocked.Exchange(ref _completed, 1) != 0)
            {
                return;
            }

            try
            {
                _completion.SetException(ex);
            }
            finally
            {
                Frame.Dispose();
            }
        }
    }
}
