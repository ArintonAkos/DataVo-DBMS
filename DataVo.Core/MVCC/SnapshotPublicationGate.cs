namespace DataVo.Core.MVCC;

/// <summary>
/// Keeps snapshot reads and version-publishing writes apart at statement granularity. A statement that reads
/// through a transaction snapshot enters as a reader; a write statement that publishes MVCC versions (one that
/// runs while a transaction is open) enters as a writer. Readers share the gate with readers and writers with
/// writers, but the two groups never overlap, so a snapshot never observes a write half-way between its storage
/// change and its version metadata.
/// </summary>
/// <remarks>
/// The gate is phase-fair: a waiting group takes the next turn, so neither side starves. A writer that already
/// holds row or table locks bypasses the turn order (it only waits for active readers, which never wait on
/// locks), which keeps the gate out of any lock cycle. Entries nest per thread; a nested entry of either kind
/// joins the thread's outer entry.
/// </remarks>
internal sealed class SnapshotPublicationGate
{
    private readonly object _sync = new();
    private int _readers;
    private int _writers;
    private int _waitingReaders;
    private int _waitingWriters;
    private bool _readersTurn;

    [ThreadStatic] private static SnapshotPublicationGate? t_owner;
    [ThreadStatic] private static int t_depth;
    [ThreadStatic] private static bool t_writing;

    /// <summary>Enters as a snapshot reader; returns once no version-publishing writer is active.</summary>
    public void EnterRead()
    {
        if (TryNest())
        {
            return;
        }

        lock (_sync)
        {
            _waitingReaders++;
            while (_writers > 0 || (!_readersTurn && _waitingWriters > 0))
            {
                if (_writers > 0)
                {
                    _readersTurn = true;
                }

                Monitor.Wait(_sync);
            }

            _waitingReaders--;
            _readers++;
        }

        Own(writing: false);
    }

    /// <summary>Enters as a version-publishing writer; returns once no snapshot reader is active.</summary>
    /// <param name="holdsLocks">The thread already holds row or table locks, so it must not queue behind waiting readers.</param>
    public void EnterWrite(bool holdsLocks)
    {
        if (TryNest())
        {
            return;
        }

        lock (_sync)
        {
            _waitingWriters++;
            while (_readers > 0 || (!holdsLocks && _readersTurn && _waitingReaders > 0))
            {
                if (_readers > 0)
                {
                    _readersTurn = false;
                }

                Monitor.Wait(_sync);
            }

            _waitingWriters--;
            _writers++;
        }

        Own(writing: true);
    }

    /// <summary>Leaves the gate entered by the matching <see cref="EnterRead"/> or <see cref="EnterWrite"/>.</summary>
    public void Exit()
    {
        if (t_owner != this || t_depth == 0)
        {
            return;
        }

        if (--t_depth > 0)
        {
            return;
        }

        bool writing = t_writing;
        t_owner = null;
        lock (_sync)
        {
            if (writing)
            {
                _writers--;
            }
            else
            {
                _readers--;
            }

            Monitor.PulseAll(_sync);
        }
    }

    private bool TryNest()
    {
        if (t_owner == this && t_depth > 0)
        {
            t_depth++;
            return true;
        }

        return false;
    }

    private void Own(bool writing)
    {
        t_owner = this;
        t_depth = 1;
        t_writing = writing;
    }
}
