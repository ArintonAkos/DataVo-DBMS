namespace DataVo.Core.MVCC;

/// <summary>
/// Orders commits against snapshots. Every write unit (one auto-commit statement, or one <c>COMMIT</c>) takes a
/// commit stamp from the transaction-id sequence when it first writes, and keeps it "in flight" until its effects
/// are fully published. A transaction's snapshot is the id it is given at <c>BEGIN</c>: a version is visible when
/// its stamp is lower. <c>BEGIN</c> waits for the write units already in flight, so every stamp below the snapshot
/// belongs to a unit that has finished, and every unit that starts later gets a higher stamp — the snapshot is
/// exactly the set of commits that finished before it began.
/// </summary>
/// <remarks>
/// Whether a write unit records versions at all is decided once, when it takes its stamp: only when a transaction
/// is open can a snapshot need them. A unit that records versions also enters the
/// <see cref="SnapshotPublicationGate"/> as a writer, so snapshot reads never see it half applied.
/// Units are tracked per thread (statements run synchronously on one thread), which keeps the bookkeeping
/// allocation-free.
/// </remarks>
public sealed class CommitClock
{
    private readonly TransactionIdAllocator _ids;
    private readonly Func<long, bool> _anyOtherTransactionOpen;
    private readonly object _sync = new();
    private readonly HashSet<long> _inFlight = [];
    private readonly int _waitTimeoutMs;
    private readonly Action? _versionedUnitRetired;

    [ThreadStatic] private static UnitState? t_unit;

    /// <summary>The thread's current write unit: its nesting depth and, once it writes, its stamp.</summary>
    private sealed class UnitState
    {
        public int Depth;
        public long Stamp;
        public bool Versioned;
        public bool HoldsGate;
        public CommitClock? Owner;
    }

    /// <summary>Creates a clock over an id sequence.</summary>
    /// <param name="ids">The sequence that issues transaction ids and commit stamps.</param>
    /// <param name="anyOtherTransactionOpen">Reports whether any explicit transaction other than the given id (0 for none) is open.</param>
    /// <param name="waitTimeoutMs">How long <c>BEGIN</c> waits for in-flight write units.</param>
    /// <param name="versionedUnitRetired">
    /// Runs after a unit that recorded versions has finished: its versions may have been published after the
    /// transactions that needed them ended, so they can be vacuumed now.
    /// </param>
    public CommitClock(TransactionIdAllocator ids, Func<long, bool> anyOtherTransactionOpen, int waitTimeoutMs = 30000, Action? versionedUnitRetired = null)
    {
        _ids = ids;
        _anyOtherTransactionOpen = anyOtherTransactionOpen;
        _waitTimeoutMs = waitTimeoutMs;
        _versionedUnitRetired = versionedUnitRetired;
    }

    /// <summary>Keeps snapshot reads apart from version-publishing writes.</summary>
    internal SnapshotPublicationGate Gate { get; } = new();

    /// <summary>
    /// Opens (or nests into) the current thread's write unit for a block that takes its stamp before it opens the
    /// storage write batch (for example, to take the stamp before a table lock). Dispose it after the batch.
    /// </summary>
    /// <returns>A scope that closes the unit.</returns>
    internal static UnitScope OpenUnit()
    {
        EnterUnit();
        return default;
    }

    /// <summary>Closes the write unit opened by <see cref="OpenUnit"/>.</summary>
    internal readonly struct UnitScope : IDisposable
    {
        /// <inheritdoc />
        public void Dispose() => ExitUnit();
    }

    /// <summary>Opens (or nests into) the current thread's write unit. Pair with <see cref="ExitUnit"/>.</summary>
    internal static void EnterUnit() => (t_unit ??= new UnitState()).Depth++;

    /// <summary>Closes the current thread's write unit; the outermost exit retires its stamp.</summary>
    internal static void ExitUnit()
    {
        UnitState? unit = t_unit;
        if (unit is null || unit.Depth == 0 || --unit.Depth > 0)
        {
            return;
        }

        CommitClock? owner = unit.Owner;
        long stamp = unit.Stamp;
        bool holdsGate = unit.HoldsGate;
        bool versioned = unit.Versioned;
        unit.Owner = null;
        unit.Stamp = 0;
        unit.Versioned = false;
        unit.HoldsGate = false;
        owner?.Retire(stamp, holdsGate, versioned);
    }

    /// <summary>
    /// Returns the current write unit's commit stamp, taking it on first use. Outside a write unit (recovery,
    /// direct storage use) a plain id is issued and nothing is tracked.
    /// </summary>
    /// <param name="committingTransactionId">
    /// For a <c>COMMIT</c>, the committing transaction's id: it stays registered while it commits, but its own
    /// snapshot does not need the versions its commit replaces.
    /// </param>
    /// <returns>The commit stamp.</returns>
    public long AcquireStamp(long committingTransactionId = 0)
    {
        UnitState? unit = t_unit;
        if (unit is not { Depth: > 0 } || (unit.Owner is not null && unit.Owner != this))
        {
            return _ids.AllocateTransactionId();
        }

        if (unit.Stamp != 0)
        {
            return unit.Stamp;
        }

        long stamp;
        bool versioned;
        lock (_sync)
        {
            stamp = _ids.AllocateTransactionId();
            versioned = _anyOtherTransactionOpen(committingTransactionId);
            _inFlight.Add(stamp);
        }

        unit.Owner = this;
        unit.Stamp = stamp;
        unit.Versioned = versioned;
        if (versioned)
        {
            Gate.EnterWrite(holdsLocks: Transactions.LockManager.CurrentThreadHoldsLocks);
            unit.HoldsGate = true;
        }

        return stamp;
    }

    /// <summary>
    /// Gets whether the current write unit records MVCC versions. Takes the unit's stamp first, so the decision
    /// is fixed before the caller writes. Outside a write unit, reports whether any transaction is open.
    /// </summary>
    public bool CurrentUnitRecordsVersions()
    {
        UnitState? unit = t_unit;
        if (unit is not { Depth: > 0 } || (unit.Owner is not null && unit.Owner != this))
        {
            return _anyOtherTransactionOpen(0);
        }

        if (unit.Stamp == 0)
        {
            AcquireStamp();
        }

        return unit.Versioned;
    }

    /// <summary>
    /// Takes a snapshot for a new transaction: issues its id, registers it as open through
    /// <paramref name="register"/> (atomically, so every later write unit records versions for it), then waits
    /// until every write unit that was already in flight has finished publishing.
    /// </summary>
    /// <param name="register">Registers the transaction as open; may throw to refuse it.</param>
    /// <param name="unregister">Undoes <paramref name="register"/> when the wait fails.</param>
    /// <returns>The transaction's snapshot.</returns>
    /// <exception cref="TimeoutException">An in-flight write unit did not finish in time.</exception>
    public TransactionSnapshot BeginSnapshot(Action<TransactionSnapshot> register, Action unregister)
    {
        long own = t_unit is { Depth: > 0, Owner: { } owner } current && owner == this ? current.Stamp : 0;
        TransactionSnapshot snapshot;
        long[] pending;
        lock (_sync)
        {
            long id = _ids.AllocateTransactionId();
            snapshot = new TransactionSnapshot(snapshotTimestamp: id, transactionId: id);
            register(snapshot);
            pending = _inFlight.Count == 0 ? [] : [.. _inFlight.Where(stamp => stamp != own)];
        }

        if (pending.Length > 0 && !WaitForRetirement(pending))
        {
            unregister();
            throw new TimeoutException("Timed out waiting for in-flight commits before starting the transaction.");
        }

        return snapshot;
    }

    /// <summary>Gets the number of write units currently in flight (diagnostics and tests).</summary>
    public int InFlightCount
    {
        get
        {
            lock (_sync)
            {
                return _inFlight.Count;
            }
        }
    }

    private bool WaitForRetirement(long[] stamps)
    {
        long deadline = Compat.MonotonicClock.GetMilliseconds() + _waitTimeoutMs;
        lock (_sync)
        {
            foreach (long stamp in stamps)
            {
                while (_inFlight.Contains(stamp))
                {
                    long remaining = deadline - Compat.MonotonicClock.GetMilliseconds();
                    if (remaining <= 0 || !Monitor.Wait(_sync, (int)Math.Min(remaining, int.MaxValue)))
                    {
                        if (_inFlight.Contains(stamp))
                        {
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }

    private void Retire(long stamp, bool holdsGate, bool versioned)
    {
        if (holdsGate)
        {
            Gate.Exit();
        }

        if (stamp == 0)
        {
            return;
        }

        lock (_sync)
        {
            _inFlight.Remove(stamp);
            Monitor.PulseAll(_sync);
        }

        if (versioned)
        {
            _versionedUnitRetired?.Invoke();
        }
    }
}
