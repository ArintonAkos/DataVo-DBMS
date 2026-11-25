# INSERT Intent + Row Locking (v0.5.1 Investigation)

Status: planned for v0.5.1. Nothing here changes v0.5.0.

## Why

On 25 November 2026, master CI run `45690201364` failed one job (`Tests (macos-latest, net8.0)`):
`WriterProgressUnderReadLoadTests.Writers_CompleteWritesInEveryWindow_UnderContinuousPointReads(mode: InMemory)`.

| Writer | Writes per 300 ms window |
| --- | --- |
| 0 (`InsertTyped`) | 19570, 22374, 22361, 44884, **0** |
| 1 (compiled `UPDATE`) | 2446, 1757, 3170, 784, 4842 |

The inserting writer completed nothing for a whole window while the updating writer and the readers kept
going, so the stall was not a GC suspension or a paused VM. The run used the 3-vCPU macOS runner with
8 readers and 2 writers. It did not reproduce locally (25 runs, plus 12 runs restricted to efficiency cores).

## What Local Measurement Showed

Per-phase timestamps in `DataVoContext.InsertTyped` and `LockManager.AcquireTableLock` (temporary, not
committed) put nearly all of a slow insert's time in one place: waiting in
`ReaderWriterLockSlim.TryEnterWriteLock` for the table lock (`AcquireRetainedWriteLock`). Under contention
that wait reached 4–16 ms. Every other insert phase stayed near 0 ms.

- `InsertTyped` takes an **exclusive table write lock** for every single-row insert.
- A compiled `UPDATE` takes a **table intent (shared) lock** for each row write lock it holds (`AcquireTableIntentReadForRowWrite`).
  So any in-flight update blocks every insert on the same table.
- A general-path compiled `UPDATE` held its locks for up to ~17 ms. That time was split between waiting
  for row locks (behind the insert's table lock) and allocating revalidate/apply work (dictionaries per row)
  done under the lock. The two writers therefore wait on each other through the table lock.
- Ruled out by experiment: large-object-heap allocations in the insert path (a 132 KB row page every
  4,096 inserts, the PK map resizes) stalling behind background gen2 GCs. With 18 forced background GCs
  over an 8M-object heap, inserts that allocated on the LOH had a 0.02 ms median and a 1.6 ms maximum.

What is still unproven: whether this lock coupling alone produces a 300 ms stall on the CI runner, where
oversubscription makes lock handoffs much slower. As of `a93a3430`, the test's failure message reports
both writers' windows, each writer's longest single write (and the window it ended in), and GC
collections and pause time. A recurrence will say whether the stall is one long write (a lock wait) or GC.

## Work Items

1. **INSERT on intent + row (key) locks.** Take the table intent lock plus a row lock on the new primary
   key instead of the exclusive table lock. Then inserts and updates of different rows no longer exclude
   each other. Things to settle first:
   - What the exclusive lock protects today: row-id assignment (`TableStore.AddRaw` already serializes
     under the store lock), primary-key and unique-index checks against concurrent inserts of the same key,
     foreign-key checks, and the change recorder's publication order.
   - Deadlock detection: the wait-for graph must see key locks taken by inserts.
   - DDL (`ALTER`, `DROP`, index builds) must still take the exclusive table lock and wait for intents.
2. **Why compiled `UPDATE` stays off the zero-alloc path here.** `EnableZeroAllocCompiledUpdate` is on by
   default, but `TryUpdateFixedWidthFastPath` returns early unless `StorageMode == Lsm`. It also returns
   early when change capture is enabled or MVCC records versions. On `InMemory`, every compiled update
   therefore takes the dictionary path, which allocates and holds locks for longer. Decide whether
   `InMemory` gets a byte-patch path (its rows are serialized the same way), and measure the lock hold
   time before and after.
3. **Lock-entry churn.** `LockManager` removes and disposes a table's `ReaderWriterLockSlim` whenever its
   last user releases with `removeWhenIdle`. A row write's intent release does that, so a busy table can
   allocate a new lock object repeatedly. Measure it, and keep table entries alive while the table exists.

## Acceptance

- `WriterProgressUnderReadLoadTests` keeps its assertions. Every writer completes writes in every
  300 ms window on all CI runners.
- An insert plus update contention benchmark (`concurrent-ops` with a mixed writer) shows no regression
  in throughput or p99.
- Lock-order and deadlock tests cover insert key locks against update row locks and DDL.
