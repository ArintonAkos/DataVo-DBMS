# LSM Crash Atomicity Design

## Status

Approved 2026-11-04 as written: Option A, with all three open questions answered as recommended
(writes become visible only once durable; per-table WALs are retired with a one-time migration;
relaxed mode uses the same commit log).

Implementation status (2026-11-04): slices 1–4 are implemented. Every statement and every transaction
commit is one commit-log frame; per-table WALs are retired with one-time migration; recovery replays
frames above each table's flushed sequence; pruning follows flushes. `CrashRecoveryTests` passes for
Lsm, including a two-table transaction and multi-row `UPDATE` invariant.

Deviation, still open: writes are published to the latest-row view when the statement makes them, not
after the frame is durable, so the "visible only once durable" decision is not implemented yet. A
statement that fails part-way also keeps its earlier mutations (no statement rollback). Both are tracked
as the next atomicity slice.

## Problem

`CrashRecoveryTests` (tests/DataVo.CrashHarness, SIGKILL at random points) shows that a hard crash
during an autocommit `UPDATE` deletes an already committed row in Lsm strict mode. In every failing
run, the missing row was exactly the target of the `UPDATE` that was in flight at the kill.

Cause: the SQL `UPDATE` path (`Parser/DML/Update.cs`) calls `StorageContext.DeleteFromTable(old)`
and then `StorageContext.InsertSerializedRow(new)`. In Lsm each call becomes its own checksummed
WAL frame in the table's `active.wal`, followed by its own durability wait. Recovery replays frames
up to the first torn one, so a crash between the two frames keeps the tombstone and loses the new
version.

The same shape breaks wider guarantees:

- a statement that changes several rows can be partially applied after a crash;
- an explicit transaction is applied by `Commit.FlushContext` as many independent storage calls, and
  Lsm has no commit record at all (`Commit.cs` only logs for Disk), so a crash mid-commit leaves part
  of the transaction;
- Lsm keeps one WAL per table, so even a perfect per-table batch could not make a two-table
  transaction atomic.

## Goals

1. An acknowledged statement or transaction is either fully present or fully absent after a crash,
   in Lsm strict mode, across any number of rows and tables.
2. No acknowledged write is ever lost (strict mode), and nothing unacknowledged beyond the in-flight
   unit appears partially.
3. Keep the current performance profile: group commit, allocation-free bulk ingest, and the lock-free
   read path must not regress materially (measured with the existing whitepaper and arena
   benchmarks, not assumed).
4. Recovery stays idempotent: replaying the log any number of times yields the same state.

## Non-Goals

- Disk mode (legacy; documented as not crash-safe for `UPDATE`).
- Relaxed mode (`LsmStrictFsync = false`) keeps "may lose recent writes", but must never expose a
  partially applied unit after a crash either.
- Changing isolation semantics for concurrent readers (see Visibility below).
- Distributed or multi-directory transactions.

## Options

### A. One engine-level commit log (recommended)

Like RocksDB with column families: one WAL per data directory shared by all tables, while MemTables
and SSTables stay per table.

- Every write unit (an autocommit statement, or an explicit transaction at `COMMIT`) is encoded as
  **one** checksummed commit frame: an ordered list of `Put(table, rowId, bytes)` and
  `Delete(table, rowId)` operations.
- The frame's LSN is the sequence number of every operation in it (the "seqno = WAL Lsn" keystone
  from the LSM storage spec), so replaying a frame is idempotent.
- Row ids are reserved before the frame is written, so replay never has to re-allocate them.
- Per-table `active.wal` files are retired; each table's manifest records the highest commit LSN
  that its SSTables contain (`FlushedLsn`).
- Recovery: read the commit log in order, stop at the first torn or checksum-failing frame, and apply
  each operation whose table has `FlushedLsn < frame LSN`.
- The log is pruned up to `min(FlushedLsn)` over all tables after flushes, like the existing Disk
  checkpointer.

Pros: atomic for any statement or transaction, any number of tables; one fsync per group of commits
instead of one per table; simplest recovery rule. Cons: the biggest change (WAL format, recovery,
pruning, flush bookkeeping); one log means one append point for all tables (mitigated by the
existing group-commit leader/follower scheme).

### B. Per-table multi-operation frames

Keep per-table WALs and let one frame hold several operations for one table. Fixes the reproduced
single-row bug and single-table statements, but not multi-table transactions. Smaller change, but it
cannot reach goal 1.

### C. B plus a coordinator log (two-phase)

Per-table "prepared" frames tagged with a transaction id, plus a small commit-record log; recovery
applies only transactions with a durable commit record. Reaches goal 1 but needs two fsyncs per
multi-table commit and a more complex recovery than A.

**Recommendation: A.** It is the established design for LSM engines with several key spaces, meets
every goal, and replaces per-table WAL machinery instead of adding a second protocol on top.

## Design (Option A)

### Write path

1. A statement (or `COMMIT`) opens a `StorageWriteBatch` on the engine. Calls that today go straight
   to the backend (`InsertSerializedRow`, `InsertTypedRows`, `DeleteFromTable`, fixed-width patch)
   append operations to the batch and return reserved row ids immediately.
2. `UPDATE` in Lsm becomes a single `Put` to the **same row id** (as the compiled fixed-width path
   already does) instead of delete plus append; index maintenance then only moves keys whose value
   changed. This also removes the per-update B-tree churn.
3. At the end of the statement, the batch is encoded into one commit frame, appended to the commit
   log, and made durable through the group committer (strict mode waits; relaxed mode does not).
4. After the frame is durable, its operations are applied to the per-table MemTables and to
   `LatestRows`, all with seqno = frame LSN.
5. Explicit transactions already buffer in `TransactionContext`; `Commit.FlushContext` runs inside one
   batch, so the whole transaction becomes one frame.

### Reads inside a statement

Some paths read their own writes before the statement ends (constraint checks within a multi-row
`INSERT`, change capture after-images). The batch keeps a per-statement overlay keyed by
(table, row id) that `StorageContext` reads consult first. Statements that read only before writing
(most `UPDATE`/`DELETE`) never touch the overlay.

### Visibility

Today `LatestRows` is updated before the fsync completes, so lock-free readers can see writes that are
not yet durable. With this design, writes become visible only after their frame is durable (step 4).
That is stricter than today, and matches synchronous-commit databases. It is a semantics change and
is listed for approval.

### Flush and pruning

- MemTable freeze happens only between applied frames, so an SSTable never contains part of a frame.
- After an SSTable and its manifest edit are durable (existing Plan 6 Step 2 ordering), the table's
  `FlushedLsn` advances. The commit log is pruned up to the minimum `FlushedLsn` across tables.
- A dropped table no longer holds pruning back.

### Recovery

1. Open all tables from their manifests (as today); read each `FlushedLsn`.
2. Scan the commit log; validate each frame's length and CRC32C; stop at the first invalid frame and
   truncate the torn tail.
3. For each operation with `frame LSN > FlushedLsn(table)`, apply it to that table's MemTable.
4. Restore `NextRowId` and `NextSeqno` from the maximum seen values.
5. Rebuild indexes from rows (unchanged from the current Lsm open path).

### Format and migration

- The commit log is a new file `commit.wal` in the data directory, using the existing binary WAL
  frame header and CRC32C.
- The storage format is explicitly unstable in v0.1. On open, a directory that still has per-table
  `active.wal` files is migrated once: those WALs are replayed into their MemTables and flushed, and
  then removed. No silent data loss on upgrade.

## Testing

- Re-enable `CrashRecoveryTests` for Lsm (it must pass repeatedly; run it in a loop in CI).
- Extend the harness with: multi-row `UPDATE`/`DELETE` statements; explicit transactions across two
  tables, with an invariant check (for example, a transfer between two tables whose sum must stay
  constant); kills during MemTable flush and compaction.
- Unit tests for frame encoding, torn-tail truncation, idempotent replay (replay twice equals replay
  once), and pruning watermarks.
- Benchmarks before and after: whitepaper ingest/YCSB/space-recovery and the arena lanes; report
  regressions rather than hide them.

## Delivery Slices

1. Commit-log frame format, writer with group commit, reader, torn-tail handling (unit-tested).
2. `StorageWriteBatch` plus statement overlay in `StorageContext`; Lsm backend applies batches.
   Same-row-id `UPDATE` for Lsm.
3. Explicit transactions through one batch; per-table WAL retirement and one-time migration.
4. Flush `FlushedLsn` bookkeeping, pruning, and recovery replay; re-enable and extend the crash tests.
5. Benchmarks, documentation updates (remove the Known Limitations entries for Lsm).

## Open Questions for Approval

1. Accept the visibility change: writes visible only once durable (recommended), or keep today's
   behavior where readers can see not-yet-durable writes?
2. Accept retiring per-table WAL files in favor of one `commit.wal` (with one-time migration)?
3. Should relaxed mode (`LsmStrictFsync = false`) also use the commit log (atomic but not fsynced per
   commit)? Recommended: yes, one code path.
