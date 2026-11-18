# Transactions and MVCC

DataVo's concurrency model is designed for embedded workloads where many reads should stay cheap while writes remain coordinated enough to keep table and index state consistent. In plain terms: point reads are optimized so they do not have to serialize through a global lock, while writes use row and table coordination where the engine needs a clear mutation boundary.

The simplest transaction is an explicit group of statements followed by `COMMIT`. DataVo buffers the transaction's changes in the session and applies them when the transaction commits.

```sql
BEGIN TRANSACTION;

INSERT INTO Users (Id, Name, IsActive)
VALUES (1, 'Ada', true);

UPDATE Users
SET IsActive = false
WHERE Id = 1;

COMMIT;
```

If the transaction should not be applied, use `ROLLBACK`. The buffered changes are discarded.

```sql
BEGIN TRANSACTION;

INSERT INTO Users (Id, Name, IsActive)
VALUES (2, 'Grace', true);

ROLLBACK;
```

Transactions are session-bound. If your application uses explicit sessions with `DataVoContext.Execute(sql, sessionId)`, the transaction belongs to that session. This lets one part of an embedded application hold a transaction open without accidentally mixing it with statements from another logical workflow.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});

Guid sessionId = Guid.NewGuid();

db.Execute("CREATE DATABASE App", sessionId);
db.Execute("USE App", sessionId);
db.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(80))", sessionId);

db.Execute("BEGIN TRANSACTION", sessionId);
db.Execute("INSERT INTO Users (Id, Name) VALUES (1, 'Ada')", sessionId);
db.Execute("COMMIT", sessionId);
```

MVCC, or multi-version concurrency control, is the part of the engine that lets reads reason about row visibility without forcing every read to block every writer. Each explicit transaction reads from a **snapshot** taken at `BEGIN`.

## Snapshot isolation

A snapshot is ordered by commit, not by transaction start. It contains exactly the commits that had *finished* when the transaction began:

- A transaction never sees a commit that finished after it began — including the commit of a transaction that began *earlier* and was still open (or committing) at its `BEGIN`.
- Auto-commit statements follow the same rule: a statement that was still running when a transaction began is invisible to it, even if it finishes a moment later.
- Rows changed or deleted after the snapshot was taken are read as they were (the engine keeps the old row images while any snapshot may need them).

```sql
-- Session A                         -- Session B
BEGIN;                               BEGIN;
UPDATE Accounts SET Bal = 90
WHERE Id = 1;
COMMIT;
                                     -- Still reads Bal = 100: A committed after B began.
                                     SELECT Bal FROM Accounts WHERE Id = 1;
                                     COMMIT;
```

`BEGIN` waits for write statements and commits that are already in flight to finish publishing, so a snapshot never contains half of one. The wait is bounded by `LockAcquireTimeoutMs`.

## Reading your own writes

Every statement inside a transaction — `SELECT`, `UPDATE`, `DELETE`, joins, subqueries, index lookups, and compiled LINQ `ExecuteUpdate`/`ExecuteDelete`, which run as SQL — reads through the snapshot *plus the transaction's own uncommitted writes*. Statements build on each other exactly as in standard SQL:

```sql
BEGIN;
INSERT INTO Users (Id, Name, IsActive) VALUES (3, 'Linus', true);
UPDATE Users SET Name = 'Linus T.' WHERE Name = 'Linus';   -- matches the buffered insert
UPDATE Users SET IsActive = false WHERE Name = 'Linus T.'; -- matches the updated value
DELETE FROM Users WHERE IsActive = false AND Id = 3;       -- removes it again
COMMIT;                                                     -- nothing is written for Id 3
```

Repeated updates of one row coalesce into a single pending version. Key checks inside the transaction account for its own writes: a key it deleted is free again, a key it inserted is taken.

`SAVEPOINT name`, `ROLLBACK TO SAVEPOINT name` and `RELEASE SAVEPOINT name` restore the transaction's buffered inserts, updates and deletes to the state at the savepoint; savepoints created after it are discarded.

## Write conflicts

Two transactions that change the same row conflict, and the first to commit wins. The second one fails with a conflict error — either at the `UPDATE`/`DELETE` statement, if the other commit already finished, or at its own `COMMIT`. A failed `COMMIT` ends the transaction; retry it from `BEGIN`.

At `COMMIT` the engine also re-checks, against what other sessions committed meanwhile, the primary and unique keys of buffered inserts and of updated values, and the foreign keys of buffered inserts.

## Atomic commit

A `COMMIT` is all or nothing. Write conflicts and key checks run before anything is applied. The foreign keys of buffered inserts are checked after the commit's deletes and updates are applied (a child may reference a parent the same transaction inserts), so a later failure must undo earlier steps: on LSM the whole commit is one write batch, discarded on failure; the in-memory engine journals every applied change and undoes it. A failed `COMMIT` leaves rows, indexes and version metadata exactly as they were.

## Concurrency behaviour

Readers outside transactions never wait for MVCC. While at least one transaction is open, a statement that reads through a snapshot and a write statement that must record versions for it do not overlap: each waits for the other's *statement* to finish (never for a whole transaction). Write statements do not wait for each other because of MVCC; they coordinate only through row and table locks.

Version metadata and retained row images are dropped as soon as no open snapshot can need them. The cleanup work follows the commits made while transactions were open; it never scans the whole table or version map.

## Isolation contract

The isolation level is **snapshot isolation**. It prevents dirty reads, non-repeatable reads, lost updates and read skew, and a transaction's repeated reads (including with predicates) return the same rows. It is not serializable: **write skew** is possible — two transactions that each read rows the other updates, and update disjoint rows, can both commit.

```sql
-- Both see two doctors on call; each takes a different one off call; both commit.
-- Session A                                   -- Session B
BEGIN;                                         BEGIN;
SELECT COUNT(*) FROM OnCall WHERE Active = true;
                                               SELECT COUNT(*) FROM OnCall WHERE Active = true;
UPDATE OnCall SET Active = false WHERE Id = 1;
                                               UPDATE OnCall SET Active = false WHERE Id = 2;
COMMIT;                                        COMMIT;   -- succeeds: no row was written by both
```

Guard such invariants with a row both transactions update (for example a counter row), which turns the skew into a write conflict.

Durability depends on the storage configuration. For the LSM engine, use `LsmStrictFsync = true` when committed writes must wait for the WAL to reach stable storage.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

Relaxed LSM mode is useful for benchmarks and rebuildable data, but it has a different contract: recent acknowledged writes can be lost on power or kernel failure because the WAL is not synchronously forced to stable storage.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_cache",
    LsmStrictFsync = false
});
```

## ACID & Isolation Contract

Supported: session-bound `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK` and savepoints (DML is buffered until commit or rollback); snapshot isolation ordered by commit; read-your-own-writes across chained DML; first-committer-wins write conflicts; atomic `COMMIT` on every storage engine; single-row lock-free reads; per-table write locks and row-level write coordination for auto-commit statements; strict LSM commit durability (`LsmStrictFsync = true`); and disk sync writes (`SyncDiskWrites = true`). DataVo does **not** guarantee serializability (write skew is possible) or full ACID equivalence to PostgreSQL or SQLite.
