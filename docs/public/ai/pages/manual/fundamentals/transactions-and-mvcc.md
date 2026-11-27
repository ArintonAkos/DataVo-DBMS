# Transactions & MVCC

> Source route: /manual/fundamentals/transactions-and-mvcc
> Source file: manual/fundamentals/transactions-and-mvcc.md

DataVo provides multi-version concurrency control (MVCC) with **snapshot isolation**. Readers see a consistent point-in-time view without blocking writers, while explicit transactions buffer writes and commit atomically with first-committer-wins conflict detection.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DataDirectory = "./datavo_lsm_data"
});

db.Execute("CREATE DATABASE App; USE App;");
db.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR, score INT)");

db.Execute("BEGIN TRANSACTION");
db.Execute("INSERT INTO players (id, name, score) VALUES (1, 'Ada', 100)");
db.Execute("UPDATE players SET score = 150 WHERE id = 1");
db.Execute("COMMIT");
```

> **`DataVoContext` uses SQL statements for transactions.** There is no `db.BeginTransaction()` method on `DataVoContext`: issue `BEGIN` / `COMMIT` / `ROLLBACK` via `db.Execute(...)`. If you need a `DbTransaction` object, use `DataVoConnection.BeginTransaction()` in [ADO.NET](/manual/client-interfaces/ado-net).

## Session-Bound Transactions & Savepoints

Transactions belong to a session. When running concurrent workflows against a shared `DataVoContext`, pass an explicit `Guid sessionId` to keep each transaction isolated:

```csharp
Guid sessionId = Guid.NewGuid();

db.Execute("BEGIN", sessionId);
db.Execute("INSERT INTO players (id, name, score) VALUES (2, 'Lin', 90)", sessionId);
db.Execute("SAVEPOINT before_bonus", sessionId);
db.Execute("UPDATE players SET score = 999 WHERE id = 2", sessionId);
db.Execute("ROLLBACK TO SAVEPOINT before_bonus", sessionId); // reverts bonus only
db.Execute("RELEASE SAVEPOINT before_bonus", sessionId);
db.Execute("COMMIT", sessionId);                             // commits score = 90
```

`ROLLBACK` discards all buffered changes for that session. `SAVEPOINT <name>`, `ROLLBACK TO [SAVEPOINT] <name>`, and `RELEASE [SAVEPOINT] <name>` rewind or discard nested buffered changes without aborting the outer transaction.

## MVCC Snapshot Isolation

When a transaction executes `BEGIN`, DataVo captures a **snapshot ordered by commit**, not by transaction start time:

- A transaction sees only commits that **finished** before its `BEGIN` (waiting up to `LockAcquireTimeoutMs` for any in-flight publish to complete).
- Auto-commit statements still running when `BEGIN` starts remain invisible even if they finish a moment later.
- Old row versions are retained automatically while any active snapshot needs them and reclaimed once no open snapshot can observe them.

<div class="sketch-diagram">
  <img class="light-only" src="/manual/fig-03-mvcc-snapshot-light.svg" alt="MVCC snapshot isolation showing Session B reading its BEGIN snapshot while Session A commits a newer version" />
  <img class="dark-only" src="/manual/fig-03-mvcc-snapshot-dark.svg" alt="MVCC snapshot isolation showing Session B reading its BEGIN snapshot while Session A commits a newer version" />
</div>

```sql
-- Session A                                -- Session B
BEGIN;                                      BEGIN;
UPDATE players SET score = 200 WHERE id = 1;
COMMIT;
                                            -- Still sees score = 150 (A committed after B began)
                                            SELECT score FROM players WHERE id = 1;
                                            COMMIT;
```

## Read-Your-Own-Writes

Every statement inside an open transaction (`SELECT`, `UPDATE`, `DELETE`, joins, subqueries, index lookups, and compiled LINQ `ExecuteUpdate`) reads through the transaction's snapshot **plus its own uncommitted writes**:

```sql
BEGIN;
INSERT INTO players (id, name, score) VALUES (3, 'Grace', 50);
UPDATE players SET score = 120 WHERE name = 'Grace';  -- sees buffered insert
DELETE FROM players WHERE score = 120 AND id = 3;     -- removes buffered row
COMMIT;                                               -- net no-op for id = 3
```

Multiple updates to the same row coalesce into a single pending version, and key checks account for keys inserted or deleted earlier in the same transaction.

## Write Conflicts & Atomic Commit

DataVo enforces **first-committer-wins**:

- If two transactions modify the same row, the first to commit succeeds. The second fails with a conflict error, either during `UPDATE`/`DELETE` if the other commit already finished, or at `COMMIT`. A failed `COMMIT` ends the transaction; retry from `BEGIN`.
- At `COMMIT`, DataVo re-validates primary keys, `UNIQUE` constraints, and foreign keys against concurrent commits. Swapping key values between rows in the same transaction is supported because old rows are removed before new ones are written.
- On `Lsm`, the entire `COMMIT` is written as one atomic batch and discarded on failure. On `InMemory`, applied steps are journaled and undone on failure.

## Isolation Guarantees

| Anomaly | Prevented? | Notes |
| --- | --- | --- |
| Dirty reads | Yes | Uncommitted writes are private to the session |
| Non-repeatable reads | Yes | Snapshot pinned at `BEGIN` |
| Lost updates | Yes | First-committer-wins detects concurrent row writes |
| Read skew | Yes | Multi-row reads observe one consistent commit watermark |
| Write skew | **No** | Snapshot isolation is not serializable; guard cross-row invariants by updating a shared row |

## ADO.NET Transactions

When using `DataVo.Data` (or Dapper), use standard `DataVoConnection.BeginTransaction()`:

```csharp
using DataVo.Data;

using var conn = new DataVoConnection("StorageMode=Lsm;DataSource=./datavo_lsm_data");
conn.Open();

using var tx = conn.BeginTransaction();
using var cmd = conn.CreateCommand();
cmd.Transaction = tx;
cmd.CommandText = "UPDATE players SET score = 300 WHERE id = 1";
cmd.ExecuteNonQuery();
tx.Commit();
```

## See Also

- [SQL Transaction Statements](/manual/sql/statements/transactions): Exact syntax for `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT`
- [LSM & Durability](/manual/fundamentals/lsm-and-durability): Crash atomicity and strict fsync guarantees
- [ADO.NET Provider](/manual/client-interfaces/ado-net): `DataVoConnection` and `DataVoTransaction` usage
