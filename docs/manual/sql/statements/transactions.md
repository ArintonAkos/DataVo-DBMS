---
title: Transactions
description: Control multi-statement atomic transactions, nested savepoints, MVCC snapshot isolation, and conflict handling in DataVo v0.4.
---

# Transactions

DataVo supports multi-statement atomic transactions with **MVCC snapshot isolation**, **read-your-own-writes**, **first-committer-wins** write-conflict detection, and **nested savepoints**. Statements executed outside an explicit `BEGIN` block run in autocommit mode.

## Syntax

```sql
-- Start an explicit transaction
BEGIN [TRANSACTION];

-- Publish all buffered writes atomically
COMMIT;

-- Discard all uncommitted writes and close the transaction
ROLLBACK;

-- Define a named savepoint inside an active transaction
SAVEPOINT <savepoint_name>;

-- Rewind changes made after a savepoint without closing the transaction
ROLLBACK TO [SAVEPOINT] <savepoint_name>;

-- Remove a savepoint (and any newer savepoints) while keeping its changes
RELEASE [SAVEPOINT] <savepoint_name>;
```

## Statements Reference

| Statement | Optional Form | Description |
| --- | --- | --- |
| `BEGIN` | `BEGIN TRANSACTION` | Starts an explicit transaction for the current session and pins a commit-ordered MVCC read snapshot. |
| `COMMIT` | — | Re-validates constraints, checks for write conflicts, and atomically publishes all buffered writes to the storage engine and WAL. |
| `ROLLBACK` | — | Discards all buffered writes for the current session and closes the active transaction. |
| `SAVEPOINT <name>` | — | Records a named rollback point within the open transaction. |
| `ROLLBACK TO <name>` | `ROLLBACK TO SAVEPOINT <name>` | Undoes all writes made after `<name>` while keeping earlier writes and the outer transaction open. |
| `RELEASE <name>` | `RELEASE SAVEPOINT <name>` | Drops `<name>` (and any savepoints created after it) while keeping all writes made since `<name>`. |

## Basic Transaction Flow

Group related writes inside `BEGIN` ... `COMMIT` so either all changes become visible together or none do:

```sql
BEGIN TRANSACTION;

UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;

COMMIT;
```

If an error occurs or an application check fails before committing, issue `ROLLBACK` to discard every buffered change cleanly:

```sql
BEGIN;

DELETE FROM orders WHERE status = 'draft';
-- Abort instead of publishing the delete
ROLLBACK;
```

## Nested Savepoints

Savepoints let you partially undo work inside a long transaction without aborting earlier changes:

```sql
BEGIN TRANSACTION;

INSERT INTO players (id, name, score, region) VALUES (10, 'Ada', 900, 'EU');
SAVEPOINT after_insert;

UPDATE players SET score = 50 WHERE id = 10;
SAVEPOINT after_penalty;

DELETE FROM players WHERE id = 10;

-- Undo the delete and penalty, keeping the initial insert
ROLLBACK TO SAVEPOINT after_insert;

-- Clean up the savepoint and commit Ada with score = 900
RELEASE SAVEPOINT after_insert;
COMMIT;
```

## Snapshot Isolation & Read-Your-Own-Writes

When a session runs `BEGIN`, DataVo captures an **MVCC snapshot ordered by commit**:

- **Consistent reads:** The transaction sees only commits that finished before its `BEGIN`. Concurrent readers never block writers, and writers never block readers.
- **Read-your-own-writes:** Every `SELECT`, `UPDATE`, `DELETE`, join, subquery, and index lookup inside the transaction sees the pinned snapshot overlaid with the session's own uncommitted writes. Multiple updates to the same row coalesce into a single pending version.

```sql
BEGIN;

INSERT INTO players (id, name, score, region) VALUES (11, 'Grace', 50, 'US');
UPDATE players SET score = 120 WHERE name = 'Grace';  -- sees buffered insert
SELECT score FROM players WHERE id = 11;              -- returns 120
DELETE FROM players WHERE id = 11 AND score = 120;    -- removes buffered row

COMMIT;                                               -- net no-op for id = 11
```

::: tip Fast-Path Note Inside Explicit Transactions
Queries inside an explicit transaction evaluate against the transaction's write buffer rather than storage-only shortcuts such as `COLUMNAR` aggregates, guaranteeing read-your-own-writes consistency.
:::

## Write Conflicts (First-Committer-Wins)

DataVo enforces **first-committer-wins** under snapshot isolation:

- If two concurrent transactions update or delete the same row, the first transaction to `COMMIT` succeeds.
- The second transaction fails with a write-conflict error—either immediately during `UPDATE`/`DELETE` if the competing commit has already finished, or at `COMMIT`.
- At `COMMIT`, DataVo also re-validates `PRIMARY KEY`, `UNIQUE`, and `FOREIGN KEY` constraints against concurrent commits.
- On `Lsm`, `COMMIT` publishes as a single atomic batch and is discarded on failure. On `InMemory`, applied steps are journaled and rolled back on failure.

::: warning Snapshot Isolation & Retries
- A failed `COMMIT` terminates the transaction; catch the conflict error and retry the unit of work starting from a new `BEGIN`.
- Snapshot isolation prevents dirty reads, non-repeatable reads, lost updates, and read skew, but is **not serializable** (write skew across disjoint rows is possible unless transactions synchronize on a shared row).
:::

## Session Transactions in C#

Transactions are scoped to a session ID. `DataVoContext` does not expose a `BeginTransaction()` C# method—execute transaction SQL statements directly and pass a `Guid sessionId` when running concurrent transactions on a shared context:

```csharp
Guid txSession = Guid.NewGuid();

db.Execute("BEGIN", txSession);
db.Execute("INSERT INTO players (id, name, score, region) VALUES (12, 'Lin', 200, 'APAC')", txSession);
db.Execute("COMMIT", txSession);
```

When using the ADO.NET provider (`DataVo.Data`), `DataVoConnection.BeginTransaction()` wraps these SQL statements in a standard `DbTransaction`.

## See Also

- [Transactions & MVCC](/manual/fundamentals/transactions-and-mvcc) — Architecture of MVCC snapshots, conflict semantics, and ADO.NET examples
- [LSM & Durability](/manual/fundamentals/lsm-and-durability) — WAL crash atomicity and fsync guarantees on `COMMIT`
- [INSERT](/manual/sql/statements/insert) — Inserting rows inside transactions
- [UPDATE](/manual/sql/statements/update) — Modifying rows and triggering conflict detection
- [DELETE](/manual/sql/statements/delete) — Deleting rows atomically within transactions
- [Databases & Security](/manual/sql/statements/databases-and-security) — `TRANSACTION` permission required when RBAC is enabled
