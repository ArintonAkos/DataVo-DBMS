---
title: Transactions & Savepoints
description: SQL transaction control statements and MVCC snapshot isolation behavior in DataVo v0.4.
---

# Transactions & Savepoints

DataVo provides multi-statement atomic transactions with MVCC snapshot isolation and nested savepoints. Transaction boundaries are controlled directly through SQL statements (or `DataVoConnection.BeginTransaction()` when using ADO.NET).

```sql
BEGIN TRANSACTION;

INSERT INTO players (Id, Name, Score, Region) VALUES (10, 'Ada', 900, 'EU');
SAVEPOINT after_insert;

UPDATE players SET Score = 50 WHERE Id = 10;
ROLLBACK TO SAVEPOINT after_insert;

RELEASE SAVEPOINT after_insert;
COMMIT;
```

## Statement Reference

| Statement | Optional Syntax | Behavior |
| --- | --- | --- |
| `BEGIN` | `BEGIN TRANSACTION` | Starts an explicit transaction and captures an MVCC read snapshot for the current session. |
| `COMMIT` | — | Atomically publishes all buffered writes in the transaction to the storage engine and WAL (if enabled). |
| `ROLLBACK` | — | Discards all uncommitted writes in the current transaction and closes the transaction. |
| `SAVEPOINT <name>` | — | Marks a named rollback point within the active transaction. |
| `ROLLBACK TO <name>` | `ROLLBACK TO SAVEPOINT <name>` | Undoes all writes made after `<name>` while keeping the transaction open and earlier writes intact. |
| `RELEASE <name>` | `RELEASE SAVEPOINT <name>` | Removes the named savepoint (and any newer savepoints) without rolling back its changes. |

## MVCC Snapshot Behavior

- **Snapshot isolation (not serializable):** A transaction sees a consistent snapshot of rows committed before it began, plus its own uncommitted writes. Concurrent readers never block writers, and writers never block readers.
- **Autocommit outside `BEGIN`:** Statements executed outside an explicit `BEGIN` ... `COMMIT` block commit automatically.
- **Query path note:** Queries executed inside an explicit transaction evaluate against the transaction's write buffer on the general evaluator rather than bypassing it via storage-only fast paths (such as `COLUMNAR` aggregates).
- **C# usage:** `DataVoContext` does not expose a `BeginTransaction()` C# method—execute `db.Execute("BEGIN")` / `db.Execute("COMMIT")` / `db.Execute("ROLLBACK")` directly. If you use ADO.NET (`DataVo.Data`), `DataVoConnection.BeginTransaction()` wraps these SQL statements for you.

## See Also
- [Transactions & MVCC](/manual/fundamentals/transactions-and-mvcc) — Architecture of MVCC snapshots, write-conflict semantics, and WAL durability
- [DML Statements](/manual/sql/statements/dml) — `INSERT`, `UPDATE`, and `DELETE` statements that participate in transactions
- [ADO.NET Provider](/manual/client-interfaces/ado-net) — Using `DataVoTransaction` with `DataVoConnection`
