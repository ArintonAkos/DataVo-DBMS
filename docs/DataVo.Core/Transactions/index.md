# Transactions Overview

The `Transactions` module is the engine's coordination layer for ACID-oriented execution. It manages explicit transaction lifecycles, table-level locking, write-ahead logging, and crash recovery.

## Responsibilities

- Track session-scoped transaction state.
- Buffer DML operations until `COMMIT`.
- Serialize conflicting table writes while allowing concurrent reads.
- Persist committed transaction intent to the WAL before mutating disk storage.
- Recover uncheckpointed transactions during startup.

## Main Components

| Component               | Role                                                                                        |
| :---------------------- | :------------------------------------------------------------------------------------------ |
| `TransactionManager.cs` | Opens, commits, and rolls back explicit transactions on a per-session basis.                |
| `TransactionContext.cs` | Buffers inserted rows, deleted row ids, and updated column sets until commit time.          |
| `LockManager.cs`        | Maintains per-table `ReaderWriterLockSlim` instances keyed by `{database}.{table}`.         |
| `WalEntry.cs`           | Serializes a committed transaction into replayable WAL operations.                          |
| `WalReader.cs`          | Reads persisted WAL entries from disk.                                                      |
| `WalWriter.cs`          | Appends new WAL entries, marks entries checkpointed, and prunes the log when appropriate.   |
| `RecoveryManager.cs`    | Replays uncheckpointed WAL entries on startup before normal execution resumes.              |
| `WalFileStore.cs`       | Provides synchronized low-level file access for WAL reads, appends, rewrites, and deletion. |

## ACID Mapping

### Atomicity

Explicit transactions accumulate DML in `TransactionContext`. Until `COMMIT` runs, no physical table files are changed.

### Consistency

Constraint checks remain enforced by the DML actions before data reaches base storage or indexes.

### Isolation

`LockManager` uses table-level reader/writer locks:

- `SELECT` acquires shared read locks.
- Auto-commit `INSERT`, `UPDATE`, and `DELETE` acquire exclusive write locks.
- `COMMIT` acquires exclusive write locks for all affected tables before flushing buffered changes.

### Durability

In disk mode with WAL enabled:

1. `COMMIT` converts the buffered transaction into a `WalEntry`.
2. The entry is appended to `datavo.wal` and flushed to disk.
3. The storage engine applies table and index mutations.
4. The WAL entry is marked checkpointed.
5. On restart, `RecoveryManager` replays any uncheckpointed entries.

## Durability Flow

1. User session begins an explicit transaction.
2. DML actions buffer work in `TransactionContext`.
3. `COMMIT` acquires table write locks.
4. A `WalEntry` is appended to the WAL and synced.
5. Buffered changes are flushed to table storage and indexes.
6. The entry is checkpointed and later pruned when threshold rules permit.

## Notes

- In-memory storage mode skips WAL creation entirely.
- Recovery runs from `StorageContext.Initialize()` when disk mode and WAL are enabled.
- WAL replay uses the same flush logic as normal commit to keep index maintenance behavior consistent.
