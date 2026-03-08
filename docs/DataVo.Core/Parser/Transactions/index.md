# Parser Transactions Overview

The `Parser/Transactions` actions execute SQL transaction commands such as `BEGIN`, `COMMIT`, and `ROLLBACK`.

## Responsibilities

- Start explicit transaction scopes.
- End transactions either by durable commit or in-memory rollback.
- Bridge SQL transaction commands to `TransactionManager`, `LockManager`, and WAL infrastructure.

## Command Behavior

| Command                       | Behavior                                                                                                                                 |
| :---------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------- |
| `BEGIN` / `BEGIN TRANSACTION` | Creates a new session-scoped `TransactionContext`.                                                                                       |
| `COMMIT`                      | Acquires write locks for all affected tables, writes a WAL entry in disk mode, flushes buffered changes, then checkpoints the WAL entry. |
| `ROLLBACK`                    | Discards buffered operations without touching base storage files.                                                                        |

## Commit Lifecycle

`COMMIT` is the most important ACID boundary in the parser layer.

1. Retrieve and detach the current `TransactionContext` from `TransactionManager`.
2. Determine which tables are affected by inserts, deletes, or updates.
3. Acquire table-level write locks in deterministic order.
4. If WAL is enabled, serialize the transaction into a `WalEntry` and append it to the log.
5. Flush inserts, deletes, and updates to base storage and indexes.
6. Mark the WAL entry as checkpointed.
7. Release write locks in reverse order.

## Interaction with DML

DML actions are transaction-aware:

- In auto-commit mode they apply writes immediately under write locks.
- In explicit transaction mode they only buffer operations.
- `COMMIT` is the moment where buffered changes become durable and visible in persistent storage.

## Interaction with Recovery

The parser commit path and startup recovery share the same flush routine. This keeps normal commits and WAL replay aligned, especially for index maintenance and out-of-place updates.
