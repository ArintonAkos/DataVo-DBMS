# DataVo.Core Structure

`DataVo.Core` contains the database engine itself: SQL parsing, transactional state management, indexing, row storage, and disk persistence.

## High-Level Responsibilities

At runtime, the core project coordinates the full lifecycle of a query:

1. Parse SQL into executable actions.
2. Validate schema and constraints.
3. Route reads and writes through the storage layer.
4. Maintain indexes and catalog metadata.
5. Enforce ACID-oriented behavior through transactions, locking, and WAL-backed durability.

## Architectural Breakdown

| Sub-Module        | Primary Function                                                                                             |
| :---------------- | :----------------------------------------------------------------------------------------------------------- |
| **BTree**         | Maintains primary, unique, and foreign-key index structures used for fast lookup and constraint enforcement. |
| **Parser**        | Converts SQL text into AST-driven actions for DDL, DML, DQL, and transaction commands.                       |
| **Transactions**  | Implements explicit transactions, table-level locking, WAL serialization, and crash recovery coordination.   |
| **StorageEngine** | Provides the physical persistence abstraction for in-memory and disk-backed table storage.                   |
| **Cache**         | Tracks session-level state such as the active database.                                                      |
| **Logger**        | Emits operational diagnostics and execution traces.                                                          |

## Key Entry Points

- [DataVoContext](./DataVoContext.md)
- [StorageContext](./StorageEngine/StorageContext.md)
- [DataVoConfig](./StorageEngine/Config/DataVoConfig.md)
- [Runtime Overview](./Runtime/index.md)

## Read This Alongside

If you want usage-oriented documentation with SQL examples and expected behavior, start with the [Feature Documentation](../features/index.md) section.

This `DataVo.Core` area is contributor-facing and explains how the code is organized internally.

## Documentation Status

This module reference is being expanded incrementally. Some subsystems are already documented at file level, while others still have overview-only pages and are being filled in batch by batch.

## ACID Features Implemented

- **Atomicity:** Explicit `BEGIN`, `COMMIT`, and `ROLLBACK` buffer writes in memory and flush them as a unit.
- **Consistency:** Existing key and foreign-key validation remains enforced during inserts, updates, and deletes.
- **Isolation:** Table-scoped `ReaderWriterLockSlim` instances allow concurrent `SELECT` operations while serializing writes.
- **Durability:** Disk mode can persist committed transactions to a write-ahead log before mutating base table files.
