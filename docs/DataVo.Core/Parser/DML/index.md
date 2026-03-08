# DML (Parser Actions) Overview

The `DML` module within the `Parser` is responsible for applying row mutations to existing tables. It translates `INSERT`, `UPDATE`, and `DELETE` statements into storage-engine operations while preserving constraint correctness and transaction semantics.

## Core Responsibilities

- **Persistence Writing:** Applies inserts, updates, and deletes to table storage and secondary indexes.
- **Constraint Validation Checks:** Enforces primary-key, unique-key, and foreign-key rules before data is persisted.
- **Transaction Awareness:** Buffers writes during explicit transactions and flushes them only at `COMMIT` time.
- **Write Isolation:** Uses table-level write locks for auto-commit writes so concurrent sessions cannot corrupt the same table.

## Component Breakdown

| Component (File) | Architectural Role                                                                                                                                                                          |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DeleteFrom.cs`  | Determines matched row ID arrays passing identifiers explicitly to storage contexts removing indices bounds.                                                                                |
| `InsertInto.cs`  | Analyzes batches executing parallel scalar binding inserting payloads translating literals into internal native byte alignments.                                                            |
| `Update.cs`      | Executes an out-of-place record replacement replacing legacy values and allocating dynamic B-tree modifications explicitly generating batch constraints preventing intra-query duplication. |
| `Vacuum.cs`      | Orchestrates a physical defragmentation system sweeping tombstoned nodes and physically clustering live binary chunks into continuous IO alignments.                                        |

## Dependencies & Interactions

The DML actions collaborate with `StorageContext`, `IndexManager`, `Catalog`, and `TransactionManager`. In auto-commit mode they acquire table-level write locks through `LockManager`. In explicit transactions they buffer work in `TransactionContext` and defer physical writes until `COMMIT`.

## Implementation Specifics

- **Auto-Commit Write Path:** `INSERT`, `UPDATE`, and `DELETE` acquire an exclusive table lock, validate constraints, mutate base storage, then update indexes.
- **Explicit Transaction Path:** The same statements do not mutate disk immediately; instead they write into `TransactionContext` buffers.
- **Delete Behavior:** `DELETE` still enforces `RESTRICT` and `CASCADE` rules before tombstoning rows and removing corresponding index entries.
- **Update Methodology:** `UPDATE` uses an out-of-place strategy: remove the old row, insert a new row version, then rebuild affected index entries for the replacement row.
