# BTree Overview

The `BTree` module contains the indexing subsystem used by `DataVo.Core` to map logical index keys to physical row IDs. It includes multiple index implementations, shared contracts, key encoding helpers, paged file access, and debugger-oriented inspection utilities.

## What this module does

- Creates and persists index files for primary key, unique key, and user-defined indexes.
- Loads index files lazily when a query or write operation first touches them.
- Supports multiple physical implementations behind a single `IIndex` contract.
- Encodes logical keys into byte sequences that preserve sort order in the binary B+Tree implementation.
- Provides diagnostics for inspecting index files during debugging.

## Main Components

| Component | Purpose |
| :--- | :--- |
| `IndexManager.cs` | Central entry point for creating, loading, caching, mutating, flushing, and dropping indexes. |
| `IndexKeyEncoder.cs` | Converts logical key strings into fixed-size byte arrays for the binary B+Tree and builds composite keys from row data. |
| `BTreeDumpUtility.cs` | Produces human-readable dumps of index files for debugging and troubleshooting. |
| `BTreeNode.cs` | Generic in-memory B-Tree node used by the JSON-backed implementation. |
| `Binary/` | Classic binary B-Tree implementation using fixed-size pages. |
| `BPlus/` | Binary B+Tree implementation with linked leaf pages. This is the default index engine. |
| `Core/` | Shared contracts and index-type metadata (`IIndex`, `IndexType`, `JsonBTreeIndex`). |

## Runtime Role

The query and storage layers use this module in several places:

- **Table creation**: creates empty PK and UK indexes.
- **Index creation**: bulk-loads an index from existing table rows.
- **Insert / Update / Delete**: mutates index contents as table rows change.
- **Select / Where evaluation**: resolves equality predicates using `IndexManager.FilterUsingIndex`.
- **Vacuum / maintenance**: rebuilds index files from compacted table storage.

## Index Implementations

| Implementation | Notes |
| :--- | :--- |
| `JsonBTreeIndex` | Fully in-memory generic B-Tree serialized to JSON. Simple and easy to inspect, but less efficient for large or frequently mutated indexes. |
| `BinaryBTreeIndex` | Disk-backed classic B-Tree using fixed-size pages. Stores key/value mappings directly in pages. |
| `BinaryBPlusTreeIndex` | Disk-backed B+Tree using fixed-size pages and linked leaves. Chosen by default through `IndexManager.DefaultIndexType`. |

## Key Design Details

- Keys exposed through `IIndex` are always strings.
- Composite keys are joined with `##`.
- In the binary B+Tree implementation, keys are encoded into 32-byte arrays.
- Integer values are sign-flipped before byte encoding so lexicographic byte comparison preserves numeric sort order.
- Buffered persistence is supported through `IndexManager.ConfigurePersistence`.

## Related Pages

- `IndexManager.md`
- `Binary/index.md`
- `BPlus/index.md`
- `Core/index.md`
