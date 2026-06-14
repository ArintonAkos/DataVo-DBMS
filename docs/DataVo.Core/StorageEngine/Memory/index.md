# Memory (StorageEngine) Overview

`InMemoryStorageEngine` is a non-persistent `IStorageEngine` backed by in-process memory. It's useful for tests and ephemeral workloads.

## Implementation notes

- Stores each table as `ConcurrentDictionary<string, List<byte[]>>` keyed by `<database>.<table>`.
- RowIds are **1-based** to avoid collision with the B+Tree sentinel value `0`.
- Deletes are tombstones: a row slot is set to `null` so RowIds do not shift.
- `CompactTable` rebuilds the table list, removing tombstones and reassigning RowIds.
