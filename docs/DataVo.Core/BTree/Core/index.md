# Core BTree Contracts

The `Core` submodule contains the shared abstractions that allow the rest of the system to work with different index engines through a common API.

## Files in this folder

| File                | Purpose                                                                                                                                  |
| :------------------ | :--------------------------------------------------------------------------------------------------------------------------------------- |
| `IIndex.cs`         | Defines the common operations supported by all index engines: insert, delete-by-rowID, search, row-ID existence checks, and persistence. |
| `IndexType.cs`      | Enumerates the supported concrete index engines.                                                                                         |
| `JsonBTreeIndex.cs` | JSON-backed B-Tree implementation built on `BTreeNode<string, long>`.                                                                    |

## `IIndex`

`IIndex` is the contract used by `IndexManager` and the rest of the storage/query pipeline. All implementations expose the same high-level capabilities:

- insert a logical key → row ID mapping,
- remove row IDs from the index,
- search exact matches by logical key,
- test whether a row ID appears anywhere in the index,
- persist the index to disk.

## `IndexType`

`IndexType` tells `IndexManager` which concrete implementation to create:

- `JsonBTree`
- `BinaryBTree`
- `BinaryBPlusTree`

## `JsonBTreeIndex`

`JsonBTreeIndex` is the simplest implementation in the subsystem:

- the tree lives in managed memory,
- nodes are represented by `BTreeNode<string, long>`,
- the full tree is serialized to JSON when saved,
- deletes are handled by collecting entries and rebuilding the tree.

This implementation is useful for compatibility, testing, and inspection, but it is less suitable for large or heavily updated indexes than the binary implementations.
