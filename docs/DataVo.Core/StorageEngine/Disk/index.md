# Disk (StorageEngine) Overview

The `Disk` module encapsulates the direct-to-file synchronization required to establish Non-Volatile (ACID Durable) persistence guarantees. It organizes logical `DataVo` sequences into raw binary structures, physically locking and manipulating `.page` and `.db` files optimally on the host operating system.

## Core Responsibilities
* **File Mutating:** Coordinates precise binary writes to the underlying data files (`.db`). Organizes file streams, extracts byte pointers, and controls how structs and chunks are safely formatted to avoid fragmentation.
* **Transaction Mapping:** Regulates IO operations to guarantee atomic inserts, updates, and deletes. Evaluates limits and handles cache boundaries effectively to delay hardware flushes until necessary.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `DiskStorageEngine.cs` | The primary implementer of `IStorageEngine` for persistent data. Handles binary pointer offsets, organic byte arrays serialization, safe structural tombstoning, vacuuming/compaction, and direct file IO operations. |

## Dependencies & Interactions
Invoked by the `DataVoContext`, the `DiskStorageEngine` securely translates DML queries into physical bytes using the `Serialization` module. It cooperates explicitly with `IndexManager` to map unindexed file coordinates (RowIDs) against the B+Tree nodes cleanly.
