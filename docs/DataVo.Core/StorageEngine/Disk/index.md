# Disk (StorageEngine) Overview

The `Disk` module encapsulates the direct-to-file storage backend used by `DataVo` when persistence is enabled. It manages binary table files, row tombstones, and compaction. Durability for committed transactions is coordinated alongside this module through the WAL and recovery components in `DataVo.Core/Transactions`.

## Core Responsibilities

- **File Mutating:** Coordinates precise binary writes to per-table `.dat` files.
- **Row Addressing:** Uses byte offsets as stable disk row identifiers.
- **Tombstoning and Compaction:** Marks deleted rows in place and rewrites compacted files during `VACUUM`.

## Component Breakdown

| Component (File)       | Architectural Role                                                                                                                                                                                                    |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DiskStorageEngine.cs` | The primary implementer of `IStorageEngine` for persistent data. Handles binary pointer offsets, organic byte arrays serialization, safe structural tombstoning, vacuuming/compaction, and direct file IO operations. |

## Dependencies & Interactions

Invoked by `StorageContext`, the `DiskStorageEngine` translates logical rows into physical bytes using the `Serialization` module. It cooperates with `IndexManager` to keep B-Tree row references in sync. During durable commits, WAL entries are written before these physical mutations occur; on startup, recovery can replay those WAL entries back through the same storage APIs.
