# StorageEngine Overview

The `StorageEngine` module handles the physical allocation, serialization, and persistence of the data managed by `DataVo.Core`. It abstracts file I/O and memory-backed storage away from higher-level query execution so the engine can switch between disk mode and in-memory mode without changing parser logic.

## Core Responsibilities

- **Physical Persistence:** Translates logical table structures and row collections into absolute byte streams written to persistent media.
- **Recovery Bootstrap:** Runs WAL-based crash recovery during initialization when disk mode and WAL are enabled.
- **Serialization Protocols:** Defines exactly how row values are laid out sequentially in binary.

## Component Breakdown

| Component (File/Dir) | Architectural Role                                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `Config/`            | Houses storage-mode and WAL-related configuration, including WAL enablement, file path selection, and checkpoint thresholds.                    |
| `Disk/`              | Contains the primary concrete implementation of direct-to-file row storage.                                                                     |
| `Memory/`            | Houses the ephemeral, volatile array-backed implementation utilized for ultra-fast transient operations or unit tests.                          |
| `Serialization/`     | Implements the low-level generic bit-packing and offset calculators used to compact primitives into dense byte arrays.                          |
| `IStorageEngine.cs`  | The master interface defining the universal contract methods (Read, Write, Delete page/row) any storage backend must support.                   |
| `StorageContext.cs`  | The central state supervisor managing active storage configuration, routing operations to the current engine, and invoking recovery on startup. |

## Dependencies & Interactions

The `StorageEngine` sits at the bottom of the dependency stack. It is driven by parser actions, index maintenance, and transaction recovery code. In disk mode, `StorageContext.Initialize()` can invoke `RecoveryManager` to replay uncheckpointed WAL entries before normal query execution resumes.
