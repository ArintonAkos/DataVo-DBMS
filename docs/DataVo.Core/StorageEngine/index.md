# StorageEngine Overview
The `StorageEngine` module handles the physical allocation, serialization, and durability of the data managed by `DataVo.Core`. It completely abstracts away the specifics of file I/O and memory mapping from the higher-level logical evaluators, permitting the system to swap between localized disk storage or entirely in-memory test databases seamlessly.

## Core Responsibilities
* **Physical Persistence:** Translates logical table structures and row collections into absolute byte streams written to persistent media.
* **Page Management:** Allocates and manages fixed-size data pages, orchestrating writes, fragmentation, and pointer layouts.
* **Serialization Protocols:** Defines exactly how primitive data types and nested B-Tree structures are laid out sequentially in binary.

## Component Breakdown

| Component (File/Dir) | Architectural Role |
|----------------------|--------------------|
| `Config/` | Houses configurations dictating page capacities, buffer pool limits, and auto-sync thresholds. |
| `Disk/` | Contains the primary concrete implementation of direct-to-file byte serialization and data persistence. |
| `Memory/` | Houses the ephemeral, volatile array-backed implementation utilized for ultra-fast transient operations or unit tests. |
| `Serialization/` | Implements the low-level generic bit-packing and offset calculators used to compact primitives into dense byte arrays. |
| `IStorageEngine.cs` | The master interface defining the universal contract methods (Read, Write, Delete page/row) any storage backend must support. |
| `StorageContext.cs` | The central state supervisor managing active database connections and routing operations to the currently wired storage engine. |

## Dependencies & Interactions
The `StorageEngine` sits at the very bottom of the dependency stack. It is controlled exclusively by the `Services` and the `BTree` modules, which provide it with structured data. It leverages the `Cache` to prevent redundant page fetches and uses custom `Exceptions` when encountering physical file corruption or path resolution failures.
