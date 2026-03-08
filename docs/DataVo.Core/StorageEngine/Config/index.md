# Config (StorageEngine) Overview

The `Config` directory isolates the crucial tunable configuration parameters that govern how the `StorageEngine` interacts with the underlying hardware, memory buffers, and internal data structures. Centralizing these limits ensures uniform boundaries and query execution predictability.

## Core Responsibilities
* **Physical Constraints:** Establishes critical numeric boundaries such as physical page sizes, automatic buffering limits, and B-Tree array capacities. 
* **Performance Tuning:** Allows developers to manipulate IO thresholds and caching subsystems by modifying a single source of truth.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `DataVoConfig.cs` | Implements a static singleton defining logical constraints. This includes limits like `PAGE_SIZE`, `MAX_BUFFER_PAGES`, and B-Tree `NODE_CAPACITY` which standardizes file system alignment and byte boundaries universally. |

## Dependencies & Interactions
This configuration is instantiated continuously throughout the `DiskStorageEngine.cs` and `IndexManager.cs` to explicitly calculate payload offsets, determine when a buffer needs to be flushed to disk, and decide when to split or merge B+Tree nodes.
