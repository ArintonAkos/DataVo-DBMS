# Binary Overview
The `Binary` module contains the implementation of a classic Binary B-Tree disk-backed index. In a standard B-Tree, keys and their associated data pointers (Row IDs) can reside in both internal routing nodes and leaf nodes.

## Core Responsibilities
* **Index Mechanics:** Manages the logical tree hierarchy, routing keys, and balancing rules specific to a standard B-Tree.
* **Binary Serialization:** Handles the direct conversion of B-Tree nodes into raw binary sequences written to fixed-size disk blocks.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `BinaryBTreeIndex.cs` | Implements the core classical B-Tree algorithms (Traversal, Insertion, and Splitting) supporting exact-match data retrieval. |
| `BTreePage.cs` | Represents an individual generic node in the B-Tree containing a distributed array of keys and Row IDs. |
| `DiskPager.cs` | The physical I/O interface mapping `BTreePage` memory states directly to native system disk chunks via `FileStream`. |

## Dependencies & Interactions
Like the `BPlus` module, this module implements the `IIndex` interface from `DataVo.Core/BTree/Core`. It is dynamically instantiated by the `IndexManager` when a standard Binary B-Tree is required for specific column statistics or user-defined configurations.

## Implementation Specifics
* **Supported Capabilities:** Exact match equality lookups, node splitting, and fast binary byte-packing.
* **Not Supported / Limitations:** Because data pointers exist in internal nodes, ordered range scans (e.g., `BETWEEN X AND Y`) are fundamentally slower than the B+Tree variant since it requires full tree traversals instead of linear leaf transversals.
