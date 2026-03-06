# BPlus Overview
The `BPlus` module contains the physical implementation of a B+Tree search index. Unlike standard B-Trees, a B+Tree guarantees that all data pointers (Row IDs) exist exclusively at the leaf level, and that these leaf nodes are sequentially linked. This structural guarantee dramatically accelerates range scans and sequential traversals compared to a classical B-Tree.

## Core Responsibilities
* **Page Management:** Allocates and manages fixed-size disk pages (`BPlusTreePage`) that contain either internal routing keys or terminal leaf keys mapping to Row IDs.
* **Disk Paging:** The `BPlusDiskPager` reads and writes raw binary blocks utilizing the underlying file system, completely bypassing in-memory JSON representations for scalability.
* **Index Traversing & Mutating:** The `BinaryBPlusTreeIndex` orchestrates insertions, deletions, node splitting on overflow, and key lookups. 

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `BinaryBPlusTreeIndex.cs` | The primary orchestrator. Implements the high-level tree algorithms (Search, Insert, Split, Delete) for the B+Tree variant. |
| `BPlusDiskPager.cs` | Handles the low-level serialization and deserialization of the `BPlusTreePage` blocks to and from the physical `.btree` file. |
| `BPlusTreePage.cs` | Represents a single logical node (internal or leaf) residing within a disk-bound memory page in the B+Tree structure. |

## Dependencies & Interactions
This module is instantiated by the `IndexManager` whenever a table explicitly designates its index type to be a B+Tree. It acts entirely independently of the evaluation layer or SQL parser, fulfilling requests passed down via the `IIndex` interface present in `DataVo.Core/BTree/Core`.

## Implementation Specifics
* **Supported Capabilities:** Exact match lookup, range scans (via linked leaves), node splitting due to capacity overflow, and strict binary persistence (no JSON overhead).
* **Limitations:** Dynamic shrinking or merging of nodes upon deletion is currently naïve. Tombstoning or zeroing out is favored over complex rebalancing during deletion to optimize write throughput.
