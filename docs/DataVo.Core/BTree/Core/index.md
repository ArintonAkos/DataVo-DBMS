# Core (BTree) Overview
The `Core` module within the `BTree` directory establishes the foundational contracts and interface primitives required by all underlying indexing strategies in `DataVo.Core`. 

## Core Responsibilities
* **Abstraction Guarantee:** Defines the `IIndex` interface, guaranteeing that the storage and evaluation engine can query any tree structure uniformly.
* **Variant Definitions:** Identifies the algorithmic approach utilized by a specific file via the `IndexType` enumeration.
* **Legacy Structures:** Contains the original, un-optimized `JsonBTreeIndex` used during early prototyping phases of the storage engine.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `IIndex.cs` | The universal interface defining operations like `Insert`, `Delete`, and `Lookup` that all B-Tree variants must implement. |
| `IndexType.cs` | An enumeration indicating whether an index file is serialized as JSON, a standard Binary B-Tree, or a Binary B+Tree. |
| `JsonBTreeIndex.cs` | A legacy index implementation that serializes the entire tree state to a JSON string on disk. |

## Dependencies & Interactions
This module is the bedrock of the indexing subsystem. The `IndexManager` centrally relies on `IIndex` to avoid tight coupling to the physical `BinaryBTreeIndex` or `BinaryBPlusTreeIndex` implementations, enabling polymorphic behavior based on the config state tracking the index metadata. 

## Implementation Specifics
* **Note on JsonBTreeIndex:** The `JsonBTreeIndex` is functionally supported but actively deprecated for production database usage due to the severe performance bottleneck of materializing entire strings into memory during every read/write. It is retained strictly as a fallback or for specialized debugging routines.
