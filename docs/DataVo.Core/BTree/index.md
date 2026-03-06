# BTree Overview
The `BTree` module is the localized indexing and clustering subsystem within `DataVo.Core`. It encapsulates the logic for constructing, persisting, and traversing both B-Tree and B+Tree data structures on disk, providing high-performance data retrieval capabilities necessary for a functional SQL engine.

## Core Responsibilities
* **Index State Management:** Initializes and manages instances of disk-backed tree indexes mapping keys to database row identifiers.
* **Node Serialization:** Handles the encoding and decoding of index keys and tree nodes for byte-level persistence.
* **Algorithmic Abstraction:** Provides modular implementations of specific balancing algorithms (e.g., standard Binary B-Trees and B+Trees) inside its subcomponents.
* **Diagnostic Telemetry:** Exposes utilities for dumping the structural topology of the active indexes to aid in debugging root-to-leaf paths.

## Component Breakdown

| Component (File/Dir) | Architectural Role |
|----------------------|--------------------|
| `BPlus/` | Contains the specific subclass implementations for standard B+Tree leaf and internal node variants optimized for range scans. |
| `Binary/` | Contains the classic B-Tree variant logic focusing on raw binary key-value distributions. |
| `Core/` | Encapsulates the abstract base classes and shared interfaces defining overarching tree topology mechanics. |
| `BTreeDumpUtility.cs` | Provides diagnostic routines to visualize and stringify the internal state of a B-Tree for debugging purposes. |
| `BTreeNode.cs` | Represents a generic node within the B-Tree hierarchy, encapsulating the list of keys and child pointers. |
| `IndexKeyEncoder.cs` | Handles the binary serialization and deserialization of strongly-typed index keys mapping them to underlying row identifiers. |
| `IndexManager.cs` | Orchestrates the lifecycle of created indexes, coordinating read/write access and acting as the primary entry point for the StorageEngine. |

## Dependencies & Interactions
This module is invoked primarily by the `StorageEngine` and `Services` layers during `INSERT`, `UPDATE`, `DELETE`, and fast-path `SELECT` evaluations to either mutating or look up row offsets based on indexed columns. It directly acts upon disk files through internal file streams, maintaining isolated persistence from the general table rows.
