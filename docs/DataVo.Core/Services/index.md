# Services Overview
The `Services` module contains high-level orchestrators that bridge the gap between the purely analytical `Parser` module and the low-level physical manipulations of the `StorageEngine`. It enforces business constraints, handles complex relational operations, and sequences execution steps.

## Core Responsibilities
* **Execution Orchestration:** Executes multi-step logical plans, such as coordinating subqueries or applying post-fetch aggregations.
* **Constraint Enforcement:** Validates that operations (like table creation or column modification) do not violate the systemic constraints defined in the catalog.
* **Relational Math Operations:** Isolates specific algorithms like topological sorting for foreign key validation or aggregation hashing.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `AggregationService.cs` | Applies mapping and reduction functions to compute aggregates over a set of fetched column values. |
| `TableColumnService.cs` | Validates and manages the addition, removal, or type coercion of columns against active table schemas. |
| `TableParserService.cs` | Assists the primary parser in resolving complex table alias bindings and correlated subqueries. |
| `TableService.cs` | Acts as the primary manager for DDL table operations, coordinating with the storage engine to create or drop underlying physical files. |
| `TopologicalSort.cs` | Implements graph sorting algorithms primarily utilized to resolve referential dependency chains during table drops or cascading inserts. |

## Dependencies & Interactions
The `Services` are instantiated globally via the `DataVoContext` and invoked predominantly after a statement object has been yielded by the `Parser`. They act as intermediaries, interpreting the parsed `Models` and commanding the `StorageEngine` to manipulate the corresponding physical files or `BTree` indices.
