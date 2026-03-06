# DQL (Models) Overview
The `DQL` (Data Query Language) models represent read-only operations that extract, project, and visualize data from the database catalog without mutating its underlying structure or values. These models predominantly encompass the complex `SELECT` statement and meta-queries like `DESCRIBE`.

## Core Responsibilities
* **Select Projections:** Wrap the structural logic determining which specific columns (or `*`) are being asked for, including mapped aliases and aggregate expressions.
* **Component Grouping:** Act as a container binding together multiple sub-clauses (`WHERE`, `JOIN`, `GROUP BY`) into a unified, coherent definition representing the full query.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `DescribeModel.cs` | Captures the target table name required to fetch and format the internal Schema definition (types, constraints) for the user. |
| `SelectModel.cs` | A heavy abstraction enveloping the entire `SELECT` execution block, mapping out selected fields, data source bindings, inner join structures, grouping requirements, and filter trees. |
| `UseModel.cs` | Tracks the desired namespace representing the target catalog context an active session intends to switch to. |

## Dependencies & Interactions
Assembled primarily by `DataVo.Core/Parser/DQL/Select.cs`, these models act as blueprints subsequently executed by the relational math orchestrators in `DataVo.Core/Services` or the `DataVo.Core/Parser/QueryEngine.cs`. 

## Implementation Specifics
* **Select Model Extensibility:** Natively supports deeply nested clauses combining `WhereModel`, `JoinModel`, and `GroupByModel` objects logically defining exactly what conditions to map during the execution phase.
