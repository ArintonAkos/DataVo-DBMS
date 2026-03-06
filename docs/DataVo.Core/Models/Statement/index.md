# Statement (Models) Overview
The `Statement` module contains the sub-clause models acting as foundational building blocks for overarching queries like the `SELECT` or `UPDATE` models. Rather than representing an independent query command, these objects represent isolated SQL clauses describing filtering, joining, or aggregating mechanics.

## Core Responsibilities
* **Condition Wrapping:** Represent raw logical filtering conditions built into Expression trees indicating inclusion or exclusion thresholds.
* **Algebraic Scoping:** Map structural relationship semantics dictating data consolidation routes like Cartesian products or column reduction groupings.

## Component Breakdown

| Component (File/Dir) | Architectural Role |
|----------------------|--------------------|
| `Utils/` | Sub-package defining ephemeral evaluation structures like generic `Record` definitions handling active memory tables. |
| `AggregateModel.cs` | Represents mathematical formulas (e.g., `SUM`, `AVG`) invoked actively upon selected output expressions. |
| `GroupByModel.cs` | Wraps targeted columns and bound conditions determining how independent rows are compressed into distinct aggregate clusters. |
| `JoinModel.cs` | Tracks the specific table binding targets, configured join methodologies (such as `LEFT` or `CROSS`), and relational predicate identifiers combining isolated catalogs. |
| `WhereModel.cs` | Captures binary expression trees dictating truthy logic evaluating against a given context. |

## Dependencies & Interactions
Constructed by targeted procedural parsers inside `DataVo.Core/Parser/Statements/`, these models bind directly attributes on parent classes like `SelectModel` or `UpdateModel`. They abstract the complex hierarchical tokens representing an operation until it is translated into an actionable state by the evaluator.

## Implementation Specifics
* **Join Model Support:** Actively records the parsed variation of join operations dynamically instructing `JoinStrategies` if processing involves hashes or classical loop iterations.
