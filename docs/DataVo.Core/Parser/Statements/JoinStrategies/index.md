# JoinStrategies Overview

The `JoinStrategies` module implements the algorithms used to evaluate SQL `JOIN` constraints between disparate tables. It utilizes the Strategy design pattern (`IJoinStrategy`), allowing the query planner to dynamically select the appropriate iterative algorithm matching the parsed SQL `JOIN` type (`INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`).

## Core Responsibilities
* **Relational Calculus:** Calculates Cartesian products and enforces conditional bounds defined in `ON` logic, dynamically linking referenced data sequences.
* **Join Resolution:** Maps specific query targets (`INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`) to dedicated evaluation workflows optimizing memory allocations and sequential lookups.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `IJoinStrategy.cs` | Defines the central `Execute` method contract and manages the universal `HashLookupThreshold` for dynamically swapping between nested loops and optimized hash joins. |
| `InnerJoinStrategy.cs` | Implements inner join behavior. Drops unmatched sequences on both sides. Depending on target sizes, natively toggles between `ExecuteHashJoin` and `ExecuteNestedLoopJoin` logic. |
| `LeftJoinStrategy.cs` | Implements left outer join behavior. Fully preserves the left table dataset, generating `null`-padded dictionary rows for unmatched right-side mappings. |
| `RightJoinStrategy.cs` | Implements right outer join behavior. Iterates the preserved right dataset, cleanly padding missing left payload references. |
| `FullJoinStrategy.cs` | Implements full outer join behavior. Combines bidirectional tracking cleanly padding unmatched elements actively on both source and target sides. |
| `CrossJoinStrategy.cs` | Computes the unrestricted Cartesian product (`N x M` matrices), indiscriminately merging tables when `ON` clauses are absent. |
| `JoinLookupTable.cs` | Provides a grouped dictionary wrapper (`Dictionary<dynamic, List<Record>>`) to rapidly associate common keys during hash-based lookups. |
| `JoinStrategyContext.cs` | Encapsulates state handling functionality (e.g. determining row hash identities, creating null-padded target rows) across join boundaries. |

## Dependencies & Interactions
Triggered explicitly during evaluation within `StatementEvaluator.cs`. Strategies receive the `JoinStrategyContext` which manages standard table abstractions and constructs target row identities via `JoinedRowId` identifiers cleanly isolating execution loops.

## Implementation Specifics
* **Performance Enhancements:** By default, outer join implementations toggle from un-indexed Nested Loop algorithms to an aggressively grouped Hash Lookup strategy when the right target dataset size meets the `IJoinStrategy.HashLookupThreshold`. This bypasses expensive sequential checks without fully committing to pre-calculated indexes.
