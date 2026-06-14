# JoinStrategies Overview

The `JoinStrategies` module contains the physical join algorithms used during query evaluation.

A strategy implements `IJoinStrategy` and is selected based on join type (`INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`) and a simple size heuristic for hash lookup vs nested loops.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `IJoinStrategy.cs` | Defines the central `Execute` method contract and manages the universal `HashLookupThreshold` for dynamically swapping between nested loops and optimized hash joins. |
| `InnerJoinStrategy.cs` | Implements inner join behavior. Drops unmatched sequences on both sides. Depending on input sizes, toggles between hash lookup and nested loop join. |
| `LeftJoinStrategy.cs` | Implements left outer join behavior. Fully preserves the left table dataset, generating `null`-padded dictionary rows for unmatched right-side mappings. |
| `RightJoinStrategy.cs` | Implements right outer join behavior. Iterates the preserved right dataset, cleanly padding missing left payload references. |
| `FullJoinStrategy.cs` | Implements full outer join behavior. Combines bidirectional tracking cleanly padding unmatched elements actively on both source and target sides. |
| `CrossJoinStrategy.cs` | Computes the unrestricted Cartesian product (`N x M` matrices), indiscriminately merging tables when `ON` clauses are absent. |
| `JoinLookupTable.cs` | Provides a grouped dictionary wrapper (`Dictionary<dynamic, List<Record>>`) to rapidly associate common keys during hash-based lookups. |
| `JoinStrategyContext.cs` | Encapsulates state handling functionality (e.g. determining row hash identities, creating null-padded target rows) across join boundaries. |

## Dependencies & interactions

Strategies are triggered during statement evaluation (via the statement mechanism/evaluator layer) and operate on the in-memory row/table types in `Parser/Types`.

## Notes

- `IJoinStrategy.HashLookupThreshold` controls when implementations switch to hash lookup.
