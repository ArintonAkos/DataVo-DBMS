# JoinStrategies Overview
The `JoinStrategies` module implements the complex relational algebraic algorithms utilized when integrating disparate tables mapped dynamically during query definitions. Rather than providing a single monolithic evaluator, this module implements a rich Strategy pattern defining highly optimized iteration algorithms dictating distinct performance profiles.

## Core Responsibilities
* **Relational Calculus:** Calculates Cartesian products defining logic constraints seamlessly filtering records linking specific unique indices effectively.
* **Algorithm Selection:** Translates defined Enum targets capturing `INNER`, `LEFT`, or `FULL` strings determining which loop mapping strategy explicitly applies.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `IJoinStrategy.cs` | Establishes the mutual interface dictating calculation boundaries defining standard `PerformJoin` evaluation footprints. |
| `CrossJoinStrategy.cs` | Implements brute-force Cartesian arrays producing `N x M` output matrices capturing distinct row permutations indiscriminately. |
| `InnerJoinStrategy.cs` | Uses localized indexed lookups bypassing physical nesting loops dynamically validating referential overlap actively matching column variants seamlessly. |
| `LeftJoinStrategy.cs, RightJoinStrategy.cs` | Isolates outer relational joins tracking primary table constraints seamlessly padding non-existent referencing payloads utilizing native `null` arrays aggressively optimizing empty targets proactively. |
| `FullJoinStrategy.cs` | Represents total inclusive boundaries combining outer projections resolving empty associations bi-directionally. |
| `JoinLookupTable.cs` | In-memory structural hash-map tracking distinct equality relations enabling ultra-fast scalar evaluations drastically outperforming standard `O(n^2)` iteration costs dynamically mapping right-side arrays securely. |

## Dependencies & Interactions
Initiated through the `JoinStrategyContext.cs` which maps logical SQL enums translating targeted query shapes directly launching applicable strategies inside `StatementEvaluator.cs`. Produces merged datasets utilizing ephemeral abstractions defined via `Parser/Types`.

## Implementation Specifics
* **Performance Enhancements:** Strongly favors caching algorithms mapping the smaller table physically utilizing `JoinLookupTable.cs` drastically accelerating index checks avoiding manual loop iteration when mapping large Cartesian domains.
