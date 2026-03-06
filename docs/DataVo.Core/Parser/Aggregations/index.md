# Aggregations (Parser) Overview
The `Aggregations` module comprises procedural calculators designed to interpret mathematical and structural column consolidations mapped during ad-hoc queries displaying scalar data accumulations.

## Core Responsibilities
* **Reduction Execution:** Operates as a factory executing calculation kernels designed to collapse array-bound table subsets into distinct scalar primitives.
* **Calculation Semantics:** Tracks exact calculation phases defining unique mathematical boundaries such as summing lists, analyzing counts, or determining threshold boundaries.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `Aggregation.cs` | The abstract parent dictionary defining how values compress progressively across iterative loops traversing result records. |
| `Avg.cs` | Processes the mathematical average across grouped numerical rows capturing discrete fraction calculations. |
| `Count.cs` | Dynamically executes cumulative counters representing logical instances. |
| `Max.cs, Min.cs` | Establishes the boundary calculations filtering sets logically comparing active indices against highest/lowest established baselines. |
| `Sum.cs` | Compiles raw addition aggregates progressively folding sets against total arithmetic sums. |

## Dependencies & Interactions
`Aggregation` configurations are invoked predominantly via the `AggregationService.cs` located inside the core logic pipeline matching logic against identifiers identified inside the `Models/Statement/AggregateModel.cs` schemas.

## Implementation Specifics
* **Supported Capabilities:** All standard arithmetic reductions natively implemented effectively interacting through `DataTypes` casting allowing complex internal SQL projections mapping seamlessly against integer sizes.
