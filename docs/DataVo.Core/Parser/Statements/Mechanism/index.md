# Mechanism Overview
The `Mechanism` sub-package is the brutal computational engine powering raw evaluation bounds translating filtering constraints (e.g., `WHERE` clauses) cleanly onto multi-gigabyte collections mapped physically into the active execution cache.

## Core Responsibilities
* **Logical Filtering:** Evaluates tree definitions isolating row conditions validating strings natively bypassing redundant physical disk traversals intelligently.
* **Loop Iteration:** Tracks the primary iterator processing records seamlessly resolving column identifiers mapping aliases distinctly across complex join overlaps.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `ExpressionEvaluatorCore.cs` | The standalone binary logic evaluation solver aggressively analyzing arbitrary math, logic statements (`> 15 AND Name = 'A'`), mapping variables independently across nested data sources reliably. |
| `StatementEvaluator.cs` | The primary iteration loop orchestrating the entire database result calculation integrating active joins, filtering where trees bounding outputs natively. |
| `StatementEvaluatorWOJoin.cs` | Optimized, streamlined execution loop completely eschewing relational algebra contexts prioritizing raw speed analyzing singular table outputs exclusively. |

## Dependencies & Interactions
Executed aggressively by `Select`, `DeleteFrom`, and `Update` operations to identify exact record bounds mapping output sets dynamically extracting row identities passed subsequently referencing targeted disk locations mapped natively across the memory pool limits internally.
