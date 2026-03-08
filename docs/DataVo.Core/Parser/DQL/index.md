# DQL (Parser Actions) Overview
The `DQL` component is responsible for read-oriented query execution, primarily `SELECT`. It evaluates filters, joins, grouping, aggregation, ordering, and projection while preserving read isolation semantics.

## Core Responsibilities
* **Select Sequencing:** Executes the logical query pipeline from source resolution to result projection.
* **Expression Evaluation:** Applies `WHERE`, `HAVING`, and join predicates against row sets.
* **Read Isolation:** Acquires shared table-level locks so multiple readers can proceed concurrently while writers wait.

## Component Breakdown

| Component (File) | Architectural Role |
|------------------|--------------------|
| `Select.cs` | The standalone orchestrator dynamically generating temporary internal cross-sections mapping `QueryResult` datasets utilizing distinct evaluation mechanisms mapping nested loop paths dynamically. |

## Dependencies & Interactions
`Select` depends on the statement-evaluation pipeline, binding metadata, and the storage layer. Before execution it resolves all referenced tables and acquires read locks through `LockManager`, ensuring that reads remain consistent with concurrent DML activity at the table level.

## Implementation Specifics
* **Execution Pathways:** Queries choose between filtered evaluation, join evaluation, or full table scan depending on the clauses present.
* **Lock Scope:** Read locks are acquired for every referenced table before row evaluation begins and are released after projection and limit handling complete.
