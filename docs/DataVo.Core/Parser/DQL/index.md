# DQL (Parser Actions) Overview

The `DQL` component is responsible for read-oriented query execution, primarily `SELECT`. It evaluates filters, joins, grouping, aggregation, ordering, and projection while preserving read isolation semantics.

## Core Responsibilities

- **Select Sequencing:** Executes the logical query pipeline from source resolution to result projection.
- **Expression Evaluation:** Applies `WHERE`, `HAVING`, and join predicates against row sets.
- **Read Isolation:** Acquires shared table-level locks so multiple readers can proceed concurrently while writers wait.

## Component Breakdown

| Component (File) | Architectural Role                                                                                                                                                                                  |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Select.cs` and partials | Coordinates `SELECT` execution: table binding, lock acquisition, predicate evaluation, joins, grouping/window handling, projection, ordering, and limit/offset application. |
| `Select.Planner.cs` | Chooses between legacy and Volcano execution paths using join shape, predicate complexity, estimated row counts, selectivity, and feature-cost weights. |
| `Select.FastPathDecisions.cs` | Contains vector and hybrid fast-path decision helpers, including candidate sizing, selectivity estimates, and guardrails that decide when optimized vector predicate/order-by execution is eligible. |

## Dependencies & Interactions

`Select` depends on the statement-evaluation pipeline, binding metadata, and the storage layer. Before execution it resolves all referenced tables and acquires read locks through `LockManager`, ensuring that reads remain consistent with concurrent DML activity at the table level.

## Implementation Specifics

- **Execution Pathways:** Queries choose between filtered evaluation, join evaluation, or full table scan depending on the clauses present.
- **Lock Scope:** Read locks are acquired for every referenced table before row evaluation begins and are released after projection and limit handling complete.
