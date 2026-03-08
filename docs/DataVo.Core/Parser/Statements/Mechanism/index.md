# Mechanism Overview

The `Mechanism` sub-package contains the core expression evaluation engine that powers WHERE clause filtering. It translates parsed expression trees into filtered result sets by choosing optimal access paths (B-Tree index lookups vs. full table scans).

## Core Responsibilities

- **Expression Evaluation**: Recursively walks binary expression trees, evaluating leaf conditions and combining results with `AND` (intersection) and `OR` (union) operators.
- **Index Optimization**: Automatically detects when an equality condition targets an indexed column and routes the lookup through the B-Tree `IndexManager` instead of scanning all rows.
- **Predicate Building**: Constructs per-row filter predicates for range comparisons (`>`, `<`, `>=`, `<=`), inequality (`!=`), and null checks (`IS NULL`, `IS NOT NULL`).

## Components

| Component | Purpose |
| :--- | :--- |
| `ExpressionEvaluatorCore<T>` | Abstract base class that implements the recursive evaluation algorithm. Handles node type dispatch, normalization, and logical operator combination. Subclasses define what "a result set" looks like and how leaf conditions are evaluated. |
| `StatementEvaluator` | Concrete evaluator for queries with JOINs. Returns `HashedTable` (keyed by `JoinedRowId`). After filtering base table rows, automatically passes them through the configured `Join` strategy. Used by `Select` queries with JOIN clauses. |
| `StatementEvaluatorWOJoin` | Lightweight evaluator for single-table operations. Returns `HashSet<long>` (row IDs only). Used by `DELETE`, `UPDATE`, and single-table `SELECT` queries where no JOIN is needed. |

## How It Works

1. The `Evaluate(ExpressionNode)` method in `ExpressionEvaluatorCore` receives the root of a WHERE clause expression tree.
2. **Literal nodes** (`TRUE` / `FALSE`) short-circuit to returning all rows or an empty set.
3. **Comparison nodes** are normalized (column on the left, literal on the right) and dispatched:
   - `column = literal` → `HandleIndexableStatement` (tries index, then scan)
   - `column <op> literal` → `HandleNonIndexableStatement` (full scan)
   - `column <op> column` → `HandleTwoColumnExpression` (full scan)
   - `literal <op> literal` → `HandleConstantExpression` (evaluate once)
4. **Logical nodes** (`AND` / `OR`) recursively evaluate both children and combine with intersection or union.

## Dependencies

- **`IndexManager`** — for B-Tree index lookups (`FilterUsingIndex`).
- **`StorageContext`** — for loading rows from disk by row ID.
- **`ExpressionValueComparer`** — for type-aware equality and ordering comparisons.
- **`ExpressionNodeNormalizer`** — for ensuring column references are on the left side of comparisons.

## Consumers

This package is used by:
- `Select` (DQL) — via `StatementEvaluator` (with JOIN) or `StatementEvaluatorWOJoin`.
- `DeleteFrom` (DML) — via `StatementEvaluatorWOJoin` to identify rows for deletion.
- `Update` (DML) — via `StatementEvaluatorWOJoin` to identify rows for modification.
