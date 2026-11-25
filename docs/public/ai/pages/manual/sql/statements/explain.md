# EXPLAIN

> Source route: /manual/sql/statements/explain
> Source file: manual/sql/statements/explain.md

`EXPLAIN` answers two questions about a `SELECT`:

1. **How does it execute?** The physical operator that produced the rows (`PrimaryKeyPointLookup`, `CompiledIndexLookup`, `ColumnarSimdAggregate`, `IntegerHashJoin`, `VectorHnswScan`, `StreamingTableScan`, …) and whether that is a typed, zero-allocation fast path.
2. **If it is slow, why?** When the statement falls back to the general dictionary evaluator, `FallbackReason` names the exact clause that disqualified each fast path (`SELECT DISTINCT`, `a computed projection`, `OR across joined tables`, `Explicit transaction MVCC scan`, …).

## Syntax

```sql
EXPLAIN SELECT ...;
```

::: warning Supported Statements
`EXPLAIN` accepts **only a single `SELECT` statement**. Queries using `UNION`, common table expressions (`WITH ...`), or DML statements (`INSERT`, `UPDATE`, `DELETE`) are not supported by `EXPLAIN` and throw a `ParserException`.
:::

::: info EXPLAIN runs the query once
Which fast path takes a statement is decided while it reads (an index may be missing a key, a storage engine may not stream a shape), so a static estimate cannot report it faithfully. `EXPLAIN` therefore runs the `SELECT` once under an execution trace, discards the result rows, and reports what actually ran, like `EXPLAIN ANALYZE` in other databases. `SELECT` has no side effects, but on a very large scan `EXPLAIN` costs one execution. If the statement fails when run (for example a subquery over a missing table), `EXPLAIN` still returns the planner columns and puts the error in `Details`.
:::

## Output Columns

`EXPLAIN` returns **1 row** with these **15 columns**:

| Column | .NET Type | Description |
| --- | --- | --- |
| `Plan` | `string` | The general evaluator's logical plan (`VolcanoNoJoin`, `VolcanoInnerJoin`, `LegacyNoJoinScan`, `LegacyWhereExpression`, `LegacyWhereJoin`, `LegacyJoinOnly`). Applies only when `Operator` is `GeneralDictionaryEvaluator`. |
| `Physical` | `string` | The general evaluator's pipeline: `"Volcano"` or `"Legacy"`. |
| `EstimatedCost` | `int` | Heuristic relative cost of that plan (`>= 1`). |
| `Reason` | `string` | Why the planner chose that plan over the alternative. |
| `Operator` | `string` | The operator that actually produced the rows (see [Operators](#operators)). `null` when the statement failed to run. |
| `FastPath` | `bool` | `true` when `Operator` is a typed fast path (no per-row dictionaries; zero managed allocation per scanned row). |
| `FallbackReason` | `string` | `null` on a fast path. Otherwise the first fast path that declined and why, as `"<Path>: <reason>"` — e.g. `TypedJoin: OR across joined tables`. |
| `DeclinedPaths` | `string` | Every fast path that declined, in the order they were tried, separated by `; `. Also filled when a later fast path (such as `VectorHnswScan`) accepted. |
| `EstimatedRows` | `long` | The planner's row estimate for the `FROM` table. |
| `RowsScanned` | `long` | Rows the operator read from storage (index lookups: rows fetched by key; scans: rows visited; columnar aggregates and index counts: `0`). `null` when the operator does not count them. |
| `RowsReturned` | `long` | Rows the `SELECT` returned. |
| `SegmentsRead` | `long` | Columnar chunk groups read from SSTables (LSM out-of-core columnar scans). |
| `SegmentsSkipped` | `long` | Columnar chunk groups skipped because their zone maps (per-chunk min/max) ruled the `WHERE` out — never paged in, never copied. |
| `IndexesUsed` | `string` | Indexes the operator read, comma separated (`_PK_<table>` is the primary key). |
| `Details` | `string` | Operator detail: join probe and build steps, vector routing, the general plan and reason, or the execution error. |

## Operators

| `Operator` | `FastPath` | When it runs |
| --- | --- | --- |
| `PrimaryKeyPointLookup` | ✅ | Equality on a single-column primary key. Reads only the matching row through the key index. |
| `CompiledIndexLookup` | ✅ | Equality on a secondary B-Tree index. Reads only the rows the index names, decoded typed. |
| `PrimaryKeyRangeScan` | ✅ | A range (`<`, `<=`, `>`, `>=`) on an integer or clustered primary key: only the rows in the range are read. |
| `ColumnarSimdAggregate` | ✅ | `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` over a table with a `USING COLUMNAR` index: folded over dense columns with SIMD kernels, no row decoded. On LSM this includes flushed SSTable segments with zone-map skipping. |
| `IndexGroupCount` | ✅ | `COUNT(*) ... GROUP BY` answered from an index's key counts. |
| `StreamingTableScan` | ✅ | A typed scan that decodes only the columns the query reads, with filters, aggregates or a bounded top-N applied while streaming. |
| `MvccSnapshotScan` | ✅ | The typed scan inside an explicit transaction: committed rows of the snapshot, retained images, and the transaction's own writes. |
| `IntegerHashJoin` | ✅ | `INNER`/`LEFT` equi-joins on integer columns of stored tables: hash builds per joined table, a streaming (or index-driven) probe. |
| `VectorHnswScan` | ✅ | `ORDER BY <distance> LIMIT k` or a distance-threshold `WHERE` served by an HNSW index; `Details` shows expansion passes and hybrid routing. |
| `ConstantProjection` | ✅ | `SELECT` of constants without `FROM`. |
| `GeneralDictionaryEvaluator` | ❌ | The general evaluator: rows materialized as dictionaries and run through the Volcano or legacy pipeline chosen by the planner (`Plan`, `Physical`). |

## Fallback Reasons

A `FallbackReason` is `"<Path>: <reason>"`, where `<Path>` is `TypedScan` (single table) or `TypedJoin`.

| Reason | What to change |
| --- | --- |
| `SELECT DISTINCT` | Use `GROUP BY` on the same columns (typed), or accept the general path. |
| `a computed projection` | Project plain columns and compute in the application, or accept the general path. Vector distance projections are still served by `VectorHnswScan`. |
| `a window function` | Window functions run on the general evaluator. |
| `a common table expression (WITH)` / `a derived table or CTE source` | Query the stored table directly. |
| `OR across joined tables` | Each `OR` must read one table; split the query or move the condition into the join. |
| `Explicit transaction MVCC scan` | Joins inside an explicit transaction read the snapshot through the general evaluator; single-table reads stay typed (`MvccSnapshotScan`). |
| `a subquery in WHERE` | Join instead, or accept the general path. |
| `a comparison that is not column-versus-literal (<op>)` | Compare a column with a literal or parameter (no arithmetic, no column-to-column comparison). |
| `the operator <op>` / `a comparison with a NULL literal` | Use `=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `IS [NOT] NULL`. |
| `LIKE whose pattern is not a literal` | Pass the pattern as a literal or parameter. |
| `equality on <col>, one column of a composite primary key` | Composite-key lookups use the general key path. |
| `GROUP BY without aggregates` / `GROUP BY or HAVING without aggregates` | Add an aggregate or use the general path. |
| `the join is not an INNER/LEFT equality of integer columns of distinct stored tables` | Typed joins need integer keys; text, GUID, `RIGHT`/`FULL`/`CROSS` joins and self-joins use the general path. |
| `an ORDER BY key that is not a joined column` | Order joined rows by a column of a joined table. |
| `the storage engine cannot stream typed rows` | A custom (`Custom`/`Wasm`) storage engine that does not stream typed rows. |

## Usage Examples

### SQL

```sql
-- A fast path: Operator = PrimaryKeyPointLookup, FastPath = true, RowsScanned = 1
EXPLAIN SELECT name FROM players WHERE id = 42;

-- A fallback: Operator = GeneralDictionaryEvaluator,
-- FallbackReason = 'TypedJoin: OR across joined tables'
EXPLAIN SELECT p.name
FROM players p
INNER JOIN teams t ON p.team_id = t.id
WHERE p.score > 48 OR t.name = 'team-3';

-- LSM out-of-core columnar aggregate: SegmentsSkipped counts zone-map skips
EXPLAIN SELECT COUNT(*) FROM readings WHERE reading > 49000;
```

### Inspecting `EXPLAIN` Output in C#

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

db.Execute("CREATE DATABASE App; USE App;");
db.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR(80), region VARCHAR(16), score INT)");

var row = db.Execute("EXPLAIN SELECT DISTINCT region FROM players")[0].Data[0];

Console.WriteLine($"Operator:       {row["Operator"]}");       // GeneralDictionaryEvaluator
Console.WriteLine($"FastPath:       {row["FastPath"]}");       // False
Console.WriteLine($"FallbackReason: {row["FallbackReason"]}"); // TypedScan: SELECT DISTINCT
Console.WriteLine($"RowsScanned:    {row["RowsScanned"]}");
Console.WriteLine($"Details:        {row["Details"]}");        // the general plan and its reason
```

## General Evaluator Plans (`Plan` / `Physical`)

| `Plan` | `Physical` | When Selected |
| --- | --- | --- |
| `VolcanoNoJoin` | `Volcano` | Single-table query with `EnableVolcanoExecution = true`, no `HAVING`, and no `WHERE` subquery, when Volcano's estimated cost is lower than Legacy's. |
| `VolcanoInnerJoin` | `Volcano` | Query with a connected all-`INNER JOIN` graph, `EnableVolcanoExecution = true`, and no `WHERE` subquery, when Volcano's estimated cost is lower than Legacy's (or when join-cardinality feedback persistence is enabled). |
| `LegacyNoJoinScan` | `Legacy` | Single-table query without `WHERE` when Legacy is cheaper than Volcano, or when Volcano is disabled or ineligible (for example, `HAVING` is present). |
| `LegacyWhereExpression` | `Legacy` | Filtered query whose `WHERE` predicate includes arithmetic (`+`, `-`, `*`, `/`), scalar functions, or aggregate arguments requiring expression evaluation, when Legacy is chosen or Volcano is ineligible. |
| `LegacyWhereJoin` | `Legacy` | Filtered join query (or single-table simple `WHERE` query) where Legacy is estimated cheaper, or where the query uses non-`INNER` joins, disconnected joins, subqueries, or `HAVING`. |
| `LegacyJoinOnly` | `Legacy` | Join query without a `WHERE` clause when Legacy is cheaper or when Volcano cannot handle the join shape (such as `LEFT`, `RIGHT`, `FULL`, or `CROSS` joins). |

## Common `Reason` Messages & Fallbacks

| `Reason` Pattern | Meaning |
| --- | --- |
| `single-table filter with no subquery; compared to <LegacyPlan> cost <N>` | Volcano won the cost comparison for a filtered single-table scan. |
| `JOIN graph is fully INNER and connected; compared to LegacyWhereJoin cost <N>` | Volcano won the cost comparison for a connected `INNER JOIN` with `WHERE`. |
| `join-only query has connected INNER JOIN graph; compared to LegacyJoinOnly cost <N>` | Volcano won the cost comparison for a connected `INNER JOIN` without `WHERE`. |
| `join feedback persistence enabled; ...; compared to <LegacyPlan> cost <N>` | `EnableVolcanoJoinCardinalityFeedbackPersistence` forced the Volcano join pipeline so learned join statistics can be persisted. |
| `legacy filter path estimated cheaper for current predicate; compared to VolcanoNoJoin cost <N>` | Both paths were eligible, but Legacy had lower startup/estimated cost. |
| `legacy join path estimated cheaper for current predicate; compared to VolcanoInnerJoin cost <N>` | Both join paths were eligible, but Legacy had lower estimated cost. |
| `legacy no-join scan estimated cheaper; compared to VolcanoNoJoin cost <N>` | Simple full scan without `WHERE` had lower estimated cost on Legacy. |
| `expression predicate requires legacy evaluator` | Volcano was ineligible (disabled, `HAVING`, or subquery) and the `WHERE` clause uses arithmetic or scalar functions. |
| `fallback where/join evaluation` | Volcano was ineligible for a filtered query (for example, `LEFT JOIN`, disconnected join, or `WHERE` subquery). |
| `join without where uses legacy join strategy` | Join without `WHERE` uses a non-`INNER` or disconnected join, or Volcano is disabled. |
| `volcano disabled or unsupported` | Single-table query without `WHERE` could not use Volcano (disabled in config or `HAVING` present). |

::: tip Planner estimate vs. executed operator
- `Plan`, `Physical`, `EstimatedCost` and `Reason` come from the planner's cost model, which avoids materializing table rows, so small row counts often favor `Legacy` (startup `7–10`) over `Volcano` (`20–30`). They describe what the general evaluator *would* do.
- `Operator`, `FastPath` and `FallbackReason` describe what *did* run: the typed fast-path cascade (point lookup, key range, SIMD columnar aggregate, typed hash join, HNSW) runs ahead of the general evaluator. See [Query Planner & Fast Paths](/manual/internals/query-planner).
:::

## See Also

- [SELECT](/manual/sql/statements/select): Full `SELECT` syntax, joins, grouping, and subqueries
- [Query Planner & Fast Paths](/manual/internals/query-planner): Runtime dispatch cascade for typed single-table, join, and vector fast paths
- [CREATE INDEX](/manual/sql/statements/indexes): Adding B-Tree, HNSW, FLAT, and Columnar indexes to speed up queries
- [Configuration Reference](/manual/reference/configuration): `EnableVolcanoExecution` and join-cardinality feedback settings
