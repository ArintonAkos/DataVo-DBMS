# EXPLAIN

> Source route: /manual/sql/statements/explain
> Source file: manual/sql/statements/explain.md

`EXPLAIN` validates a `SELECT` query against the active catalog and returns a single diagnostic row describing which logical plan and physical execution path the planner selects, along with its estimated cost and decision rationale—without executing the query or returning table rows.

## Syntax

```sql
EXPLAIN SELECT ...;
```

::: warning Supported Statements
In DataVo `v0.4`, `EXPLAIN` accepts **only a single `SELECT` statement**. Queries using `UNION`, common table expressions (`WITH ...`), or DML statements (`INSERT`, `UPDATE`, `DELETE`) are not supported by `EXPLAIN` and throw a `ParserException`.
:::

## Output Columns

`EXPLAIN` always returns **1 row** with the following **4 columns** (produced by `SelectPlannerDiagnostics`):

| Column | .NET Type | Description |
| --- | --- | --- |
| `Plan` | `string` | Logical plan variant chosen by the planner (`VolcanoNoJoin`, `VolcanoInnerJoin`, `LegacyNoJoinScan`, `LegacyWhereExpression`, `LegacyWhereJoin`, or `LegacyJoinOnly`). |
| `Physical` | `string` | Physical execution strategy selected: `"Volcano"` or `"Legacy"`. |
| `EstimatedCost` | `int` | Heuristic relative cost estimate (integer `>= 1`) combining startup cost, selectivity-weighted row cost, predicate complexity, and pipeline features (`ORDER BY`, `DISTINCT`, `GROUP BY`, `LIMIT`/`OFFSET`). |
| `Reason` | `string` | Human-readable explanation of why the plan was selected, including the competing plan's cost when Volcano and Legacy were both eligible. |

## Usage Examples

### SQL

```sql
-- Single-table filtered query
EXPLAIN SELECT id, name, score
FROM players
WHERE region = 'EU' AND score >= 500
ORDER BY score DESC
LIMIT 10;

-- Multi-table INNER JOIN query
EXPLAIN SELECT p.name, d.name
FROM players p
INNER JOIN departments d ON p.department_id = d.id
WHERE p.score > 100;
```

### Inspecting `EXPLAIN` Output in C#

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE App; USE App;");
db.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR(80), region VARCHAR(16), score INT)");

var result = db.Execute(
    "EXPLAIN SELECT id, name FROM players WHERE region = 'EU' AND score >= 500");

var row = result.Data[0];
Console.WriteLine($"Plan:          {row["Plan"]}");
Console.WriteLine($"Physical:      {row["Physical"]}");
Console.WriteLine($"EstimatedCost: {row["EstimatedCost"]}");
Console.WriteLine($"Reason:        {row["Reason"]}");
```

## Logical Plans & Physical Paths

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

::: tip Diagnostic Estimates vs. Runtime Fast Paths
- `EXPLAIN` intentionally avoids materializing table rows when estimating costs, so small or diagnostic row counts often favor `Legacy` due to its lower startup overhead (`7–10`) compared to `Volcano` (`20–30`).
- During actual query execution, DataVo also runs its **typed fast-path cascade** (B-Tree point lookup, PK range seek, SIMD columnar aggregate, typed hash/index join, and hybrid vector search) ahead of the general evaluator. See [Query Planner & Fast Paths](/manual/internals/query-planner) for runtime routing rules.
:::

## See Also

- [SELECT](/manual/sql/statements/select) — Full `SELECT` syntax, joins, grouping, and subqueries
- [Query Planner & Fast Paths](/manual/internals/query-planner) — Runtime dispatch cascade for typed single-table, join, and vector fast paths
- [CREATE INDEX](/manual/sql/statements/indexes) — Adding B-Tree, HNSW, FLAT, and Columnar indexes to speed up queries
- [Configuration Reference](/manual/reference/configuration) — `EnableVolcanoExecution` and join-cardinality feedback settings
