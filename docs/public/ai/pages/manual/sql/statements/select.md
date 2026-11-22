# SELECT & EXPLAIN

> Source route: /manual/sql/statements/select
> Source file: manual/sql/statements/select.md

`SELECT` retrieves, filters, joins, aggregates, and ranks rows from one or more tables. `EXPLAIN` inspects the query planner's chosen execution path without returning table rows.

```sql
SELECT p.Region, COUNT(*) AS PlayerCount, AVG(p.Score) AS AvgScore
FROM players p
WHERE p.Score >= 100
GROUP BY p.Region
HAVING COUNT(*) >= 5
ORDER BY AvgScore DESC
LIMIT 10 OFFSET 0;
```

## Clause Reference

| Clause | Syntax | Notes |
| --- | --- | --- |
| `SELECT` | `SELECT [DISTINCT] <expr> [AS alias], ...` | Supports `*`, column refs, aliases, aggregates, `RANK()`, scalar functions, arithmetic, and vector distance. |
| `FROM` | `FROM <table> [AS alias]` | Accepts base tables and CTE names. |
| `JOIN` | `INNER \| LEFT \| RIGHT \| FULL [OUTER] \| CROSS JOIN <table> [alias] ON <cond>` | `INNER` and `LEFT` on integer equality keys use typed hash joins; others use the general evaluator. |
| `WHERE` | `WHERE <predicate>` | Supports `AND`, `OR`, `NOT`, comparisons, `IN`, `BETWEEN`, `LIKE`/`NOT LIKE`, `IS [NOT] NULL`, subqueries, and vector distance predicates. |
| `GROUP BY` | `GROUP BY <col>, ...` | Groups rows before applying aggregate functions. |
| `HAVING` | `HAVING <predicate>` | Filters grouped results; also works **without** `GROUP BY` (treats the filtered table as a single group). |
| `ORDER BY` | `ORDER BY <expr> [ASC \| DESC], ...` | Supports multi-column sorting and vector distance ranking. |
| `LIMIT` / `OFFSET` | `LIMIT <n> [OFFSET <m>]` | `ORDER BY ... LIMIT k` uses a bounded top-k heap on typed scans and joins. |

## Joins

```sql
SELECT u.Name, d.Name AS Department
FROM Users u
LEFT JOIN Departments d ON u.DepartmentId = d.Id;
```

`INNER JOIN` and `LEFT JOIN` on integer equality keys run on DataVo's typed hash-join path across any number of tables. `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, self-joins, text/`GUID` join keys, and cross-table `OR` predicates are fully supported on the general evaluator.

## CTEs (`WITH`), Subqueries, and `UNION`

Non-recursive CTEs are materialized before the main query runs. `WITH RECURSIVE` is supported for [Live Reactive Views](/manual/fundamentals/reactive-views) only.

```sql
WITH HighScorers AS (
    SELECT Id, Name, Region FROM players WHERE Score >= 900
)
SELECT Region, COUNT(*) AS EliteCount
FROM HighScorers
GROUP BY Region;
```

- **Subqueries:** Use `col IN (SELECT ...)`, `EXISTS (SELECT ...)`, and `NOT EXISTS (SELECT ...)` in `WHERE`.
- **Set operations:** Combine compatible `SELECT` branches with `UNION` (deduplicated) or `UNION ALL` (preserves duplicates). Parenthesized `UNION` branches, `INTERSECT`, and `EXCEPT` are not supported in `v0.4`.

## `EXPLAIN`

Prefix any `SELECT` statement with `EXPLAIN` to inspect how the planner executes it:

```sql
EXPLAIN SELECT Id, Name FROM players WHERE Score > 500 ORDER BY Score DESC LIMIT 10;
```

`EXPLAIN` returns a single diagnostic row with four columns:

| Column | Description |
| --- | --- |
| `Plan` | High-level logical plan summary |
| `Physical` | Selected physical operator or fast path (e.g., typed scan, columnar aggregate, HNSW search) |
| `EstimatedCost` | Planner's estimated relative execution cost |
| `Reason` | Why the planner chose (or fell back from) a fast path |

## See Also
- [Functions & Operators](/manual/sql/functions) — Aggregates, `RANK()`, `LOWER`/`UPPER`, and vector distance operators
- [Query Planner](/manual/internals/query-planner) — Deep dive into DataVo's typed fast paths and fallback rules
- [Streaming Queries](/manual/fundamentals/streaming-queries) — Lazy `StreamQuery` and zero-allocation `StreamRef` execution
