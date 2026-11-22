# SELECT

> Source route: /manual/sql/statements/select
> Source file: manual/sql/statements/select.md

The `SELECT` statement queries one or more tables or common table expressions (CTEs), applies row filters, joins, grouping, window ranking, ordering, and pagination, and returns a result set.

## Syntax

```sql
[WITH cte_name AS (select_statement) [, ...]]
SELECT [DISTINCT]
    { * | table_or_alias.* | expression [[AS] alias] } [, ...]
FROM table_name [[AS] alias]
    [{ [INNER] | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER] | CROSS }
     JOIN table_name [[AS] alias] [ON join_condition]] [...]
[WHERE filter_condition]
[GROUP BY column_ref [, ...]]
[HAVING group_condition]
[UNION [ALL] select_statement [...]]
[ORDER BY expression [ASC | DESC] [, ...]]
[LIMIT count [OFFSET skip]]
```

## Parameters

| Clause / Parameter | Description |
| --- | --- |
| `WITH cte_name AS (...)` | Defines one or more non-recursive common table expressions scoped to the query. |
| `DISTINCT` | Eliminates duplicate output rows after projection. |
| `expression [[AS] alias]` | Column reference, literal, arithmetic expression, aggregate, `RANK()` window expression, scalar function (`LOWER`, `UPPER`), or vector distance (`<->`, `<=>`). |
| `FROM table_name [[AS] alias]` | Source base table or CTE name, with an optional alias. |
| `JOIN ... ON join_condition` | Combines rows from multiple tables using `INNER`, `LEFT [OUTER]`, `RIGHT [OUTER]`, `FULL [OUTER]`, or `CROSS JOIN`. |
| `WHERE filter_condition` | Filters individual rows before grouping and projection. |
| `GROUP BY column_ref, ...` | Partitions matching rows into groups for aggregate evaluation. |
| `HAVING group_condition` | Filters aggregated groups; also supported without `GROUP BY` to filter a single whole-table aggregate group. |
| `UNION [ALL]` | Vertically combines the result rows of two or more compatible `SELECT` queries. |
| `ORDER BY expression [ASC \| DESC]` | Sorts result rows ascending (`ASC`, default) or descending (`DESC`). |
| `LIMIT count [OFFSET skip]` | Caps the number of returned rows and optionally skips the first `skip` rows. |

## Columns & Aliases

Project all columns with `*`, qualify columns by table or alias (`p.Name`), or compute derived values with arithmetic, scalar functions, aggregates, and vector distances. Use `AS` (optional) to assign an output column name.

```sql
SELECT
    p.Id,
    UPPER(p.Name) AS PlayerName,
    p.Score + 100 AS BoostedScore,
    p.Region
FROM players AS p;
```

- Column aliases are reflected in result headers and can be referenced in `ORDER BY`.
- When joining multiple tables that share column names (such as `Id`), qualify each reference with its table name or alias to avoid ambiguity errors.

## FROM

The `FROM` clause specifies the primary table or CTE to read. Assigning a short alias makes multi-table queries and qualified column references concise.

```sql
SELECT p.Name, p.Score
FROM players p;
```

## WHERE

The `WHERE` clause filters rows before grouping or window evaluation. DataVo supports comparison, logical, pattern, range, nullability, subquery, and vector distance predicates.

```sql
SELECT Id, Name, Score, Region
FROM players
WHERE Region IN ('EU', 'NA')
  AND Score BETWEEN 500 AND 1500
  AND Name LIKE 'A%'
  AND Name NOT LIKE '%_bot'
  AND Region IS NOT NULL;
```

Supported `WHERE` predicates include:

- **Comparison:** `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=` (text comparisons are ordinal UTF-8 code-point order).
- **Logical:** `AND`, `OR`, `NOT` (with parentheses for grouping).
- **Membership & Range:** `IN (v1, v2, ...)`, `NOT IN (...)`, `BETWEEN low AND high`, `NOT BETWEEN low AND high`.
- **Pattern & Null:** `LIKE`, `NOT LIKE` (`%` matches any sequence, `_` matches any single character), `IS NULL`, `IS NOT NULL`.
- **Vector Distance:** `<->` (L2 / Euclidean) and `<=>` (Cosine) against a `VECTOR(n)` column.

::: info DataVo Extension
Vector distance operators `<->` (L2) and `<=>` (Cosine) can be used directly inside `WHERE` predicates to filter by a maximum distance threshold:

```sql
SELECT Id, Title, Category
FROM documents
WHERE Category = 'guide'
  AND Embedding <=> '[0.12, 0.85, 0.44]' < 0.25;
```

Vector literals must be single-quoted strings (or bound parameters such as `@queryVec`) whose element count matches the column's declared `VECTOR(n)` dimension.
:::

::: tip Clustered Range Seeks
`BETWEEN` and range predicates on a clustered primary key (single `INT` or `GUID` `PRIMARY KEY` in `Lsm` mode) perform an index range seek instead of a full table scan.
:::

## JOIN

DataVo supports `INNER JOIN`, `LEFT [OUTER] JOIN`, `RIGHT [OUTER] JOIN`, `FULL [OUTER] JOIN`, `CROSS JOIN`, self-joins, and multi-table join chains.

### INNER and LEFT JOIN

```sql
SELECT
    p.Name,
    p.Region,
    m.Points,
    m.PlayedOn
FROM players p
INNER JOIN matches m ON m.PlayerId = p.Id
WHERE m.Points >= 50;
```

Use `LEFT JOIN` (or `LEFT OUTER JOIN`) to retain rows from the left table even when no matching row exists on the right (`NULL` fills unmatched right columns):

```sql
SELECT
    p.Id,
    p.Name,
    m.Points
FROM players p
LEFT JOIN matches m ON m.PlayerId = p.Id;
```

### RIGHT, FULL OUTER, and CROSS JOIN

```sql
-- Retain all match rows even if a player record is missing
SELECT p.Name, m.Id AS MatchId, m.Points
FROM players p
RIGHT OUTER JOIN matches m ON m.PlayerId = p.Id;

-- Retain unmatched rows from both sides
SELECT p.Name, m.Id AS MatchId
FROM players p
FULL OUTER JOIN matches m ON m.PlayerId = p.Id;

-- Cartesian product (no ON clause)
SELECT p.Name, d.Title
FROM players p
CROSS JOIN documents d;
```

::: tip Join Fast Paths
`INNER JOIN` and `LEFT JOIN` on integer equality keys execute on DataVo's typed hash-join path across any number of joined tables. `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, self-joins, `VARCHAR`/`GUID` join keys, and cross-table `OR` predicates are fully supported via the general evaluator.
:::

## GROUP BY & HAVING

`GROUP BY` partitions rows sharing identical values across one or more columns so aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) compute one result per group. `HAVING` filters the resulting groups.

```sql
SELECT
    p.Region,
    COUNT(*) AS PlayerCount,
    AVG(p.Score) AS AvgScore,
    MAX(p.Score) AS TopScore
FROM players p
WHERE p.Score > 0
GROUP BY p.Region
HAVING COUNT(*) >= 5 AND AVG(p.Score) >= 600
ORDER BY AvgScore DESC;
```

- `HAVING` can reference aggregates that are not projected in the `SELECT` list.
- `HAVING` is also valid **without** `GROUP BY`; in that case, the filtered table is treated as a single group:

```sql
SELECT COUNT(*) AS ActivePlayers, SUM(Score) AS TotalScore
FROM players
WHERE Score >= 100
HAVING COUNT(*) > 0;
```

## DISTINCT

Add `DISTINCT` immediately after `SELECT` to deduplicate identical output rows.

```sql
SELECT DISTINCT Region
FROM players
ORDER BY Region ASC;
```

For per-aggregate deduplication rather than whole-row deduplication, pass `DISTINCT` inside an aggregate call (for example, `COUNT(DISTINCT Region)`). See [Functions & Operators](/manual/sql/functions) for details.

## ORDER BY

`ORDER BY` sorts the final result set by one or more columns, aliases, or expressions. Each sort key accepts `ASC` (default) or `DESC`.

```sql
SELECT Id, Name, Region, Score
FROM players
ORDER BY Region ASC, Score DESC, Name ASC;
```

### Vector Distance Ranking

Combine `ORDER BY` with `<->` (L2) or `<=>` (Cosine) and `LIMIT` to run k-nearest-neighbor (k-NN) similarity search:

```sql
SELECT
    Id,
    Title,
    Embedding <=> '[0.12, 0.85, 0.44]' AS Distance
FROM documents
WHERE Category = 'reference'
ORDER BY Distance ASC
LIMIT 5;
```

When an `HNSW` or `FLAT` index exists on `Embedding` with a matching distance metric, the planner accelerates `ORDER BY Embedding <=> ... LIMIT k` automatically.

## LIMIT & OFFSET

`LIMIT` restricts how many rows are returned. Combine `LIMIT` with `OFFSET` to paginate through sorted results.

```sql
-- Page 2 (rows 11 through 20)
SELECT Id, Name, Score
FROM players
ORDER BY Score DESC
LIMIT 10 OFFSET 10;
```

::: tip Top-K Execution
Queries of the form `ORDER BY ... LIMIT k` on typed scans and typed joins use a bounded top-k heap rather than sorting the entire table in memory.
:::

## Subqueries

DataVo supports `IN`, `EXISTS`, and `NOT EXISTS` subqueries inside `WHERE` predicates.

### `IN (SELECT ...)`

```sql
SELECT Id, Name, Score
FROM players
WHERE Id IN (
    SELECT PlayerId
    FROM matches
    WHERE Points >= 100
);
```

### `EXISTS` and `NOT EXISTS`

```sql
-- Players who have played at least one match
SELECT p.Id, p.Name
FROM players p
WHERE EXISTS (
    SELECT 1
    FROM matches m
    WHERE m.PlayerId = p.Id
);

-- Players with no recorded matches
SELECT p.Id, p.Name
FROM players p
WHERE NOT EXISTS (
    SELECT 1
    FROM matches m
    WHERE m.PlayerId = p.Id
);
```

## CTEs (`WITH`)

Use `WITH` to define one or more named temporary result sets (Common Table Expressions) that simplify complex queries. Non-recursive CTEs are materialized once before the outer `SELECT` executes and can be joined or aggregated like base tables.

```sql
WITH HighScorers AS (
    SELECT Id, Name, Region, Score
    FROM players
    WHERE Score >= 900
),
MatchTotals AS (
    SELECT PlayerId, SUM(Points) AS TotalPoints
    FROM matches
    GROUP BY PlayerId
)
SELECT
    h.Name,
    h.Region,
    m.TotalPoints
FROM HighScorers h
LEFT JOIN MatchTotals m ON m.PlayerId = h.Id
ORDER BY h.Score DESC;
```

::: info DataVo Extension: `WITH RECURSIVE`
Standard one-shot `SELECT` queries support non-recursive `WITH` CTEs. **`WITH RECURSIVE`** is supported specifically for [Live Reactive Views](/manual/fundamentals/reactive-views) created via `Subscribe` in C#.
:::

## Window Functions

DataVo v0.4 supports the `RANK()` window function to assign competition ranks (`1, 2, 2, 4, ...`) within optional partitions without collapsing rows.

```sql
SELECT
    Name,
    Region,
    Score,
    RANK() OVER (PARTITION BY Region ORDER BY Score DESC) AS RegionRank
FROM players
ORDER BY Region ASC, RegionRank ASC;
```

- `ORDER BY <column> [ASC | DESC]` inside `OVER (...)` is **required** and accepts a single column reference.
- `PARTITION BY <col1> [, <col2>, ...]` is **optional**; omitting it ranks across the entire result set.

::: warning Window Function Scope in v0.4
Only `RANK() OVER ([PARTITION BY ...] ORDER BY <col> [ASC | DESC])` is supported in v0.4. `ROW_NUMBER()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, and window frame clauses (`ROWS BETWEEN ...`) are not implemented and throw a `ParserException`.
:::

## Set Operations

Combine rows from two or more `SELECT` statements with matching column counts using `UNION` or `UNION ALL`.

```sql
-- Deduplicated union
SELECT Id, Name FROM players WHERE Region = 'EU'
UNION
SELECT Id, Name FROM players WHERE Score >= 1200;

-- Preserve all rows including duplicates
SELECT Id, Name FROM players WHERE Region = 'EU'
UNION ALL
SELECT Id, Name FROM players WHERE Region = 'NA';
```

- `UNION` removes duplicate rows across branches.
- `UNION ALL` concatenates all branch rows without deduplication and avoids the hashing cost.

::: warning Unsupported Set Operations
`INTERSECT`, `EXCEPT`, and parenthesized `UNION` sub-branches are not supported in v0.4.
:::

## See Also

- [Functions & Operators](/manual/sql/functions): Aggregates, `RANK()`, `LOWER`/`UPPER`, and vector distance operators
- [EXPLAIN](/manual/sql/statements/explain): Inspecting query plans, physical operators, and fast-path choices
- [Indexes](/manual/sql/statements/indexes): Accelerating `WHERE`, `JOIN`, and vector `ORDER BY` queries
- [Query Planner](/manual/internals/query-planner): Typed fast paths, bounded top-k heaps, and general evaluator fallback rules
