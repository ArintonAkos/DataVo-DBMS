---
title: Functions & Operators
description: Built-in aggregate, window, and scalar functions plus relational, arithmetic, and vector distance operators in DataVo v0.4.
---

# Functions & Operators

DataVo v0.4 ships a focused set of SQL aggregates, one window function (`RANK()`), two string scalar functions, standard comparison/logical/arithmetic operators, and native vector distance operators.

```sql
SELECT
    UPPER(Region) AS RegionCode,
    COUNT(*) AS Players,
    AVG(Score) AS AvgScore,
    RANK() OVER (PARTITION BY Region ORDER BY Score DESC) AS RegionRank
FROM players
WHERE Score BETWEEN 100 AND 1000 AND Name LIKE 'A%';
```

## Aggregate Functions

| Function | Return / Behavior | Notes |
| --- | --- | --- |
| `COUNT(*)` / `COUNT(col)` | `BIGINT` (`long`) | `COUNT(*)` counts all rows; `COUNT(col)` ignores `NULL`s. |
| `COUNT(DISTINCT col)` | `BIGINT` (`long`) | Counts distinct non-`NULL` values (general evaluator). `COUNT(DISTINCT *)` is a parse error. |
| `SUM([DISTINCT] col)` | Numeric sum | Ignores `NULL` values. |
| `AVG([DISTINCT] col)` | Floating-point average | Ignores `NULL` values. |
| `MIN([DISTINCT] col)` | Minimum value | Works on numeric, text, and date columns. |
| `MAX([DISTINCT] col)` | Maximum value | Works on numeric, text, and date columns. |

`HAVING` can evaluate aggregates that are not present in the `SELECT` list, and works both with and without `GROUP BY`.

## Window Functions

DataVo v0.4 supports a single window function:

```sql
RANK() OVER ([PARTITION BY <col>, ...] ORDER BY <col> [ASC | DESC], ...)
```

- `ORDER BY` inside `OVER (...)` is required; `PARTITION BY` is optional.
- **Not implemented in v0.4:** `ROW_NUMBER()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, and window frame clauses (`ROWS BETWEEN ...`) throw a `ParserException`.

## Scalar Functions

| Function | Signature | Behavior |
| --- | --- | --- |
| `LOWER` | `LOWER(expr)` | Converts text to lowercase. |
| `UPPER` | `UPPER(expr)` | Converts text to uppercase. |

::: warning Not Supported in v0.4
DataVo core SQL does **not** include `CASE WHEN`, `COALESCE`, `CAST`, `LEN`/`LENGTH`, `SUBSTRING`, `TRIM`, date/time functions, math functions, or SQL `COSINE_DISTANCE()`/`L2_DISTANCE()` scalar functions (the latter exist only as EF Core preview shims in `DataVoVectorDbFunctions`).
:::

## Operators

| Category | Operators | Notes |
| --- | --- | --- |
| Comparison | `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=` | Text comparisons are ordinal (UTF-8 code-point order). |
| Logical | `AND`, `OR`, `NOT` | Standard three-valued boolean logic. |
| Membership & Range | `IN (v1, v2, ...)`, `IN (SELECT ...)`, `BETWEEN a AND b` | `BETWEEN` on a clustered primary key performs a key-range seek. |
| Pattern & Null | `LIKE`, `NOT LIKE`, `IS NULL`, `IS NOT NULL` | `%` matches any sequence; `_` matches a single character. |
| Arithmetic | `+`, `-`, `*`, `/` | Supported in `SELECT`, `WHERE`, and `UPDATE SET` expressions. |
| Vector Distance | `<->` (L2 / Euclidean), `<=>` (Cosine) | Lower distance means closer. Valid in `SELECT`, `WHERE`, and `ORDER BY`. |

::: info Vector Operator Scope
Only `<->` (L2 / Euclidean) and `<=>` (Cosine) are supported in `v0.4`. There is **no** `<#>` inner-product operator.
:::

## See Also
- [SELECT & EXPLAIN](/manual/sql/statements/select) — Combining operators, aggregates, `GROUP BY`, `HAVING`, and `ORDER BY`
- [Vector Search](/manual/fundamentals/vector-search) — Indexing `<->` and `<=>` queries with `HNSW` and `FLAT`
- [SQL Compatibility](/manual/sql/compatibility) — Full dialect feature matrix
