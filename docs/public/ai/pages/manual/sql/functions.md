# Functions & Operators

> Source route: /manual/sql/functions
> Source file: manual/sql/functions.md

DataVo v0.5 provides a focused set of SQL aggregate functions, the `RANK()` window function, `LOWER`/`UPPER` string scalar functions, standard relational and arithmetic operators, and native vector distance operators (`<->`, `<=>`).

```sql
SELECT
    UPPER(Region) AS RegionCode,
    COUNT(*) AS Players,
    AVG(Score) AS AvgScore,
    MAX(Score) AS TopScore
FROM players
WHERE Score BETWEEN 100 AND 2000
  AND Name LIKE 'A%'
GROUP BY Region
HAVING COUNT(*) >= 2
ORDER BY AvgScore DESC;
```

## Aggregate Functions

Aggregate functions compute a single summary value over each `GROUP BY` group, or over the entire filtered table when `GROUP BY` is omitted. Every aggregate ignores `NULL` inputs except `COUNT(*)`.

| Function | Syntax | Input Types | Return Type | `NULL` / Empty Group Behavior |
| --- | --- | --- | --- | --- |
| `COUNT` | `COUNT(*)`, `COUNT([DISTINCT] expr)` | Any (`*` or column/expr) | `BIGINT` (`long`) | `COUNT(*)` counts all rows including `NULL`s; `COUNT(expr)` ignores `NULL`s. Returns `0` for empty input. |
| `SUM` | `SUM([DISTINCT] expr)` | Numeric (`INT`, `BIGINT`, `FLOAT`, `DOUBLE`) | Numeric (`BIGINT` for integers, `DOUBLE` for floats) | Ignores `NULL`s. Returns `NULL` when the group is empty or all values are `NULL`. |
| `AVG` | `AVG([DISTINCT] expr)` | Numeric (`INT`, `BIGINT`, `FLOAT`, `DOUBLE`) | `DOUBLE` (`double`) | Ignores `NULL`s. Returns `NULL` when the group is empty or all values are `NULL`. |
| `MIN` | `MIN([DISTINCT] expr)` | Numeric, `VARCHAR`, `DATE`, `DATETIME` | Matches column type | Ignores `NULL`s. Returns `NULL` when the group is empty or all values are `NULL`. |
| `MAX` | `MAX([DISTINCT] expr)` | Numeric, `VARCHAR`, `DATE`, `DATETIME` | Matches column type | Ignores `NULL`s. Returns `NULL` when the group is empty or all values are `NULL`. |

### `COUNT`

Counts rows or non-`NULL` values. Pass `DISTINCT` to count unique non-`NULL` values (`COUNT(DISTINCT *)` is a syntax error).

```sql
SELECT
    COUNT(*) AS TotalPlayers,
    COUNT(Region) AS PlayersWithRegion,
    COUNT(DISTINCT Region) AS ActiveRegions
FROM players;
```

### `SUM` & `AVG`

Compute the sum and arithmetic mean of numeric columns. Passing a non-numeric column raises an evaluation error.

```sql
SELECT
    PlayerId,
    SUM(Points) AS TotalPoints,
    AVG(Points) AS AvgPoints
FROM matches
GROUP BY PlayerId
HAVING SUM(Points) >= 200;
```

### `MIN` & `MAX`

Return the smallest or largest non-`NULL` value across numeric, `VARCHAR` (ordinal UTF-8 order), `DATE`, or `DATETIME` columns while preserving the input column's type.

```sql
SELECT
    PlayerId,
    MIN(PlayedOn) AS FirstMatch,
    MAX(PlayedOn) AS LatestMatch,
    MAX(Points) AS BestMatchPoints
FROM matches
GROUP BY PlayerId;
```

::: tip `HAVING` Without `GROUP BY`
`HAVING` can evaluate aggregates that do not appear in the `SELECT` list, and can be used without `GROUP BY` to filter a whole-table aggregation as a single group.
:::

## Window Functions

DataVo v0.5 implements a single window function: `RANK()`. Unlike aggregates, `RANK()` preserves every input row and computes a `BIGINT` (`long`) rank over each partition.

### Syntax

```sql
RANK() OVER ([PARTITION BY col1 [, col2, ...]] ORDER BY sort_col [ASC | DESC])
```

### Parameters

| Parameter | Required | Description |
| --- | --- | --- |
| `PARTITION BY col1, ...` | Optional | Splits rows into independent ranking groups. When omitted, all rows form a single partition. `NULL` partition keys group together. |
| `ORDER BY sort_col [ASC \| DESC]` | Required | Single column reference that determines rank order (`ASC` by default). |

### Behavior & Example

- Returns a 1-based `BIGINT` (`long`) using standard competition ranking: equal sort values receive the same rank, and the next distinct value skips ahead (`1, 2, 2, 4`).

```sql
SELECT
    Id,
    Name,
    Region,
    Score,
    RANK() OVER (PARTITION BY Region ORDER BY Score DESC) AS RegionRank
FROM players
ORDER BY Region ASC, RegionRank ASC;
```

::: warning Supported Window Functions in v0.5
Only `RANK() OVER ([PARTITION BY ...] ORDER BY <col> [ASC | DESC])` is supported in v0.5. `ROW_NUMBER()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, and window frame clauses (`ROWS BETWEEN ...`) are not implemented and throw a `ParserException`.
:::

## Scalar Functions

DataVo v0.5 supports two single-argument string case-conversion scalar functions in `SELECT`, `WHERE`, and `ORDER BY` expressions:

| Function | Syntax | Input | Return Type | `NULL` Behavior |
| --- | --- | --- | --- | --- |
| `LOWER` | `LOWER(expr)` | Text or convertible scalar | `VARCHAR` (`string`) | Returns `NULL` when `expr` evaluates to `NULL`. |
| `UPPER` | `UPPER(expr)` | Text or convertible scalar | `VARCHAR` (`string`) | Returns `NULL` when `expr` evaluates to `NULL`. |

Both functions convert strings using invariant-culture casing and require exactly one argument.

```sql
SELECT
    Id,
    LOWER(Name) AS NormalizedName,
    UPPER(Region) AS RegionCode
FROM players
WHERE LOWER(Region) = 'eu'
ORDER BY NormalizedName ASC;
```

::: warning Not Evaluated in `UPDATE SET`
`LOWER(...)` and `UPPER(...)` are supported in query (`SELECT`, `WHERE`, `ORDER BY`) expressions, but `UPDATE ... SET` does **not** evaluate scalar functions. Normalize strings in application code before binding parameters to `UPDATE`.
:::

## Operators

### Arithmetic Operators

| Operator | Meaning | Operands & Behavior |
| --- | --- | --- |
| `+` | Addition / String concatenation | Adds numeric operands; concatenates when either operand is a string (`Name + ' (EU)'`). |
| `-` | Subtraction | Subtracts numeric operands. |
| `*` | Multiplication | Multiplies numeric operands. |
| `/` | Division | Divides numeric operands. Dividing by `0` throws a `DivisionByZero` evaluation error. |

- **NULL propagation:** If either operand is `NULL`, all arithmetic operators evaluate to `NULL`.
- **Supported contexts:** Valid in `SELECT`, `WHERE`, `ORDER BY`, and `UPDATE SET` expressions (not inside `INSERT ... VALUES`).

```sql
SELECT
    Name + ' [' + Region + ']' AS Label,
    Score * 2 AS DoubleScore,
    (Score + 100) / 10 AS Tier
FROM players
WHERE Score - 50 >= 500;
```

### Comparison Operators

| Operator | Meaning | Notes |
| --- | --- | --- |
| `=` | Equal | Compares values across compatible numeric widths; `VARCHAR` uses ordinal UTF-8 comparison. |
| `!=`, `<>` | Not equal | Both spellings are accepted and equivalent. |
| `<` | Less than | Valid on numeric, `VARCHAR` (ordinal), `DATE`, and `DATETIME` values. |
| `<=` | Less than or equal | Valid on numeric, `VARCHAR`, `DATE`, and `DATETIME` values. |
| `>` | Greater than | Valid on numeric, `VARCHAR`, `DATE`, and `DATETIME` values. |
| `>=` | Greater than or equal | Valid on numeric, `VARCHAR`, `DATE`, and `DATETIME` values. |

Any comparison where either side is `NULL` evaluates to unknown (`NULL`), which `WHERE` and `HAVING` filters treat as false. Use `IS NULL` / `IS NOT NULL` to test for nulls.

### Logical Operators

| Operator | Syntax | Description |
| --- | --- | --- |
| `AND` | `cond1 AND cond2` | True when both conditions are true. |
| `OR` | `cond1 OR cond2` | True when either condition is true. |
| `NOT` | `NOT cond` | Negates a condition (`NOT`, `NOT LIKE`, `NOT IN`, `NOT BETWEEN`, `NOT EXISTS`). |

Use parentheses `(...)` to control precedence when mixing `AND` and `OR`.

### Membership, Range, Pattern & Null Operators

| Operator | Syntax | Description |
| --- | --- | --- |
| `IN` / `NOT IN` | `expr [NOT] IN (v1, v2, ...)` or `expr IN (SELECT ...)` | Tests membership against a literal list or a single-column subquery. |
| `BETWEEN` / `NOT BETWEEN` | `expr [NOT] BETWEEN low AND high` | Inclusive range test (`expr >= low AND expr <= high`). Performs a key-range seek on clustered primary keys. |
| `LIKE` / `NOT LIKE` | `expr [NOT] LIKE pattern` | Wildcard pattern matching where `%` matches zero or more characters and `_` matches any single character. |
| `IS NULL` / `IS NOT NULL` | `expr IS [NOT] NULL` | Tests whether an expression evaluates to `NULL`. |

```sql
SELECT Id, Name, Region, Score
FROM players
WHERE Region IN ('EU', 'NA')
  AND Score BETWEEN 500 AND 1200
  AND Name LIKE 'A%'
  AND Region IS NOT NULL;
```

### Vector Distance Operators

::: info DataVo Extension
DataVo extends SQL with two native distance operators for `VECTOR(n)` columns (`float[]`). Both return a `DOUBLE` (`double`) distance where **smaller values mean closer / more similar vectors**.
:::

| Operator | Metric | Formula / Range | Indexed By |
| --- | --- | --- | --- |
| `<->` | L2 / Euclidean distance | `sqrt(sum((a[i] - b[i])^2))`, range `[0, +∞)` | `HNSW`, `FLAT` |
| `<=>` | Cosine distance | `1 - (a · b) / (‖a‖ · ‖b‖)`, range `[0, 2]` | `HNSW`, `FLAT` |

- **Operands:** Each side must be a `VECTOR(n)` column, a single-quoted float-array literal (e.g., `'[0.12, 0.85, 0.44]'`), or a bound vector parameter (`@queryVec`). Both vectors must have the exact same dimension or an evaluation error is raised.
- **Supported clauses:** Valid in `SELECT` projections, `WHERE` distance thresholds, and `ORDER BY ... LIMIT k` nearest-neighbor ranking.

```sql
-- Top-5 nearest documents by Cosine distance with metadata filter
SELECT
    Id,
    Title,
    Category,
    Embedding <=> '[0.12, 0.85, 0.44]' AS CosineDist
FROM documents
WHERE Category = 'guide'
  AND Embedding <=> '[0.12, 0.85, 0.44]' < 0.35
ORDER BY CosineDist ASC
LIMIT 5;

-- L2 / Euclidean distance ranking
SELECT
    Id,
    Title,
    Embedding <-> @queryVec AS L2Dist
FROM documents
ORDER BY L2Dist ASC
LIMIT 10;
```

::: warning No `<#>` Operator or Core SQL Distance Functions
DataVo v0.5 supports **only** `<->` (L2) and `<=>` (Cosine). There is **no** `<#>` inner-product operator, and there are **no** core SQL `COSINE_DISTANCE()` or `L2_DISTANCE()` scalar functions; always use the infix `<=>` and `<->` operators in SQL queries.
:::

## Excluded Functions & Expressions in v0.5

::: info Intentionally Excluded in DataVo v0.5
To keep the embedded parser and execution engine compact and predictable, the following SQL functions and constructs are **not** included in DataVo v0.5 core SQL:

- **Conditional & null helpers:** `CASE WHEN ... THEN ... ELSE ... END`, `COALESCE`, `NULLIF`, `IFNULL`, `IIF`
- **Type casting & pragmas:** `CAST(... AS ...)`, `CONVERT`, `::type`, `PRAGMA`
- **String functions:** `LENGTH` / `LEN`, `SUBSTRING` / `SUBSTR`, `TRIM` / `LTRIM` / `RTRIM`, `REPLACE`, `CONCAT`, `INSTR`, `LEFT`, `RIGHT`
- **Date & time functions:** `NOW()`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `DATEADD`, `DATEDIFF`, `STRFTIME`, `EXTRACT`
- **Math functions:** `ABS`, `ROUND`, `FLOOR`, `CEIL` / `CEILING`, `POWER`, `SQRT`, `MOD`
- **Vector helper functions & inner product:** `COSINE_DISTANCE()`, `L2_DISTANCE()`, and the `<#>` operator
- **Additional window functions:** `ROW_NUMBER()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`

Perform formatting, date arithmetic, conditional projections, or type conversions in C# on query results or bound parameters.
:::

## See Also

- [SELECT](/manual/sql/statements/select): Using functions and operators in `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, and `ORDER BY`
- [Data Types](/manual/sql/data-types): Numeric widths, `DATE` representation, `VARCHAR` ordinal rules, and `VECTOR(n)`
- [Vector Search](/manual/fundamentals/vector-search): Creating `HNSW` and `FLAT` indexes for `<->` and `<=>` queries
- [SQL Compatibility](/manual/sql/compatibility): Side-by-side dialect matrix against SQLite and PostgreSQL
