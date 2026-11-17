# Queries

DataVo queries use familiar SQL clauses: choose columns, choose a table, filter rows, sort the result, and limit how much comes back.

The examples on this page use the following tables.

```sql
CREATE TABLE Departments (
  Id INT PRIMARY KEY,
  Name VARCHAR(80)
);

CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  DepartmentId INT,
  Score FLOAT,
  IsActive BIT
);

CREATE TABLE ArchivedUsers (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  IsActive BIT
);

CREATE TABLE Embeddings (
  Id INT PRIMARY KEY,
  Label VARCHAR(80),
  Emb VECTOR(3)
);
```

The smallest useful query selects a few columns from a table.

```sql
SELECT Id, Name
FROM Users;
```

Use `WHERE` to filter rows.

```sql
SELECT Id, Name, Score
FROM Users
WHERE Score > 90;
```

Combine predicates when the application needs a narrower result set.

```sql
SELECT Id, Name, Score
FROM Users
WHERE IsActive = true AND Score BETWEEN 90 AND 100;
```

Use `LIKE` for simple string matching.

```sql
SELECT Id, Name
FROM Users
WHERE Name LIKE 'A%';
```

Use `IN` when a column can match one of several values.

```sql
SELECT Id, Name
FROM Users
WHERE Id IN (1, 3, 5);
```

Sort and page results with `ORDER BY`, `LIMIT`, and `OFFSET`.

```sql
SELECT Id, Name, Score
FROM Users
ORDER BY Score DESC, Id ASC
LIMIT 10 OFFSET 0;
```

Aggregate rows with `COUNT`, `SUM`, `MIN`, and `MAX`.

```sql
SELECT IsActive, COUNT(*) AS UserCount, MAX(Score) AS HighestScore
FROM Users
GROUP BY IsActive
HAVING COUNT(*) > 0;
```

`HAVING` also works without `GROUP BY`. The whole filtered table is then one group, and `HAVING` decides whether its single result row is returned.

```sql
SELECT COUNT(*) AS ActiveCount, AVG(Score) AS AverageScore
FROM Users
WHERE IsActive = true
HAVING COUNT(*) >= 10;
```

Join tables when a query needs fields from both sides.

```sql
SELECT u.Name, d.Name AS Department
FROM Users u
INNER JOIN Departments d ON u.DepartmentId = d.Id;
```

Use `LEFT JOIN` when unmatched left rows should remain in the result.

```sql
SELECT u.Name, d.Name AS Department
FROM Users u
LEFT JOIN Departments d ON u.DepartmentId = d.Id;
```

Use subqueries for membership checks.

```sql
SELECT Id, Name
FROM Users
WHERE DepartmentId IN (
  SELECT Id
  FROM Departments
  WHERE Name LIKE 'Eng%'
);
```

Name an intermediate result with a common table expression. DataVo materializes it before running the main query; recursive CTEs are available to reactive queries only.

```sql
WITH HighScorers AS (
  SELECT Id, Name, DepartmentId FROM Users WHERE Score >= 90
)
SELECT d.Name, COUNT(*) AS N
FROM HighScorers h
INNER JOIN Departments d ON h.DepartmentId = d.Id
GROUP BY d.Name;
```

Rank rows within a partition with `RANK()`, the one supported window function.

```sql
SELECT Name, DepartmentId, RANK() OVER (PARTITION BY DepartmentId ORDER BY Score DESC) AS ScoreRank
FROM Users;
```

Combine compatible result sets with `UNION ALL`.

```sql
SELECT Name FROM Users
WHERE IsActive = true
UNION ALL
SELECT Name FROM ArchivedUsers
WHERE IsActive = true;
```

Rank vectors with distance expressions. Lower distances sort first.

```sql
SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 5;
```

## Query Support Summary

Supported: projection and aliases; `WHERE` with `AND`, `OR`, `NOT`, comparisons, `IN`, `BETWEEN`, `LIKE`/`NOT LIKE`, `IS [NOT] NULL` and vector-distance predicates; `ORDER BY` (including vector ranking), `LIMIT` and `OFFSET`; `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, with `DISTINCT` inside the parentheses; `GROUP BY` and `HAVING`, including `HAVING` without `GROUP BY`; `INNER`, `LEFT`, `RIGHT`, `FULL` and `CROSS` joins; `IN` and `EXISTS` subqueries; non-recursive common table expressions; `RANK() OVER (PARTITION BY ... ORDER BY ...)`; `UNION` and `UNION ALL`; and vector distance (`<=>` cosine, `<->` Euclidean).

All of these return correct results, but not all are equally fast. Single-table filters and aggregates, and `INNER`/`LEFT` joins on integer keys, run on DataVo's typed paths; `DISTINCT`, `RIGHT`/`FULL`/`CROSS` joins, joins on text or `GUID` keys, an `OR` across two joined tables, CTEs, window functions and queries inside an explicit transaction run on the general evaluator, which is much slower on large tables. See [Query Planner And Fast Paths](/manual/storage-engine/query-planner-fast-paths) and the [SQL compatibility matrix](/manual/sql-language/sql-compatibility).
