# Queries

> Source route: /manual/sql-language/queries
> Source file: manual/sql-language/queries.md

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

| Feature | Status | Notes |
| --- | --- | --- |
| Projection and aliases | Supported | Select named columns and computed aliases. |
| `WHERE` predicates | Supported | Includes equality, comparisons, `IN`, `BETWEEN`, `LIKE`, and vector distance predicates. |
| `ORDER BY` | Supported | Supports ascending/descending ordering and vector ranking. |
| `LIMIT` / `OFFSET` | Supported | Use for top-K and paging shapes. |
| Aggregates | Supported | Includes `COUNT`, `SUM`, `MIN`, and `MAX` in documented paths. |
| `GROUP BY` / `HAVING` | Supported | Runtime query path supports grouped aggregates. |
| Joins | Supported | `INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS`. |
| Subqueries | Supported | Includes `IN` and `EXISTS` style shapes in tested paths. |
| `UNION` / `UNION ALL` | Supported | Branch projections must be compatible. |
| Vector distance ranking | Supported | Use `<=>` for cosine and `<->` for L2. |
| Cost-based optimizer parity with mature RDBMSs | Not Supported | Planner work is early and controlled by configuration. |
