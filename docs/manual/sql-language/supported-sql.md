# Supported SQL

DataVo SQL is a compact embedded-engine dialect. It is familiar if you know SQL, but it is not a PostgreSQL, SQLite, or SQL Server compatibility layer. This page documents the supported SQL syntax.

Start by creating and selecting a database.

```sql
CREATE DATABASE App;
USE App;
```

Create tables with primary keys, scalar columns, and vector columns.

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

Insert rows with either a full-row form or a column-targeted form. Prefer the column-targeted form in application code because it is easier to read and safer to evolve.

```sql
INSERT INTO Users VALUES (1, 'Ada', 10, 98.5, true);

INSERT INTO Users (Id, Name, DepartmentId, Score, IsActive)
VALUES (2, 'Grace', 10, 95.0, true);
```

Query rows with projection, filtering, ordering, and limits.

```sql
SELECT Id, Name, Score
FROM Users
WHERE IsActive = true AND Score > 90
ORDER BY Score DESC
LIMIT 10;
```

Update and delete rows with predicates.

```sql
UPDATE Users
SET Score = 99.0
WHERE Id = 1;

DELETE FROM Users
WHERE Id = 2;
```

Create scalar indexes for lookup and ordering paths.

```sql
CREATE INDEX ix_users_name ON Users (Name);
CREATE INDEX ix_users_active_score ON Users (IsActive, Score);
```

Create vector indexes when the table stores embeddings.

```sql
CREATE INDEX idx_embeddings_hnsw
ON Embeddings (Emb)
USING HNSW;

CREATE INDEX idx_embeddings_flat
ON Embeddings (Emb)
USING FLAT;
```

Use joins when data is split across tables.

```sql
SELECT u.Name, d.Name AS Department
FROM Users u
INNER JOIN Departments d ON u.DepartmentId = d.Id;
```

Use grouped aggregates for summary queries.

```sql
SELECT IsActive, COUNT(*) AS UserCount, MAX(Score) AS HighestScore
FROM Users
GROUP BY IsActive
HAVING COUNT(*) > 0;
```

Use `UNION` or `UNION ALL` when compatible SELECT branches should be combined.

```sql
SELECT Name FROM Users WHERE IsActive = true
UNION ALL
SELECT Name FROM ArchivedUsers WHERE IsActive = true;
```

Compact table storage after deletes with `VACUUM`.

```sql
VACUUM Users;
```

## SQL Support Summary

| Feature | Status | Notes |
| --- | --- | --- |
| `CREATE DATABASE` / `USE` | Supported | Selects the active database for the session. |
| `CREATE TABLE` / `DROP TABLE` | Supported | Includes primary keys, defaults, scalar columns, and vector columns. |
| `ALTER TABLE ADD/DROP/MODIFY COLUMN` | Supported | Limited implementation; key/indexed columns have restrictions. |
| Scalar indexes | Supported | Single-column and composite scalar indexes are documented. |
| Vector indexes | Supported | `USING HNSW` and `USING FLAT`. |
| `INSERT`, `UPDATE`, `DELETE`, `VACUUM` | Supported | Core DML is available in the public subset. |
| `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET` | Supported | Includes common predicate and ordering shapes. |
| Aggregates, `GROUP BY`, `HAVING` | Supported | Used by runtime query paths. |
| Joins | Supported | `INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS`. |
| `UNION` / `UNION ALL` | Supported | Branches must project compatible columns. |
| Window functions | Not Supported | Not part of the v0.1 public claim. |
| Stored procedures and triggers | Not Supported | No procedural SQL layer in v0.1. |
| PostgreSQL dialect compatibility | Not Supported | DataVo SQL is its own dialect. |
