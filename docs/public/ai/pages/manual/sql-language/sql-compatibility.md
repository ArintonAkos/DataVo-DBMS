# SQL Compatibility Matrix

> Source route: /manual/sql-language/sql-compatibility
> Source file: manual/sql-language/sql-compatibility.md

DataVo SQL is intentionally smaller than PostgreSQL, SQLite, or SQL Server. The point of this page is to make the v0.1 boundary explicit: examples first, then the compatibility table.

The core database lifecycle is supported.

```sql
CREATE DATABASE App;
USE App;

CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  DepartmentId INT,
  IsActive BIT
);

CREATE TABLE Departments (
  Id INT PRIMARY KEY,
  Name VARCHAR(80)
);
```

Core DML is supported.

```sql
INSERT INTO Users (Id, Name, DepartmentId, IsActive)
VALUES (1, 'Ada', 10, true);

UPDATE Users
SET IsActive = false
WHERE Id = 1;

DELETE FROM Users
WHERE Id = 1;
```

Core query features are supported.

```sql
SELECT Id, Name
FROM Users
WHERE IsActive = true
ORDER BY Id ASC
LIMIT 50;
```

Joins and grouped aggregates are part of the runtime SQL surface.

```sql
SELECT d.Name AS Department, COUNT(*) AS UserCount
FROM Users u
INNER JOIN Departments d ON u.DepartmentId = d.Id
GROUP BY d.Name
HAVING COUNT(*) > 0;
```

Vector SQL is part of the v0.1 feature set.

```sql
CREATE TABLE Embeddings (
  Id INT PRIMARY KEY,
  Label VARCHAR(80),
  Emb VECTOR(3)
);

CREATE INDEX idx_embeddings_hnsw
ON Embeddings (Emb)
USING HNSW;

SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 5;
```

The unsupported areas are just as important. Do not write application code that depends on stored procedures, triggers, PostgreSQL catalogs, or full PostgreSQL syntax compatibility.

```sql
-- Not supported in DataVo v0.1:
CREATE TRIGGER users_updated
BEFORE UPDATE ON Users
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at();
```

## SQL Compatibility Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Database selection | Supported | `CREATE DATABASE`, `USE`. |
| Table DDL | Supported | `CREATE TABLE`, `DROP TABLE`, and limited `ALTER TABLE` forms. |
| Scalar indexes | Supported | Single-column and composite scalar indexes. |
| Vector indexes | Supported | HNSW and FLAT. |
| Inserts, updates, deletes | Supported | Core DML works through the parser/runtime. |
| Transactions | Supported | Explicit session transactions with `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK`. |
| Basic SELECT | Supported | Projection, filtering, ordering, limits, and offsets. |
| Joins | Supported | Runtime SQL supports major join families. |
| Aggregation | Supported | Grouped aggregate queries are supported. |
| Subqueries | Supported | Tested `IN` and `EXISTS` style shapes. |
| Vector distance SQL | Supported | `<=>`, `<->`, HNSW, and FLAT. |
| Views | Planned | Reactive query work exists, but SQL `CREATE VIEW` is not a public v0.1 feature. |
| Window functions | Not Supported | No public v0.1 support. |
| Stored procedures | Not Supported | No procedural SQL layer. |
| Triggers | Not Supported | No trigger execution layer. |
| User-defined functions | Not Supported | No public UDF API. |
| PostgreSQL catalogs | Not Supported | Future wire compatibility would need catalog mocking. |
