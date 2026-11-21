# SQL Compatibility Matrix

> Source route: /manual/sql-language/sql-compatibility
> Source file: manual/sql-language/sql-compatibility.md

DataVo SQL is its own dialect, smaller than PostgreSQL, SQLite or SQL Server. This page states which constructs it accepts, and, because performance depends on it, which of them run on a typed execution path and which on the general evaluator.

The distinction matters. Every construct listed as supported returns the correct result on both active storage engines. But a query that a typed path accepts reads cells directly from stored rows and allocates little, while a query on the general path builds a dictionary for every row it reads, which costs kilobytes per row and can make a million-row query take seconds instead of milliseconds. [Query Planner And Fast Paths](/manual/storage-engine/query-planner-fast-paths) explains the typed paths.

The core lifecycle:

```sql
CREATE DATABASE App;
USE App;

CREATE TABLE Departments (Id INT PRIMARY KEY, Name VARCHAR(80));
CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(80), DepartmentId INT, IsActive BIT);

INSERT INTO Users (Id, Name, DepartmentId, IsActive) VALUES (1, 'Ada', 10, true);
UPDATE Users SET IsActive = false WHERE Id = 1;
DELETE FROM Users WHERE Id = 1;
```

Joins, grouping and filters, including `HAVING` on an aggregate without `GROUP BY`:

```sql
SELECT d.Name AS Department, COUNT(*) AS UserCount
FROM Users u
INNER JOIN Departments d ON u.DepartmentId = d.Id
WHERE u.IsActive = true OR u.Name LIKE 'A%'
GROUP BY d.Name
HAVING COUNT(*) > 0;

SELECT COUNT(*) FROM Users WHERE IsActive = true HAVING COUNT(*) > 10;
```

Vector search is ordinary SQL:

```sql
CREATE TABLE Embeddings (Id INT PRIMARY KEY, Label VARCHAR(80), Emb VECTOR(3));
CREATE INDEX idx_embeddings_hnsw ON Embeddings (Emb) USING HNSW WITH (m = 16, ef_search = 64);

SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
WHERE Label LIKE 'warm%'
ORDER BY distance ASC
LIMIT 5;
```

## Statements and clauses

| Feature | Status | Notes |
| --- | --- | --- |
| `CREATE DATABASE`, `USE`, `DROP` | Supported | |
| `CREATE TABLE`, `DROP TABLE` | Supported | Primary keys, defaults, scalar and vector columns. See [CREATE TABLE](/manual/sql-language/create-table). |
| `ALTER TABLE` | Limited | `ADD COLUMN`; `DROP`/`MODIFY COLUMN` reject key, indexed, referenced or incompatible changes. |
| `CREATE INDEX` | Supported | B-tree (single and composite), `USING HNSW`, `USING FLAT`, `USING COLUMNAR`; `WITH (metric, m, ef_construction, ef_search, enable_diversity_heuristic)` for vector indexes. |
| `INSERT`, `UPDATE`, `DELETE`, `VACUUM` | Supported | See [Data Manipulation](/manual/sql-language/data-manipulation). |
| Transactions | Supported | `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`; snapshot isolation, not serializable. |
| `SELECT` projection, aliases, `DISTINCT` | Supported | `SELECT DISTINCT` runs on the general path. |
| `WHERE` with `AND`, `OR`, `NOT`, comparisons, `IS [NOT] NULL`, `LIKE`, `NOT LIKE` | Supported | Typed path on one table and within one table of a join. |
| `IN (list)`, `BETWEEN` | Supported | A `BETWEEN` on a clustered primary key is a key-range seek. |
| `ORDER BY`, `LIMIT`, `OFFSET` | Supported | `ORDER BY ... LIMIT k` keeps a bounded top-k on the typed scan and typed join. |
| Aggregates `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` | Supported | Including `DISTINCT` inside the parentheses (general path). `COUNT(DISTINCT *)` is a parse error. |
| `GROUP BY`, `HAVING` | Supported | `HAVING` without `GROUP BY` applies to the single group (v0.4). |
| `INNER JOIN`, `LEFT JOIN` | Supported | Typed hash join on integer equality keys, any number of tables. |
| `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN` | Supported | General path. |
| Joins on text or `GUID` keys, `OR` across two joined tables, self-joins | Supported | General path. |
| Subqueries | Supported | `IN (SELECT ...)`, `EXISTS`, `NOT EXISTS`. |
| Common table expressions | Supported | Non-recursive `WITH name AS (SELECT ...)`, materialized before the main query. `WITH RECURSIVE` is available to reactive queries only. |
| `UNION`, `UNION ALL` | Supported | |
| `INTERSECT`, `EXCEPT` | Not supported | |
| Window functions | Limited | `RANK() OVER (PARTITION BY ... ORDER BY ...)` only. No `ROW_NUMBER`, frames, or other window functions. |
| Scalar functions | Limited | `UPPER` and `LOWER`. No `LEN`, `CASE`, `CAST`, `COALESCE` or date functions. |
| Vector distance | Supported | `<=>` (cosine), `<->` (Euclidean) in projections, `WHERE` and `ORDER BY`. |
| Views (`CREATE VIEW`) | Not supported | [Live Reactive Views (Z-set IVM)](/manual/client-interfaces/reactive-views) (`Subscribe` / `SubscribeZeroAlloc`) cover continuous queries from C#. |
| Stored procedures, triggers, user-defined functions | Not supported | |
| PostgreSQL system catalogs (`pg_class`, ...) | Not supported | |
| Security commands | Supported | `CREATE USER`, `CREATE ROLE`, `GRANT`, `REVOKE`, `LOGIN`, `LOGOUT`, `SHOW USERS/ROLES/GRANTS`. Not a PostgreSQL role system. |

## Types

| Type | Notes |
| --- | --- |
| `INT`, `BIGINT` | 32- and 64-bit integers. A single `INT` primary key is clustered on LSM. |
| `FLOAT`, `DOUBLE` | `FLOAT` is **32-bit** in DataVo (SQLite's `REAL` and SQL Server's `FLOAT` are 64-bit); use `DOUBLE` for 64-bit values. |
| `BIT` | Boolean. Write values and comparisons as `TRUE`/`FALSE` (any case); `1`/`0` and `'true'`/`'false'` are also read as booleans. |
| `DATE` | Materializes as `DataVoDate`. |
| `VARCHAR(n)` | Stored as UTF-8. Comparisons and ordering are ordinal (by character code), not collation-aware or case-insensitive. |
| `GUID` (`UUID`, `UNIQUEIDENTIFIER`) | A single `GUID` primary key is clustered on LSM. |
| `VECTOR(n)` (`VEC2`, `VEC3`, `VEC4`, `FLOAT[n]`) | Fixed dimension per column, 32-bit floats. `VECTOR` is a keyword, so a column cannot be named `Vector`. |
