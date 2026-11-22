# SQL Compatibility

> Source route: /manual/sql/compatibility
> Source file: manual/sql/compatibility.md

DataVo SQL is a compact embedded-engine dialect designed for predictable, zero-allocation execution in .NET. It covers standard relational CRUD, joins, aggregates, transactions, and vector search without emulating PostgreSQL or SQLite system catalogs.

```sql
CREATE TABLE documents (
    id INT PRIMARY KEY,
    title VARCHAR(200),
    category VARCHAR(80),
    embedding VECTOR(384)
);

SELECT id, title, embedding <=> '[0.1, 0.2, ...]' AS distance
FROM documents
WHERE category = 'guide'
ORDER BY distance ASC
LIMIT 5;
```

## Dialect Matrix (DataVo v0.4 vs. SQLite & PostgreSQL)

| Feature | DataVo v0.4 | SQLite / PostgreSQL Difference |
| --- | --- | --- |
| `CREATE` / `DROP` (`DATABASE`, `TABLE`, `INDEX`) | Supported | Uses `DROP INDEX name ON table`; supports `USING BTREE`, `HNSW`, `FLAT`, `COLUMNAR`. |
| `ALTER TABLE` | Limited | Supports `ADD COLUMN`, `DROP COLUMN`, `MODIFY COLUMN`; rejects key, FK, or indexed column changes. |
| `INSERT`, `UPDATE`, `DELETE`, `VACUUM` | Supported | No `MERGE`, `UPSERT` (`ON CONFLICT`), `RETURNING`, or PostgreSQL `COPY`. |
| Transactions & Savepoints | Supported | Snapshot isolation via `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO`, `RELEASE`; not serializable. |
| `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET` | Supported | `SELECT DISTINCT` runs on the general evaluator; `ORDER BY ... LIMIT k` uses bounded top-k on typed paths. |
| Joins (`INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`) | Supported | `INNER`/`LEFT` on integer keys use typed hash joins; `RIGHT`/`FULL`/`CROSS` and text/`GUID` keys use the general path. |
| Aggregates, `GROUP BY`, `HAVING` | Supported | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` (including `DISTINCT`); `HAVING` works with or without `GROUP BY`. |
| Subqueries & CTEs | Supported | `IN (SELECT ...)`, `EXISTS`, `NOT EXISTS`; non-recursive `WITH` CTEs (`WITH RECURSIVE` is reactive-only). |
| Set Operations | Partial | `UNION` and `UNION ALL` supported; `INTERSECT` and `EXCEPT` are not supported. |
| Window Functions | Limited | `RANK() OVER ([PARTITION BY ...] ORDER BY ...)` only; no `ROW_NUMBER`, `DENSE_RANK`, `LAG`, `LEAD`, or frames. |
| Scalar Functions & Expressions | Limited | `LOWER`, `UPPER`, arithmetic `+ - * /`; no `CASE WHEN`, `COALESCE`, `CAST`, `PRAGMA`, or date/math functions. |
| Vector Search (`VECTOR(n)`, `<->`, `<=>`) | Supported | Native DataVo extension (similar to `pgvector`, without `<#>` inner-product). |
| Views, Triggers, Procedures, UDFs | Not supported | Use [Live Reactive Views](/manual/fundamentals/reactive-views) (`Subscribe`) for continuous queries in C#. |
| System Catalogs (`pg_class`, `sqlite_master`) | Not supported | Use `SHOW DATABASES`, `SHOW TABLES`, `DESCRIBE <table>`, and `EXPLAIN`. |
| RBAC Security Commands | Supported | `CREATE USER`/`ROLE`, `GRANT`, `REVOKE`, `LOGIN`, `LOGOUT`, `SHOW USERS`/`ROLES`/`GRANTS` (not PostgreSQL roles). |

## Execution Paths & Performance Notes

Every construct marked **Supported** returns identical results across storage modes, but execution speed varies by path:

- **Typed fast paths:** Single-table filters, aggregates, `BETWEEN` on clustered primary keys, and `INNER`/`LEFT` joins on integer equality keys read stored rows directly with minimal allocations.
- **General evaluator:** `SELECT DISTINCT`, `RIGHT`/`FULL`/`CROSS` joins, joins on `VARCHAR` or `GUID` keys, cross-table `OR` predicates, CTEs, window functions, and queries inside explicit transactions materialize row dictionaries and are slower on large tables.

## Key Type Differences

- **`FLOAT` is 32-bit** (`System.Single`), unlike SQLite `REAL` or SQL Server `FLOAT` (64-bit). Use `DOUBLE` for 64-bit floating-point values.
- **`VARCHAR` comparisons are ordinal** (UTF-8 byte/code-point order), not collation-aware or case-insensitive.
- **No silent type fallbacks:** Unsupported types such as `DECIMAL`, `NUMERIC`, `DATETIME`, and `TIMESTAMP` throw an explicit `ParserException` instead of silently storing text.

## See Also
- [Data Types](/manual/sql/data-types): Supported SQL types, .NET CLR mappings, and unsupported type errors
- [Query Planner](/manual/internals/query-planner): How DataVo chooses between typed fast paths and the general evaluator
- [Functions & Operators](/manual/sql/functions): Complete list of aggregates, window functions, scalar functions, and operators
