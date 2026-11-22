---
title: ALTER TABLE & DROP
description: Evolve table schemas with ALTER TABLE ADD, DROP, and MODIFY COLUMN, and remove tables or indexes with DROP TABLE and DROP INDEX in DataVo v0.4.
---

# ALTER TABLE & DROP

Use `ALTER TABLE` to add, remove, or modify non-key columns on an existing table, and use `DROP TABLE` or `DROP INDEX` to remove schema objects from the active database.

## Syntax

```sql
-- Add a column
ALTER TABLE <table_name> ADD [COLUMN] <column_name> <data_type>
    [DEFAULT <literal>] [NOT NULL | NULL];

-- Drop a column
ALTER TABLE <table_name> DROP [COLUMN] <column_name>;

-- Modify a column
ALTER TABLE <table_name> MODIFY [COLUMN] <column_name> <data_type>
    [DEFAULT <literal>] [NOT NULL | NULL];

-- Drop a table
DROP TABLE [IF EXISTS] <table_name>;

-- Drop an index
DROP INDEX <index_name> ON <table_name>;
```

## Parameters

| Clause | Statement | Description |
| --- | --- | --- |
| `ADD [COLUMN]` | `ALTER TABLE` | Appends a new column to `<table_name>` and backfills existing rows with its `DEFAULT` literal or `NULL`. The `COLUMN` keyword is optional. |
| `DROP [COLUMN]` | `ALTER TABLE` | Removes `<column_name>` from `<table_name>` and rewrites stored rows. The `COLUMN` keyword is optional. |
| `MODIFY [COLUMN]` | `ALTER TABLE` | Changes `<column_name>`'s data type, `VARCHAR(n)` length, nullability, or `DEFAULT` value, converting existing rows in place. The `COLUMN` keyword is optional. |
| `IF EXISTS` | `DROP TABLE` | Succeeds as a no-op if `<table_name>` does not exist. Supported on `DROP TABLE` and `DROP DATABASE`, **not** on `DROP INDEX`. |
| `ON <table_name>` | `DROP INDEX` | Required target table qualifier identifying which table owns `<index_name>`. |

::: warning Unsupported ALTER TABLE Operations
`RENAME COLUMN` and `RENAME TO` are **not yet supported** in DataVo `v0.4` and throw a `ParserException` (`Unknown ALTER TABLE operation`). To rename a table or column, create the new table or column, copy data over, and drop the old one.
:::

## `ALTER TABLE ... ADD COLUMN`

`ADD COLUMN` acquires a table write lock, appends the column to the catalog, backfills all existing rows with the column's `DEFAULT` literal (or `NULL` if omitted), and rewrites the table and its existing indexes.

```sql
ALTER TABLE employees ADD COLUMN title VARCHAR(80) DEFAULT 'engineer';
ALTER TABLE employees ADD bio VARCHAR(500) NULL;
ALTER TABLE employees ADD COLUMN active BIT NOT NULL DEFAULT true;
```

### Restrictions

- **No key or FK constraints:** Adding a column with `PRIMARY KEY`, `UNIQUE`, or `REFERENCES` fails at execution time with a `CatalogException`.
- **`NOT NULL` on non-empty tables:** Adding a `NOT NULL` column without a `DEFAULT` literal on a table that already contains rows throws a `ConstraintViolationException` (`NotNullViolation`). Supply a `DEFAULT` literal so existing rows can be backfilled safely.

## `ALTER TABLE ... DROP COLUMN`

`DROP COLUMN` removes a column from the catalog, strips it from all existing rows, and rebuilds remaining table indexes.

```sql
ALTER TABLE employees DROP COLUMN bio;
ALTER TABLE employees DROP title;
```

### Restrictions

DataVo rejects `DROP COLUMN` with a `CatalogException` if the target column is:

- The last remaining column in the table.
- Part of the table's `PRIMARY KEY`.
- Declared `UNIQUE`.
- A `FOREIGN KEY` column.
- Referenced by another table's `FOREIGN KEY` (parent column).
- Part of any index (`BTREE`, `HNSW`, `FLAT`, or `COLUMNAR`). Drop the index first with `DROP INDEX`, then drop the column.

## `ALTER TABLE ... MODIFY COLUMN`

`MODIFY COLUMN` updates a column's type, maximum `VARCHAR(n)` length, nullability, or `DEFAULT` literal. Existing row values are validated and converted before the table is rewritten; if any existing row fails validation, the schema remains unchanged.

```sql
-- Widen a VARCHAR column
ALTER TABLE employees MODIFY COLUMN name VARCHAR(200) NOT NULL;

-- Change default and nullability
ALTER TABLE employees MODIFY active BIT NOT NULL DEFAULT false;

-- Widen INT to BIGINT
ALTER TABLE metrics MODIFY COLUMN count BIGINT NOT NULL DEFAULT 0;
```

### Restrictions

- **Existing data validation:**
  - Switching a column to `NOT NULL` fails with `ConstraintViolationException` (`NotNullViolation`) if any existing row contains `NULL`.
  - Shrinking `VARCHAR(n)` fails with `ConstraintViolationException` (`ValueTooLong`) if any existing string exceeds the new length `n`—DataVo never silently truncates data.
  - Incompatible type conversions for existing values fail and leave the table untouched.
- **Protected columns:** You cannot modify a column that is a `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, referenced parent column, or part of any index.
- **No constraint additions:** You cannot add `PRIMARY KEY`, `UNIQUE`, or `FOREIGN KEY` constraints via `MODIFY COLUMN`.

::: tip
Because `ALTER TABLE` rewrites the table under a write lock, group schema design choices into `CREATE TABLE` when possible and drop secondary indexes before altering indexed columns.
:::

## `DROP TABLE`

`DROP TABLE` removes the table definition, all stored rows, and all indexes attached to that table from the active database.

```sql
DROP TABLE temp_imports;
DROP TABLE IF EXISTS staging_events;
```

Use `IF EXISTS` in cleanup or migration scripts so dropping an already-absent table succeeds without error.

## `DROP INDEX`

`DROP INDEX` removes an index from the catalog and deletes its physical index structure (B-Tree, HNSW/FLAT vector index, or Columnar segment registration) while keeping the underlying table data intact.

```sql
DROP INDEX ix_players_score ON players;
DROP INDEX ix_documents_emb ON documents;
DROP INDEX cx_players_stats ON players;
```

::: warning `ON <table>` Required & No `IF EXISTS`
In DataVo `v0.4`, `DROP INDEX` **requires** the `ON <table_name>` clause and does **not** support `IF EXISTS`. Running `DROP INDEX ix_foo;` or `DROP INDEX IF EXISTS ix_foo ON bar;` throws a `ParserException`.
:::

## See Also

- [CREATE TABLE](/manual/sql/statements/create-table) — Declaring tables, primary keys, unique constraints, and foreign keys
- [CREATE INDEX](/manual/sql/statements/indexes) — Creating B-Tree, HNSW, FLAT, and Columnar indexes
- [Databases & Security](/manual/sql/statements/databases-and-security) — `DROP DATABASE`, `DESCRIBE`, and `SHOW TABLES`
- [Data Types](/manual/sql/data-types) — Supported column types and length rules
- [SQL Compatibility](/manual/sql/compatibility) — Dialect differences from SQLite and PostgreSQL
