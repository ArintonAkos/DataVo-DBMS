# CREATE TABLE

> Source route: /manual/sql/statements/create-table
> Source file: manual/sql/statements/create-table.md

`CREATE TABLE` defines a new table in the active database along with its columns, data types, default values, and integrity constraints. DataVo enforces `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `DEFAULT`, and `FOREIGN KEY` constraints at write time.

## Syntax

```sql
CREATE TABLE [IF NOT EXISTS] <table_name> (
    <column_name> <data_type>
        [NOT NULL | NULL]
        [DEFAULT <literal>]
        [PRIMARY KEY]
        [UNIQUE]
        [REFERENCES <ref_table> (<ref_col>) [ON DELETE CASCADE | RESTRICT]],
    ...,
    [[CONSTRAINT <name>] PRIMARY KEY (<col1> [, <col2>, ...])],
    [[CONSTRAINT <name>] UNIQUE (<col>)],
    [[CONSTRAINT <name>] FOREIGN KEY (<col>) REFERENCES <ref_table> (<ref_col>) [ON DELETE CASCADE | RESTRICT]]
);
```

## Parameters

| Clause | Required | Description |
| --- | --- | --- |
| `IF NOT EXISTS` | No | Succeeds as a no-op if `<table_name>` already exists in the active database instead of throwing an error. |
| `<table_name>` | Yes | Unique table identifier within the active database. |
| `<column_name>` | Yes | Column identifier. Must be unique within the table and cannot be a reserved keyword such as `VECTOR`. |
| `<data_type>` | Yes | Any supported DataVo SQL type (`INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `BIT`, `DATE`, `GUID`, `VARCHAR(n)`, `VECTOR(n)`, and their aliases). See [Data Types](/manual/sql/data-types). |
| `NOT NULL` / `NULL` | No | Controls whether `NULL` values are rejected on `INSERT` and `UPDATE`. Columns allow `NULL` by default unless `NOT NULL` or `PRIMARY KEY` is specified. |
| `DEFAULT <literal>` | No | Constant fallback value used when a column is omitted from `INSERT`. Must be a literal (`NULL`, number, quoted string, `true`, or `false`). |
| `PRIMARY KEY` | No | Uniquely identifies rows. May be declared on one column or as a composite table-level constraint. At most one `PRIMARY KEY` per table. |
| `UNIQUE` | No | Enforces uniqueness across non-`NULL` values on a single column. Multiple `NULL` values are permitted. |
| `REFERENCES <ref_table>(<ref_col>)` | No | Declares a foreign-key reference to `<ref_table>(<ref_col>)`, with optional `ON DELETE CASCADE` or `ON DELETE RESTRICT` (default). |
| `CONSTRAINT <name>` | No | Optional SQL-standard prefix accepted before table-level constraints. The constraint name is parsed for compatibility and is not persisted in the catalog. |

## Basic Table Creation

Use `CREATE TABLE IF NOT EXISTS` in startup scripts so schema initialization is idempotent:

```sql
CREATE DATABASE IF NOT EXISTS App;
USE App;

CREATE TABLE IF NOT EXISTS departments (
    id INT PRIMARY KEY,
    name VARCHAR(80) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS employees (
    id INT PRIMARY KEY,
    external_id GUID NOT NULL UNIQUE,
    name VARCHAR(120) NOT NULL,
    department_id INT REFERENCES departments(id) ON DELETE RESTRICT,
    hired_on DATE,
    active BIT NOT NULL DEFAULT true
);
```

## Primary Keys

Each table may declare at most one `PRIMARY KEY`—either inline on a single column or as a table-level constraint. Declaring both a column-level and a table-level `PRIMARY KEY` on the same table throws a `ParserException`.

- **Single-column `INT` or `GUID` primary keys** are clustered on the `Lsm` storage engine, enabling fast key seeks and range scans.
- **Composite primary keys** are declared at the table level:

```sql
CREATE TABLE project_members (
    project_id INT NOT NULL,
    employee_id INT NOT NULL,
    role VARCHAR(40) NOT NULL DEFAULT 'contributor',
    CONSTRAINT pk_project_members PRIMARY KEY (project_id, employee_id)
);
```

::: tip
For optimal read and range-scan performance on `Lsm`, prefer a single `INT` or `GUID` `PRIMARY KEY` whenever your data model has a natural or surrogate key.
:::

## Nullability and Defaults

Columns are nullable by default; you may write `NULL` explicitly for clarity or `NOT NULL` to reject `NULL` on writes. `DEFAULT` accepts only constant literals:

```sql
CREATE TABLE settings (
    key VARCHAR(64) PRIMARY KEY,
    value VARCHAR(255) NULL,
    retries INT NOT NULL DEFAULT 3,
    ratio DOUBLE NOT NULL DEFAULT 0.5,
    enabled BIT NOT NULL DEFAULT false,
    label VARCHAR(32) DEFAULT 'default'
);
```

Expression defaults such as `DEFAULT NOW()` or `DEFAULT (1 + 1)` are rejected at parse/validation time.

## Unique Constraints

`UNIQUE` may be declared inline on a column or as a single-column table constraint. Multiple `NULL` values are allowed in a `UNIQUE` column because scalar indexes omit `NULL` keys.

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(160) NOT NULL UNIQUE,
    handle VARCHAR(60),
    CONSTRAINT uq_users_handle UNIQUE (handle)
);
```

Declaring a multi-column `UNIQUE (col1, col2)` constraint throws a `ParserException` in `v0.4`.

## Foreign Keys

Foreign keys validate parent row existence on `INSERT` and `UPDATE` and enforce referential actions on parent `DELETE`:

- `ON DELETE RESTRICT` (default): Rejects deleting a parent row while any child row references it.
- `ON DELETE CASCADE`: Automatically deletes referencing child rows when a parent row is deleted.

Foreign keys can be declared inline or at the table level. Table-level `FOREIGN KEY (<col>)` must reference a column declared earlier in the `CREATE TABLE` column list.

```sql
CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    total DOUBLE NOT NULL DEFAULT 0.0,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);
```

## Vector Columns

::: info DataVo Extension
`VECTOR(n)` and its shorthands (`VEC2`, `VEC3`, `VEC4`, `FLOAT[n]`, `REAL[n]`) are first-class DataVo extensions for storing fixed-dimension 32-bit float embeddings and spatial coordinates. `VECTOR` is a reserved keyword and cannot be used as a column name.
:::

```sql
CREATE TABLE documents (
    id INT PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    embedding VECTOR(384)
);

CREATE TABLE world_entities (
    id INT PRIMARY KEY,
    position VEC3 NOT NULL,
    velocity FLOAT[3]
);
```

After creating the table, attach an `HNSW` or `FLAT` index with [`CREATE INDEX`](/manual/sql/statements/indexes) to accelerate similarity search.

## Unsupported Constraints in v0.4

::: warning
`CHECK` constraints are **not supported** in DataVo `v0.4` and will fail with a `ParserException`. Enforce domain checks in application logic before writing.

Multi-column `UNIQUE (c1, c2)` constraints, expression-based `DEFAULT` clauses, and `ON UPDATE` foreign-key actions are also not supported in `v0.4`.
:::

## See Also

- [Data Types](/manual/sql/data-types) — Complete list of column types, aliases, and `.NET` CLR mappings
- [ALTER TABLE & DROP](/manual/sql/statements/alter-and-drop) — Adding, modifying, and dropping columns or tables
- [CREATE INDEX](/manual/sql/statements/indexes) — Creating `BTREE`, `HNSW`, `FLAT`, and `COLUMNAR` indexes
- [INSERT](/manual/sql/statements/insert) — Populating tables and triggering constraint checks
- [Vector Search](/manual/fundamentals/vector-search) — Indexing and querying `VECTOR(n)` columns
- [SQL Compatibility](/manual/sql/compatibility) — Full dialect comparison against SQLite and PostgreSQL
