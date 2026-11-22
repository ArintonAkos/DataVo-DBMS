# DDL Statements

> Source route: /manual/sql/statements/ddl
> Source file: manual/sql/statements/ddl.md

Data Definition Language (DDL) statements create, alter, and drop databases and tables. DataVo enforces declared primary keys, unique constraints, nullability, defaults, and foreign-key referential integrity at write time.

```sql
CREATE DATABASE IF NOT EXISTS App;
USE App;

CREATE TABLE Departments (
    Id INT PRIMARY KEY,
    Name VARCHAR(80) NOT NULL UNIQUE
);

CREATE TABLE Employees (
    Id INT PRIMARY KEY,
    Name VARCHAR(120) NOT NULL,
    DepartmentId INT REFERENCES Departments(Id) ON DELETE RESTRICT,
    Active BIT DEFAULT true
);
```

## Database Statements

```sql
CREATE DATABASE [IF NOT EXISTS] <db_name>;
DROP DATABASE [IF EXISTS] <db_name>;
```

Use `USE <db_name>` to switch the active database for the current session.

## `CREATE TABLE`

```sql
CREATE TABLE [IF NOT EXISTS] <table_name> (
    <col_name> <type> [NOT NULL] [DEFAULT <literal>] [PRIMARY KEY] [UNIQUE]
                      [REFERENCES <ref_table>(<ref_col>) [ON DELETE CASCADE | RESTRICT]],
    ...,
    [PRIMARY KEY (<col1>, ...)],
    [UNIQUE (<col>)],
    [FOREIGN KEY (<col>) REFERENCES <ref_table>(<ref_col>) [ON DELETE CASCADE | RESTRICT]]
);
```

### Supported Constraints

| Constraint | Scope | Rules in `v0.4` |
| --- | --- | --- |
| `PRIMARY KEY` | Column or table | Only one `PRIMARY KEY` per table. Single `INT` or `GUID` primary keys are clustered on `Lsm`. |
| `UNIQUE` | Single column | Enforces uniqueness across non-NULL values; multiple `NULL`s are permitted. Multi-column `UNIQUE (a, b)` is not yet supported. |
| `NOT NULL` | Column | Rejects `NULL` values on `INSERT` and `UPDATE`. |
| `DEFAULT <literal>` | Column | Must be a constant literal (`NULL`, number, quoted string, `true`/`false`). Expressions like `NOW()` are rejected. |
| `FOREIGN KEY ... REFERENCES` | Column or table | Validates parent existence on write. Supports `ON DELETE CASCADE` and `ON DELETE RESTRICT`. |

## `ALTER TABLE`

DataVo supports three `ALTER TABLE` operations:

```sql
ALTER TABLE Employees ADD COLUMN Role VARCHAR(60) DEFAULT 'member';
ALTER TABLE Employees MODIFY COLUMN Name VARCHAR(200);
ALTER TABLE Employees DROP COLUMN Role;
```

| Operation | Behavior & Restrictions |
| --- | --- |
| `ADD COLUMN` | Rewrites existing rows and backfills the new column with its `DEFAULT` literal or `NULL`. |
| `DROP COLUMN` | Rejects columns that belong to a primary key, foreign key, referenced parent key, or index. |
| `MODIFY COLUMN` | Rejects incompatible type changes or modifications to key, foreign-key, referenced, or indexed columns. |

## `DROP TABLE`

```sql
DROP TABLE [IF EXISTS] <table_name>;
```

Drops the table definition, its rows, and its associated indexes from the active database.

## See Also
- [Data Types](/manual/sql/data-types) — Complete list of column types and aliases for `CREATE TABLE`
- [Indexes](/manual/sql/statements/indexes) — Creating `BTREE`, `HNSW`, `FLAT`, and `COLUMNAR` indexes after table creation
- [DML Statements](/manual/sql/statements/dml) — Inserting, updating, and deleting rows
