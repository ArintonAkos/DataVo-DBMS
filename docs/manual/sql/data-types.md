---
title: Data Types
description: Supported SQL data types in DataVo v0.4, .NET CLR mappings, DATE storage contract, and unsupported type errors.
---

# Data Types

Every column in a DataVo table has a strict schema type. DataVo stores each accepted type natively and rejects unsupported SQL types with an explicit `ParserException` rather than silently falling back to text.

```sql
CREATE TABLE documents (
    id INT PRIMARY KEY,
    external_id GUID,
    title VARCHAR(200),
    published_on DATE,
    rating DOUBLE,
    is_public BIT,
    embedding VECTOR(384)
);
```

## Supported SQL Types

| Canonical Type | Accepted Aliases | .NET CLR Type | Storage & Behavior |
| --- | --- | --- | --- |
| `INT` | `INTEGER`, `SMALLINT`, `TINYINT`, `MEDIUMINT` | `int` (`Int32`) | 32-bit signed integer. Single `INT` primary key is clustered on `Lsm`. |
| `BIGINT` | — | `long` (`Int64`) | 64-bit signed integer. Also used as the return type of `COUNT(...)`. |
| `FLOAT` | `REAL` | `float` (`Single`) | 32-bit IEEE 754 float (not 64-bit like SQLite `REAL`). |
| `DOUBLE` | `DOUBLE PRECISION` | `double` (`Double`) | 64-bit IEEE 754 double. |
| `BIT` | `BOOL`, `BOOLEAN` | `bool` (`Boolean`) | Accepts `TRUE`/`FALSE` (any case); `1`/`0` and `'true'`/`'false'` are also parsed. |
| `DATE` | — | `DataVoDate` | Calendar date stored as a 32-bit day count since `0001-01-01`. |
| `GUID` | `UUID`, `UNIQUEIDENTIFIER` | `Guid` | 128-bit UUID. Single `GUID` primary key is clustered on `Lsm`. |
| `VARCHAR(n)` | `NVARCHAR`, `CHAR`, `NCHAR`, `TEXT`, `STRING` | `string` | UTF-8 variable-length text. Comparisons and sorting are ordinal (case-sensitive). |
| `VECTOR(n)` | `VEC2`, `VEC3`, `VEC4`, `FLOAT[n]`, `REAL[n]` | `float[]` | Fixed-dimension `n`-element array of 32-bit floats. |

::: info DataVo Extension
`VECTOR(n)` (with `VEC2`, `VEC3`, `VEC4`, `FLOAT[n]`, and `REAL[n]` shorthands) and `GUID` (`UUID`, `UNIQUEIDENTIFIER`) are first-class DataVo extensions beyond standard SQLite. `VECTOR` is a reserved keyword, so a column cannot be named `Vector`.
:::

## The `DATE` Storage & Row Contract

`DATE` stores only a calendar date (with no time-of-day or timezone component) as a zero-based 32-bit `DayNumber` (`0` = `0001-01-01`, `3652058` = `9999-12-31`).

- **Query materialization:** Reading a `DATE` column returns a `DataVoDate` struct (`DataVo.Core.DataVoDate`).
- **.NET interoperability:** On `.NET 10`, convert cleanly with `DataVoDate.FromDateOnly(DateOnly)` / `.ToDateOnly()` or `DataVoDate.FromDateTime(DateTime)` / `.ToDateTime()` (midnight `DateTimeKind.Unspecified`).
- **Timestamps:** If you need a point in time with sub-day precision, store Unix epoch milliseconds or ticks in a `BIGINT` column.

## Unsupported Types (`ParserException`)

DataVo throws a `ParserException` at `CREATE TABLE` or `ALTER TABLE` time for types it cannot store faithfully:

| Unsupported Type | Error Guidance / Workaround |
| --- | --- |
| `DECIMAL`, `NUMERIC`, `MONEY` | Use `DOUBLE` for approximate numeric values, or `VARCHAR` to preserve exact decimal digits. |
| `DATETIME`, `TIMESTAMP`, `TIME` | Use `DATE` for calendar dates, or `BIGINT` for Unix timestamps / ticks. |
| `LONG` | Use `BIGINT` for 64-bit integers. |
| `BLOB`, `BINARY`, `VARBINARY`, `JSON`, `JSONB` | Not supported as native column types in `v0.4`; encode as `VARCHAR` (e.g., Base64 or JSON text). |

## See Also
- [CREATE TABLE](/manual/sql/statements/create-table): Declaring columns, constraints, and defaults in `CREATE TABLE`
- [Indexes](/manual/sql/statements/indexes): Choosing B-Tree, HNSW, FLAT, or COLUMNAR indexes by column type
- [SQL Compatibility](/manual/sql/compatibility): Full dialect comparison against SQLite and PostgreSQL
