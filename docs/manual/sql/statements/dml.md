---
title: DML Statements (INSERT, UPDATE, DELETE, VACUUM)
description: Insert, update, delete, and compact table rows using DataVo SQL DML statements.
---

# DML Statements

Data Manipulation Language (DML) statements add, modify, remove, and compact rows in a table. All DML statements participate in explicit transactions when executed between `BEGIN` and `COMMIT`.

```sql
INSERT INTO players (Id, Name, Score, Region)
VALUES (1, 'Ada', 980, 'EU');

UPDATE players
SET Score = 1050
WHERE Id = 1;

DELETE FROM players WHERE Score < 100;
VACUUM players;
```

## `INSERT INTO`

Insert rows using either a column-targeted list (recommended for schema resilience) or a positional full-row list:

```sql
-- Column-targeted (recommended)
INSERT INTO documents (id, title, category, embedding)
VALUES (1, 'Quickstart', 'guide', '[1.0, 0.0, 0.0]');

-- Positional full-row (must match table column order)
INSERT INTO documents
VALUES (2, 'Vector Search', 'guide', '[0.9, 0.1, 0.0]');
```

- The number of values must match the number of target columns (or all table columns for positional inserts).
- Omitted columns in a column-targeted insert receive their declared `DEFAULT` literal or `NULL` (if nullable).
- Vector columns accept bracketed float array string literals such as `'[0.9, 0.1, 0.0]'` matching the column's fixed dimension.
- `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, and `FOREIGN KEY` constraints are enforced on every insert.

## `UPDATE`

Modify one or more columns on rows matching a `WHERE` predicate:

```sql
UPDATE players
SET Score = Score + 50,
    Region = 'US'
WHERE Id = 1;
```

- Omitting `WHERE` updates every row in the table.
- Updating a `PRIMARY KEY`, `UNIQUE`, or `FOREIGN KEY` column re-validates key constraints and updates secondary indexes atomically.
- Narrow single-row updates on fixed-width schemas can also use compiled fast paths via [Compiled LINQ (`ExecuteUpdate`)](/manual/client-interfaces/compiled-linq) or [Source Generators](/manual/client-interfaces/source-generators).

## `DELETE FROM`

Remove rows matching a `WHERE` predicate:

```sql
DELETE FROM players
WHERE Region = 'Legacy' AND Score = 0;
```

- Omitting `WHERE` deletes all rows in the table.
- Foreign-key `ON DELETE CASCADE` deletes dependent child rows; `ON DELETE RESTRICT` rejects the delete if child rows still reference the target key.

## `VACUUM`

Reclaim space from deleted rows and compact table storage:

```sql
VACUUM players;
```

| Statement | Scope | Notes |
| --- | --- | --- |
| `INSERT INTO ... VALUES (...)` | Single table | Supports column lists, defaults, and vector literals. No `ON CONFLICT` / `MERGE`. |
| `UPDATE ... SET ... [WHERE ...]` | Single table | Supports multi-column assignments and expressions. No `RETURNING`. |
| `DELETE FROM ... [WHERE ...]` | Single table | Enforces `ON DELETE CASCADE` / `RESTRICT` foreign keys. |
| `VACUUM <table>` | Single table | Compacts deleted slots and storage files after heavy deletes. |

## See Also
- [Transactions](/manual/sql/statements/transactions) — Grouping DML writes with `BEGIN`, `SAVEPOINT`, `COMMIT`, and `ROLLBACK`
- [DataVoContext API](/manual/client-interfaces/datavo-context-api) — High-throughput C# `BulkInsert`, `InsertTypedBatch`, and `ApplyBatch`
- [DDL Statements](/manual/sql/statements/ddl) — Defining primary keys, defaults, and foreign-key delete actions
