# INSERT

> Source route: /manual/sql/statements/insert
> Source file: manual/sql/statements/insert.md

The `INSERT` statement adds one or more new rows into a table, validating all schema constraints (`PRIMARY KEY`, `UNIQUE`, `NOT NULL`, and `FOREIGN KEY`) and updating all table indexes atomically.

## Syntax

```sql
INSERT INTO table_name [(column1, column2, ...)]
VALUES (value1, value2, ...) [, (value1, value2, ...), ...];
```

## Parameters

| Parameter | Description |
| --- | --- |
| `table_name` | Target table to receive the new rows. |
| `(column1, column2, ...)` | Optional explicit list of target columns. Recommended for schema resilience; omitted columns receive their declared `DEFAULT` or `NULL`. |
| `VALUES (...) [, ...]` | One or more parenthesized tuples of literal values or bound parameters (`@param`). Each tuple must match the column count. |

## Single-Row Insert

Provide an explicit column list (recommended) or omit it for a positional full-row insert whose values match the table's declared column order.

```sql
-- Column-targeted insert (recommended)
INSERT INTO players (Id, Name, Score, Region)
VALUES (1, 'Ada', 980, 'EU');

-- Positional full-row insert (must match column order in CREATE TABLE)
INSERT INTO players
VALUES (2, 'Grace', 1120, 'NA');
```

## Explicit Column Lists & Defaults

When you specify an explicit column list, any omitted column automatically receives its declared `DEFAULT` literal, or `NULL` if no default is defined and the column is nullable. Omitting a `NOT NULL` column without a default fails validation.

```sql
-- Region receives its DEFAULT value or NULL
INSERT INTO players (Id, Name, Score)
VALUES (3, 'Linus', 875);

-- Inserting explicit NULL values
INSERT INTO players (Id, Name, Score, Region)
VALUES (4, 'Margaret', 1050, NULL);
```

## Multi-Row `VALUES`

Insert multiple rows in a single statement by separating value tuples with commas. Multi-row inserts reduce parse overhead and execute under a single write lock in auto-commit mode.

```sql
INSERT INTO matches (Id, PlayerId, Points, PlayedOn)
VALUES
    (101, 1, 85, '2026-03-14'),
    (102, 2, 92, '2026-03-15'),
    (103, 1, 110, '2026-03-16');
```

## Inserting `VECTOR` Columns

::: info DataVo Extension
`VECTOR(n)` columns accept **single-quoted** bracketed float-array literals (such as `'[0.1, 0.2, 0.3]'`) or bound parameters (`@embedding`). The element count must exactly match the column's declared dimension `n`.
:::

```sql
-- Quoted vector literal (shown with a 3-element shorthand; provide 384 floats for VECTOR(384))
INSERT INTO documents (Id, Title, Category, Embedding)
VALUES (1, 'Quickstart Guide', 'guide', '[0.12, 0.85, 0.44]');

-- Bound parameter in parameterized SQL (recommended for high-dimensional VECTOR(384) embeddings)
INSERT INTO documents (Id, Title, Category, Embedding)
VALUES (@id, @title, @category, @embedding);
```

::: warning Literal & Expression Rules in `VALUES`
- **Quoted vectors only:** Unquoted array syntax like `[0.1, 0.2, 0.3]` is **not** accepted by the SQL parser and raises a syntax error. Always quote vector literals (`'[0.1, 0.2, 0.3]'`) or pass a `float[]` parameter.
- **No expressions in `VALUES`:** Arithmetic or function expressions inside `VALUES` (for example `VALUES (1, 'Ada', 100 + 50, 'EU')` or `UPPER('eu')`) are **not** parsed. Supply plain literals (`'text'`, `42`, `3.14`, `TRUE`, `NULL`) or bound `@parameters`.
:::

## Constraints, Indexes & Transactions

- **Constraint enforcement:** Every row is checked against `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, and `FOREIGN KEY` constraints before storage is modified. If any row in a multi-row batch fails validation, the statement aborts with an error.
- **Index maintenance:** All B-Tree, `HNSW`, `FLAT`, and `COLUMNAR` indexes on the table are updated automatically.
- **Transactions:** Inside an explicit `BEGIN` ... `COMMIT` block, inserted rows are buffered in `TransactionContext`, visible immediately to subsequent queries in the same transaction, and cleanly discarded on `ROLLBACK` or `ROLLBACK TO SAVEPOINT`.

::: warning Unsupported `INSERT` Clauses in v0.5
The following SQL extensions are **not** supported in DataVo v0.5:
- `INSERT ... ON CONFLICT DO UPDATE / DO NOTHING` (upsert)
- `INSERT OR REPLACE` / `REPLACE INTO`
- `INSERT INTO ... SELECT ...`
- `RETURNING` clauses

To upsert rows in application code, check existence or handle the key conflict inside a transaction, then run `UPDATE` or `INSERT`.
:::

## See Also

- [UPDATE](/manual/sql/statements/update): Modifying existing rows with `SET` expressions
- [DELETE](/manual/sql/statements/delete): Removing rows and compacting storage with `VACUUM`
- [CREATE TABLE](/manual/sql/statements/create-table): Defining column types, `DEFAULT` values, and constraints
- [Transactions](/manual/sql/statements/transactions): Grouping writes with `BEGIN`, `SAVEPOINT`, `COMMIT`, and `ROLLBACK`
