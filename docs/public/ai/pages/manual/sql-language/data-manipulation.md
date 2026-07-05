# Data Manipulation

> Source route: /manual/sql-language/data-manipulation
> Source file: manual/sql-language/data-manipulation.md

Data manipulation statements change table contents. In v0.1, the core workflow is straightforward: insert rows, update rows by predicate, delete rows by predicate, and vacuum storage after deletes when compaction matters.

Use column-targeted inserts for application code. The statement says exactly which columns receive values.

```sql
INSERT INTO Users (Id, Name, Score, IsActive)
VALUES (1, 'Ada', 98.5, true);
```

Full-row inserts are shorter, but they depend on table column order.

```sql
INSERT INTO Users
VALUES (2, 'Grace', 95.0, true);
```

Insert vectors as bracketed numeric literals.

```sql
INSERT INTO Embeddings (Id, Label, Emb)
VALUES (1, 'red', '[1,0,0]');

INSERT INTO Embeddings (Id, Label, Emb)
VALUES (2, 'warm-red', '[0.9,0.1,0]');
```

Update rows with `SET` and `WHERE`.

```sql
UPDATE Users
SET Score = 99.0
WHERE Id = 1;
```

Update more than one column in the same statement.

```sql
UPDATE Users
SET Score = 100.0,
    IsActive = false
WHERE Name = 'Ada';
```

Delete rows with a predicate.

```sql
DELETE FROM Users
WHERE IsActive = false;
```

After many deletes, use `VACUUM` to compact the table.

```sql
VACUUM Users;
```

Wrap related writes in a transaction when they should commit or roll back together.

```sql
BEGIN TRANSACTION;

INSERT INTO Users (Id, Name, Score, IsActive)
VALUES (3, 'Katherine', 97.0, true);

UPDATE Users
SET Score = 98.0
WHERE Id = 3;

COMMIT;
```

Rollback discards buffered changes in the current session.

```sql
BEGIN TRANSACTION;

DELETE FROM Users
WHERE Id = 3;

ROLLBACK;
```

## DML Support Summary

Supported DML: column-targeted and full-row `INSERT` (the value count must match the columns), vector literal inserts (for example `'[1,0,0]'`), `UPDATE ... SET ... WHERE ...` (with a fixed-width compiled fast path for narrow source-generated single-row shapes), `DELETE ... WHERE ...`, `VACUUM table`, and session-bound transactional DML (`BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`). `MERGE` and the PostgreSQL `COPY` protocol are **not** supported in v0.1. See the [SQL compatibility matrix](/manual/sql-language/sql-compatibility) for the authoritative feature status.
