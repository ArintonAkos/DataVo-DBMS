# Data Manipulation

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

| Feature | Status | Notes |
| --- | --- | --- |
| Column-targeted `INSERT` | Supported | Recommended form for application code. |
| Full-row `INSERT` | Supported | Value count must match table column count. |
| Vector literal inserts | Supported | Use bracketed numeric values such as `'[1,0,0]'`. |
| `UPDATE ... SET ... WHERE ...` | Supported | General path supports predicate-based updates. |
| Fixed-width compiled update fast path | Supported | Applies only to narrow source-generated single-row update shapes. |
| `DELETE ... WHERE ...` | Supported | Deletes matching rows and updates indexes. |
| `VACUUM table` | Supported | Compacts storage and rebuilds indexes after deletes. |
| Transactional DML | Supported | `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK` are session-bound. |
| `MERGE` | Not Supported | Not part of v0.1. |
| PostgreSQL `COPY` protocol | Not Supported | No bulk copy protocol in v0.1. |
