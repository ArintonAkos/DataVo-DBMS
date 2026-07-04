# SQL Reference

DataVo has a compact SQL dialect implemented by the in-process parser and execution pipeline. The examples below are based on the current source and tests.

## Create and select a database

```sql
CREATE DATABASE Demo;
USE Demo;
```

## Create a table

```sql
CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(50),
  Score FLOAT,
  Active BIT
);
```

The parser currently maps declared column types into these catalog types:

| SQL spelling | Catalog type |
| --- | --- |
| `INT` or strings containing `int` | `INT` |
| `FLOAT` | `FLOAT` |
| `BIT` | `BIT` |
| `DATE` | `DATE` |
| `GUID`, `UUID`, `UNIQUEIDENTIFIER` | `GUID` |
| `VECTOR(n)` | `VECTOR` with dimension `n` |
| Other strings, including `VARCHAR(n)` | `VARCHAR` |

`CREATE TABLE` registers catalog metadata, materializes table storage, creates a primary-key index when primary keys are declared, and creates unique-key indexes for columns marked unique.

## Insert rows

Full-row insert:

```sql
INSERT INTO Users VALUES (1, 'Alice', 95.5, true);
```

Column-targeted insert:

```sql
INSERT INTO Users (Id, Name, Score) VALUES (2, 'Bob', 88.0);
```

When no column list is supplied, the number of values must match the table's catalog column count. When a column list is supplied, the number of values must match the supplied column list.

## Query rows

```sql
SELECT Id, Name, Score
FROM Users
WHERE Score > 90
ORDER BY Score DESC
LIMIT 10;
```

The documented and tested query surface includes projection, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, aggregates, `GROUP BY`, `HAVING`, joins, `UNION`, `UNION ALL`, subqueries, and vector distance expressions.

## Update and delete

```sql
UPDATE Users
SET Score = 99.0
WHERE Id = 1;

DELETE FROM Users
WHERE Id = 2;
```

`VACUUM` compacts a table after deletes:

```sql
VACUUM Users;
```

## Scalar indexes

```sql
CREATE INDEX ix_users_name ON Users (Name);
```

Composite indexes are accepted:

```sql
CREATE INDEX ix_users_name_score ON Users (Name, Score);
```

The scalar index path builds a key from indexed column values and stores row IDs behind that key.

## Vector columns and indexes

Define a fixed-dimension vector column:

```sql
CREATE TABLE Embeddings (
  Id INT PRIMARY KEY,
  Label VARCHAR(50),
  Emb VECTOR(3)
);
```

Insert vector literals as bracketed numeric values:

```sql
INSERT INTO Embeddings (Id, Label, Emb) VALUES (1, 'A', '[1,0,0]');
INSERT INTO Embeddings (Id, Label, Emb) VALUES (2, 'B', '[0,1,0]');
```

Create an approximate HNSW vector index:

```sql
CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW;
```

Create an exact flat vector index:

```sql
CREATE INDEX idx_flat_emb ON Embeddings (Emb) USING FLAT;
```

Rank by cosine distance with `<=>`:

```sql
SELECT Id, Label, Emb <=> '[0.9,0.1,0]' AS rank
FROM Embeddings
ORDER BY rank ASC
LIMIT 1;
```

Rank by Euclidean/L2 distance with `<->`:

```sql
SELECT Id, Label, Emb <-> '[0.9,0.1,0]' AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 5;
```

Vector distance predicates can be combined with scalar filters:

```sql
SELECT Id, Label, Emb <=> '[1,0,0]' AS rank
FROM Embeddings
WHERE Emb <=> '[1,0,0]' < 0.2
ORDER BY rank ASC
LIMIT 10;
```

## Transactions

```sql
BEGIN TRANSACTION;

INSERT INTO Users (Id, Name, Score) VALUES (3, 'Cara', 91.0);
UPDATE Users SET Score = 92.0 WHERE Id = 3;

COMMIT;
```

Rollback:

```sql
BEGIN TRANSACTION;
DELETE FROM Users WHERE Id = 3;
ROLLBACK;
```
