# CREATE TABLE

`CREATE TABLE` defines the shape of rows DataVo can store. A table declaration gives each column a name and type, and it can mark a column as the primary key.

The most common table starts with an integer primary key.

```sql
CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  Score FLOAT,
  IsActive BIT
);
```

Insert a row immediately after creating the table to verify the shape.

```sql
INSERT INTO Users (Id, Name, Score, IsActive)
VALUES (1, 'Ada', 98.5, true);

SELECT Id, Name, Score, IsActive
FROM Users
WHERE Id = 1;
```

Use `VARCHAR(n)` for text fields. DataVo treats unrecognized string-like declarations as variable text in the v0.1 parser, but explicit `VARCHAR(n)` keeps schemas readable.

```sql
CREATE TABLE Articles (
  Id INT PRIMARY KEY,
  Title VARCHAR(200),
  Slug VARCHAR(200),
  Published BIT
);
```

Use `GUID`, `UUID`, or `UNIQUEIDENTIFIER` when application IDs are GUID-shaped.

```sql
CREATE TABLE ApiKeys (
  Id GUID PRIMARY KEY,
  Name VARCHAR(120),
  Revoked BIT
);
```

Use `VECTOR(n)` for embeddings. The dimension is part of the schema, so every value in that column must have the same length.

```sql
CREATE TABLE Documents (
  Id INT PRIMARY KEY,
  Title VARCHAR(200),
  Embedding VECTOR(1536)
);
```

Create scalar indexes after the table exists.

```sql
CREATE INDEX ix_users_name
ON Users (Name);

CREATE INDEX ix_users_active_score
ON Users (IsActive, Score);
```

Create vector indexes on vector columns. Use `FLAT` for exact scan behavior on small datasets and `HNSW` when a larger dataset needs faster approximate nearest-neighbor search.

```sql
CREATE INDEX idx_documents_embedding_flat
ON Documents (Embedding)
USING FLAT;

CREATE INDEX idx_documents_embedding_hnsw
ON Documents (Embedding)
USING HNSW;
```

Schema changes are supported but limited. Adding a column rewrites existing rows and fills the new column with a default or null.

```sql
ALTER TABLE Users
ADD COLUMN CreatedBy VARCHAR(80) DEFAULT 'system';
```

Dropping or modifying columns is intentionally more restrictive when keys, foreign keys, or indexes are involved.

```sql
ALTER TABLE Users
DROP COLUMN CreatedBy;

ALTER TABLE Users
MODIFY COLUMN Name VARCHAR(160);
```

## DDL Support Summary

Supported DDL: `CREATE TABLE` (catalog metadata and table storage), integer primary keys, `VARCHAR(n)`, the scalar type set (`INT`, `FLOAT`, `BIT`, `DATE`, `GUID`), `VECTOR(n)`, scalar and vector `CREATE INDEX` (`USING HNSW`, `USING FLAT`), `ALTER TABLE ADD COLUMN` (rewrites rows and backfills null/default values), and limited `ALTER TABLE DROP/MODIFY COLUMN` (which rejects key, foreign-key, referenced, indexed, or incompatible changes). PostgreSQL domains and collations are **not** supported. See the [SQL compatibility matrix](/manual/sql-language/sql-compatibility) for the authoritative feature status.
