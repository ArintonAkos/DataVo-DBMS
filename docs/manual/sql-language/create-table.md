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

Use `VARCHAR(n)` for text fields. DataVo treats unrecognized string-like declarations as variable text in the v0.2 parser, but explicit `VARCHAR(n)` keeps schemas readable.

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

For 3D positions, `VEC3` and `FLOAT[3]` are both shorthand for `VECTOR(3)`. `VEC2`, `VEC4` and `FLOAT[n]`/`REAL[n]` work the same way.

```sql
CREATE TABLE Ships (
  Id INT PRIMARY KEY,
  Position VEC3 NOT NULL,
  Velocity FLOAT[3]
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

An HNSW index ranks by cosine distance unless you choose another metric. Positions need straight-line distance, so index them with `metric = 'euclidean'` (or `'l2'`). The metric is saved with the index and kept when the database is reopened.

```sql
CREATE INDEX ix_ships_position
ON Ships (Position)
USING HNSW WITH (metric = 'euclidean');
```

Create a columnar index on the numeric columns that analytical queries filter and aggregate on. It keeps a second copy of those columns as dense arrays, so a `COUNT`, `SUM`, `AVG`, `MIN` or `MAX` that reads only indexed columns is computed many values at a time without reading rows.

```sql
CREATE INDEX cx_players
ON Players (Level, Gold, Ratio)
USING COLUMNAR;
```

- The columns must be `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `BIT` or `DATE`. Any other type is rejected.
- It applies to the in-memory storage mode. On other storage modes the statement is accepted and recorded, and queries read rows as before.
- It is used for an aggregate without `GROUP BY` whose `WHERE` is an `AND` of comparisons between an indexed column and a literal, or `IS NULL` tests. Every column in the `WHERE` and in the aggregates must be in the index. Other queries, and queries inside a transaction, read rows.
- A comparison is left to the row scan when the index could not give exactly the same answer: a fractional literal against an `INT` column, a literal that is not exactly a `FLOAT`, a `BIT` or `DATE` column in the `WHERE`, or a floating-point column that holds a NaN.
- It never serves a row lookup, so it does not replace a scalar index. Several columnar indexes on one table are kept as one. `DROP INDEX cx_players ON Players` removes it.
- It costs memory (4 or 8 bytes per row per column) and a write on every insert and delete. It is held in memory only; nothing is written to disk for it.

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

Supported DDL: `CREATE TABLE` (catalog metadata and table storage), integer primary keys, `VARCHAR(n)`, the scalar type set (`INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `BIT`, `DATE`, `GUID`), `VECTOR(n)` (with `VEC2`/`VEC3`/`VEC4` and `FLOAT[n]` shorthands), scalar and vector `CREATE INDEX` (`USING HNSW`, `USING FLAT`, and `WITH (metric = 'cosine' | 'euclidean')` for HNSW), columnar `CREATE INDEX ... USING COLUMNAR` (in-memory storage, fixed-width numeric, `BIT` and `DATE` columns), `ALTER TABLE ADD COLUMN` (rewrites rows and backfills null/default values), and limited `ALTER TABLE DROP/MODIFY COLUMN` (which rejects key, foreign-key, referenced, indexed, or incompatible changes). PostgreSQL domains and collations are **not** supported. See the [SQL compatibility matrix](/manual/sql-language/sql-compatibility) for the authoritative feature status.
