# Limits

> Source route: /manual/reference/limits
> Source file: manual/reference/limits.md

DataVo is an embedded engine, so its architectural limits are part of the product contract. Keep the database owned by one application process, validate the exact schema and query shapes you need, and maintain seed or export workflows across prerelease upgrades.

The intended deployment shape is in-process embedding.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./app_data",
    LsmStrictFsync = true
});
```

Do not point multiple independent processes at the same writable database directory. DataVo enforces single-process ownership with a `datavo.lock` file and does not support writers in more than one process. Contexts and connections inside one process that open the same directory share one engine.

```text
Supported:
  one application process
  -> one DataVoContext lifecycle
  -> one writable database directory

Not supported:
  process A -> ./shared_data
  process B -> ./shared_data
```

Do not treat prerelease storage files as a permanent long-term archival format without export/import scripts. Keep seed data or import scripts so you can rebuild if on-disk structures evolve before 1.0.

```sql
CREATE TABLE SeedUsers (
  Id INT PRIMARY KEY,
  Name VARCHAR(80)
);

INSERT INTO SeedUsers (Id, Name) VALUES (1, 'Ada');
INSERT INTO SeedUsers (Id, Name) VALUES (2, 'Grace');
```

External GUI tooling is also limited today. DBeaver, DataGrip, and pgAdmin expect a client/server wire protocol and catalog metadata. DataVo is embedded and speaks no wire protocol, so those tools cannot connect. A `DataVo.Server` bridge is planned but not scheduled.

```text
Possible future shape (unscheduled):
  DBeaver/DataGrip/pgAdmin
  -> PostgreSQL wire protocol subset
  -> DataVo.Server
  -> DataVo storage
```

Vector dimensions are fixed at the column level. If a table declares `VECTOR(1536)`, application code must insert vectors with that dimension.

```sql
CREATE TABLE Documents (
  Id INT PRIMARY KEY,
  Embedding VECTOR(1536)
);
```

The LSM engine uses a 32 MB active MemTable flush threshold internally. That is an internal engine constant, not a public tuning knob.

```text
active MemTable grows
  -> crosses 32 MB
  -> generation freezes
  -> new MemTable accepts writes
  -> background flush writes SSTable
```

## Query-shape limits

Correctness does not depend on query shape, but cost does. Shapes outside the typed paths run on the general evaluator, which builds a dictionary per row read; on a million-row table that is seconds and gigabytes of allocation per query. Those shapes are: `SELECT DISTINCT`, `RIGHT`/`FULL`/`CROSS` joins, joins on text or `GUID` keys, an `OR` across two joined tables, computed expressions in the select list, window functions, common table expressions, and any query inside an explicit transaction. See [Query Planner And Fast Paths](/manual/storage-engine/query-planner-fast-paths).

On the LSM engine a full scan reads every row from memory-mapped SSTables, so with data in the page cache a 1,000,000-row filtered scan takes about 35–75 ms; a cold read from disk was not measured. HNSW is approximate: its recall depends on the data, and it should be measured on your own embeddings.

## Limits Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Embedded in-process deployment | Supported | No standalone server is required for normal use. |
| Fixed vector dimensions | Supported | Dimension is declared with `VECTOR(n)`. |
| Internal 32 MB LSM MemTable flush threshold | Supported | Present in the engine but not exposed as a public tuning setting. |
| Multi-process writes to one database directory | Not Supported | Guarded by `datavo.lock`; do not share one writable DataVo directory across independent processes. |
| Long-term storage format compatibility guarantee | Planned | Prerelease files may require migration or rebuild across major storage upgrades. |
| PostgreSQL-wire clients | Planned, unscheduled | DBeaver, DataGrip, and pgAdmin require a future `DataVo.Server` bridge. |
| Cloud/server administration model | Not Supported | No server replication daemon, backup daemon, or HA cluster system. |
| Transaction isolation | Supported (snapshot isolation) | Snapshots are ordered by commit; conflicting writers fail first-committer-wins. Serializable isolation is not offered: write skew is possible. See [Transactions](/manual/storage-engine/transactions-acid-mvcc). |
| Columnar HTAP scan acceleration | Supported (`InMemory` & `Lsm`) | Synchronized `InMemory` column buffers and out-of-core format-v8 `Lsm` SSTable columnar segments with zone-map chunk pruning. |
