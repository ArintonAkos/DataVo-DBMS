# Limits

DataVo v0.2 Release Candidate (`v0.2.0-rc.3`) is an embedded engine, so its architectural limits are part of the product contract. Keep the database owned by one application process, validate the exact schema and query shapes you need, and maintain seed or export workflows across prerelease upgrades.

The intended deployment shape is in-process embedding.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./app_data",
    LsmStrictFsync = true
});
```

Do not point multiple independent processes at the same writable database directory. DataVo v0.2 enforces single-process ownership via `datavo.lock` and does not support cross-process concurrent writers.

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

External GUI tooling is also limited today. DBeaver, DataGrip, and pgAdmin expect a client/server wire protocol and catalog metadata. DataVo v0.2 is embedded, so those tools cannot connect until the v0.3+ `DataVo.Server` bridge is introduced.

```text
Future shape (v0.3+):
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

The LSM engine uses a 32 MB active MemTable flush threshold internally. That is an internal engine constant in v0.2, not a public tuning knob.

```text
active MemTable grows
  -> crosses 32 MB
  -> generation freezes
  -> new MemTable accepts writes
  -> background flush writes SSTable
```

## v0.2 Limits Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Embedded in-process deployment | Supported | No standalone server is required for normal use. |
| Fixed vector dimensions | Supported | Dimension is declared with `VECTOR(n)`. |
| Internal 32 MB LSM MemTable flush threshold | Supported | Present in the engine but not exposed as a public tuning setting. |
| Multi-process writes to one database directory | Not Supported | Guarded by `datavo.lock`; do not share one writable DataVo directory across independent processes. |
| Long-term storage format compatibility guarantee | Planned | Prerelease files may require migration or rebuild across major storage upgrades. |
| PostgreSQL-wire clients | Planned (`v0.3+`) | DBeaver, DataGrip, and pgAdmin require future `DataVo.Server` work. |
| Cloud/server administration model | Not Supported | No server replication daemon, backup daemon, or HA cluster system. |
| Columnar HTAP scan acceleration | Active (`v0.3`) | Full analytical scans over 1M+ rows are row-oriented in v0.2; Columnar Indexes are under active v0.3 development. |
