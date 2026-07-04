# Limits

> Source route: /manual/reference/limits
> Source file: manual/reference/limits.md

DataVo v0.1 Alpha is an embedded engine preview, so its limits are part of the product contract. The safest way to evaluate it is to keep the database owned by one application process, validate the exact schema and query shapes you need, and treat storage files as alpha artifacts.

The intended deployment shape is in-process embedding.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./app_data",
    LsmStrictFsync = true
});
```

Do not point multiple independent processes at the same writable database directory. DataVo v0.1 does not document a cross-process concurrency contract.

```text
Supported:
  one application process
  -> one DataVoContext lifecycle
  -> one writable database directory

Not supported:
  process A -> ./shared_data
  process B -> ./shared_data
```

Do not treat alpha storage files as a stable long-term format. For important evaluations, keep seed data or import scripts so you can rebuild after an engine change.

```sql
CREATE TABLE SeedUsers (
  Id INT PRIMARY KEY,
  Name VARCHAR(80)
);

INSERT INTO SeedUsers (Id, Name) VALUES (1, 'Ada');
INSERT INTO SeedUsers (Id, Name) VALUES (2, 'Grace');
```

Tooling is also limited. DBeaver, DataGrip, and pgAdmin expect a client/server protocol and catalog metadata. DataVo v0.1 is embedded, so those tools cannot connect directly.

```text
Future shape:
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

The LSM engine uses a 32 MB active MemTable flush threshold internally. That is an engine behavior in v0.1, not a public tuning knob.

```text
active MemTable grows
  -> crosses 32 MB
  -> generation freezes
  -> new MemTable accepts writes
  -> background flush writes SSTable
```

## v0.1 Limits Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Embedded in-process deployment | Supported | No standalone server is required for normal use. |
| Fixed vector dimensions | Supported | Dimension is declared with `VECTOR(n)`. |
| Internal 32 MB LSM MemTable flush threshold | Supported | Present in the engine but not exposed as a public tuning setting. |
| Multi-process writes to one database directory | Not Supported | Do not share one writable DataVo directory across independent processes. |
| Stable alpha storage format | Not Supported | v0.1 files may need migration or rebuild after engine changes. |
| PostgreSQL-wire clients | Not Supported | DBeaver, DataGrip, and pgAdmin need future `DataVo.Server` work. |
| Cloud/server administration model | Not Supported | No server roles, replication daemon, backup daemon, or HA system. |
| Large analytical optimizer parity | Not Supported | Planner work is early and controlled by configuration. |
