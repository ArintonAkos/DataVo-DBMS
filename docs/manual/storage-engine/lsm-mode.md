# LSM Mode

`StorageMode.Lsm` is DataVo's high-throughput persisted backend. It is the mode behind the headline write-path benchmark results.

Start with strict durability.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

Writes first go to the WAL and the active MemTable. The active MemTable is designed around arena-backed storage so steady-state write allocation stays low.

```sql
CREATE TABLE Events (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  Value INT
);

INSERT INTO Events (Id, Name, Value)
VALUES (1, 'started', 100);
```

When the active MemTable crosses 32 MB, DataVo freezes that generation, swaps in a new MemTable and WAL segment, and lets a background worker flush the frozen generation to SSTables.

```text
write
  -> append WAL
  -> active MemTable
  -> freeze at 32 MB
  -> background SSTable flush
  -> manifest edit
  -> covered WAL segment can be removed
```

Relaxed LSM mode uses the same broad storage shape, but it does not wait for synchronous WAL fsync before acknowledging writes.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_relaxed",
    LsmStrictFsync = false
});
```

## LSM Support Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Arena-backed MemTable | Supported | Designed to keep steady-state managed allocation low. |
| WAL segment per generation | Supported | WAL segments are retained until SSTable and manifest edits are durable. |
| 32 MB MemTable flush threshold | Supported | Internal threshold in v0.1, not a public tuning setting. |
| Background flush pipeline | Supported | Frozen generations flush outside the foreground write path. |
| Compaction | Supported | Present as part of the LSM engine; tuning remains alpha. |
| Stable on-disk format guarantee | Not Supported | v0.1 storage files may change before a stable release. |
