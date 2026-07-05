# LSM Mode

> Source route: /manual/storage-engine/lsm-mode
> Source file: manual/storage-engine/lsm-mode.md

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

The LSM engine provides an arena-backed MemTable, a WAL segment per generation (retained until SSTable and manifest edits are durable), a 32 MB MemTable flush threshold (an internal v0.1 value, not a public tuning knob), a background flush pipeline that runs outside the foreground write path, and compaction (present, with tuning still in alpha). The on-disk format is **not** guaranteed stable — v0.1 storage files may change before a stable release.
