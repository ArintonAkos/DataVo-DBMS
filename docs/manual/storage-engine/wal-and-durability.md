# WAL And Durability

Durability settings decide when DataVo is allowed to acknowledge a write. The fastest setting is not always the safest setting, so benchmark numbers should always be read with the durability mode attached.

Disk mode can use WAL with normal buffered writes.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./datavo_data",
    WalEnabled = true,
    SyncDiskWrites = false
});
```

Turn on synchronous disk writes when the workload needs a stronger flush setting.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./datavo_data",
    WalEnabled = true,
    SyncDiskWrites = true
});
```

LSM strict mode waits for WAL durability before acknowledging writes. This is the conservative production-style setting.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_strict",
    LsmStrictFsync = true
});
```

LSM relaxed mode acknowledges after the OS-buffered WAL append path. It is appropriate for caches, rebuildable data, and benchmark ceilings, but recent acknowledged writes can be lost on power or kernel failure.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_relaxed",
    LsmStrictFsync = false
});
```

When documenting or comparing results, include the storage mode and durability setting next to the number.

```text
Good:
  DataVo LSM Relaxed, 1-thread throughput: 1,215,413 ops/s

Misleading:
  DataVo throughput: 1,215,413 ops/s
```

## Durability Support Summary

Supported: disk WAL (`WalEnabled`, on by default for disk mode), disk sync writes (`SyncDiskWrites = true`), LSM strict fsync (`LsmStrictFsync = true`, which waits for WAL durability before acknowledging writes), LSM relaxed OS-buffered mode (faster, but can lose recent writes on power or kernel failure), and WAL checkpoint settings. Relaxed throughput must **not** be presented as strict durability.

## Crash Atomicity in Lsm Mode

Every SQL statement, and every explicit transaction at `COMMIT`, is written to the data directory's commit log (`commit.wal`) as **one** checksummed record. After a crash, recovery applies each record completely or not at all, so a multi-row `UPDATE` or a transaction that touches several tables is never half-applied. In strict mode a statement returns only after its record is durable; concurrent statements share fsyncs.

## Known Limitations

- **Visibility before durability (Lsm).** Other sessions can see a statement's changes slightly before its commit record is durable. If the process crashes in that window, those changes are gone after recovery even though a reader may have seen them.
- **Failed statements (Lsm).** A statement that fails part-way keeps the changes it already made; there is no statement-level rollback yet. Use an explicit transaction when partial changes are unacceptable.
- **Disk mode is legacy** and not crash-safe for `UPDATE`: it writes the new row version and removes the old one as two separate durable steps, so a crash between them can lose the row. Use Lsm for new applications.
