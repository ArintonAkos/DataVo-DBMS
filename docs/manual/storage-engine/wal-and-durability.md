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

## Known Limitations

A hard crash (process kill, power loss) **during an `UPDATE`** can lose the row being updated, in both Lsm strict mode and Disk mode. An `UPDATE` currently writes the new row version and removes the old one as two separate durable steps, and a crash between them leaves neither. Rows that are not being updated at the moment of the crash, and every acknowledged `INSERT`, survive. This is reproduced by the crash-recovery test suite and is the next durability fix for Lsm.

Statements that change several rows, and explicit transactions, are not yet atomic across a crash in Lsm mode: after a crash, some of their changes may be present without the others.

Disk mode is legacy. It will not receive the crash-atomicity fix; use Lsm for new applications.
