# WAL And Durability

> Source route: /manual/storage-engine/wal-and-durability
> Source file: manual/storage-engine/wal-and-durability.md

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

| Feature | Status | Notes |
| --- | --- | --- |
| Disk WAL | Supported | `WalEnabled` defaults to enabled for disk mode unless explicitly overridden. |
| Disk sync writes | Supported | `SyncDiskWrites = true` enables fsync-like behavior. |
| LSM strict fsync | Supported | `LsmStrictFsync = true` waits for WAL durability before acknowledging writes. |
| LSM relaxed OS-buffered mode | Supported | Faster, but can lose recent writes on power or kernel failure. |
| WAL checkpoint settings | Supported | Disk mode has checkpoint threshold and interval settings. |
| Relaxed throughput as strict durability | Not Supported | Relaxed benchmark wins must not be presented as strict durability. |
