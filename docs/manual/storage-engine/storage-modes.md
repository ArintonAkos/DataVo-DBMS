# Storage Modes

`StorageMode` chooses where DataVo stores rows. Pick the simplest mode that matches the job: in-memory for tests, disk for straightforward persistence, and LSM for high-throughput persisted workloads.

Use in-memory mode for examples and unit tests.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});
```

Use disk mode when you want simple file-backed storage and WAL configuration.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./datavo_data",
    WalEnabled = true
});
```

Use LSM mode for persisted write-heavy experiments. Strict fsync is the default durability setting to start with.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

Relaxed LSM is the benchmark/cache mode. It can be much faster because it does not wait for synchronous stable-storage flush on each acknowledged write.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_cache",
    LsmStrictFsync = false
});
```

## Storage Mode Support Summary

`StorageMode.InMemory` (ephemeral, for tests, demos, and isolated engine benchmarks), `StorageMode.Disk` (file-backed local tables with WAL options), and `StorageMode.Lsm` (WAL-covered MemTables, SSTables, manifests, and compaction) are the public launch modes. `StorageMode.Wasm` (browser/WASM) and `StorageMode.Custom` (custom storage injection) exist but are planned rather than primary v0.1 paths.
