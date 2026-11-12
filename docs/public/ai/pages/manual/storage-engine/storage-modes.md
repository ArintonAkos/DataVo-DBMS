# Storage Modes

> Source route: /manual/storage-engine/storage-modes
> Source file: manual/storage-engine/storage-modes.md

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

`StorageMode.InMemory` (ephemeral, with zero-alloc PK point lookups for tests, caching, and low-latency workloads), `StorageMode.Lsm` (WAL-covered MemTables, SSTables, manifests, clustered PKs, and compaction), and `StorageMode.Disk` (file-backed local tables with WAL options; **legacy**, see below) are the embedded storage modes in v0.2. `StorageMode.Wasm` (`DataVo.Browser`) is an `Experimental / Playground Lane` mode for the interactive documentation site only (not production-ready and not published on npm), and `StorageMode.Custom` provides custom storage injection.

`StorageMode.Lsm` is the durable mode DataVo is heading toward production with. `StorageMode.Disk` is a legacy mode: it stays available, but it receives no further durability work, and it is not crash-safe for `UPDATE` (see [WAL And Durability](./wal-and-durability.md#known-limitations)). New applications should use Lsm.

Disk and Lsm directories are served by one engine per process: contexts and connections opened on the same directory share it, and a `datavo.lock` file keeps other processes out while it is open. See [LSM Mode](./lsm-mode.md#opening-the-same-directory-twice).
