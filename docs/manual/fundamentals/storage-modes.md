---
title: Storage Modes
description: DataVo's two storage modes, InMemory and Lsm, and the Wasm and Custom embedding hooks.
---

# Storage Modes

`StorageMode` selects where DataVo stores table rows and how writes survive process restarts. DataVo has two storage modes: `InMemory` for ephemeral workloads and tests, and `Lsm` for durable on-disk persistence.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

// Ephemeral in-memory database (default)
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

// Durable log-structured database on disk
using var persisted = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DataDirectory = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

## Comparing Storage Modes

| Mode | Durability & Memory | Write Path | Best For |
| --- | --- | --- | --- |
| `StorageMode.InMemory` | Ephemeral; packed chunks in RAM | Direct append into `FlatRowStore` + hash/sorted PK map | Unit tests, caches, game state, low-latency ephemeral datasets |
| `StorageMode.Lsm` | Durable (`commit.wal` + `.sst`); memory-mapped reads | Commit log -> 32 MB MemTable -> background SSTable flush & compaction | Production embedded persistence, write-heavy and local-first apps |
| `StorageMode.Wasm` *(Experimental)* | Browser/OPFS via `WasmStorageEngine` (falls back to in-memory) | Delegated to injected WASM engine | Interactive browser playground (`DataVo.Browser`); not on npm |
| `StorageMode.Custom` | Defined by your `IStorageEngine` | Delegated to `CustomStorageEngine` | Custom storage adapters, encryption wrappers, or test mocks |

## `InMemory` and `FlatRowStore`

`StorageMode.InMemory` is the default when you omit `StorageMode`. Internally, it is backed by `FlatRowStore`, a packed chunked row store that lays out fixed-width cells in contiguous memory buffers instead of allocating one CLR object per row. Integer primary keys use an in-memory hash map for point lookups and maintain a sorted key copy on demand for range scans.

> `FlatRowStore` is an internal engine component, not a `StorageMode` enum value. Always configure `StorageMode.InMemory`.

## `Lsm`

Use `StorageMode.Lsm` for every persistent application. Each statement or transaction commits as one commit-log frame (crash-atomic), tables with a single `INT`/`GUID` primary key are clustered on it, SSTables are memory-mapped, secondary indexes are persisted, and tables can carry out-of-core columnar segments. See [LSM & Durability](/manual/fundamentals/lsm-and-durability) for tuning `LsmStrictFsync` and recovery behavior.

## `Wasm` and `Custom` Hooks

`Wasm` and `Custom` are not storage engines of their own: they plug an `IStorageEngine` you supply into DataVo (the browser playground uses `Custom`). Both accept an `IStorageEngine` instance on `DataVoConfig`:

```csharp
using var customDb = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Custom,
    CustomStorageEngine = myEngine
});
```

For `StorageMode.Wasm`, set `WasmStorageEngine` to an OPFS-backed or browser storage implementation; if left `null`, DataVo falls back to an in-memory backend.

## Directory Sharing and Locking

For `Lsm`, DataVo serves one engine per directory per process:

- **Within one process:** Multiple `DataVoContext` instances and ADO.NET connections targeting the same `DataDirectory` share a single engine until the last handle is disposed. Their configurations must match, or the second open throws `DatabaseInUseException`.
- **Across processes:** DataVo holds a `datavo.lock` file while the directory is open. Opening the same directory from a second process throws `DatabaseInUseException`.

## See Also

- [LSM & Durability](/manual/fundamentals/lsm-and-durability): WAL commit atomicity, strict vs. relaxed fsync, and SSTable layout
- [Configuration Reference](/manual/reference/configuration): Complete list of `DataVoConfig` properties and defaults
- [Transactions & MVCC](/manual/fundamentals/transactions-and-mvcc): Snapshot isolation and concurrency across storage modes
