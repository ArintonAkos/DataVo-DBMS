# Configuration Reference

> Source route: /manual/reference/configuration
> Source file: manual/reference/configuration.md

DataVo configuration starts with `DataVoConfig`. Most applications only need to choose a storage mode, a path for persisted data, and a durability setting.

For tests and examples, use in-memory storage. It keeps the database inside the process and disappears when the context is disposed.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});
```

For simple persistence, use disk mode and choose a storage path. WAL is enabled by default for disk mode unless explicitly overridden.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./datavo_data",
    WalEnabled = true
});
```

If a disk-mode workload needs stronger flush behavior, enable synchronous disk writes. This is slower, but it is the safer choice when the application cannot tolerate buffered writes being lost on a power failure.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./datavo_data",
    WalEnabled = true,
    SyncDiskWrites = true
});
```

For higher-throughput persisted workloads, use LSM mode. Strict fsync is the conservative durability default and the right starting point for important data.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

Relaxed LSM mode is a performance setting, not the same durability contract. Use it for caches, rebuildable data, or benchmark ceilings.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_cache",
    LsmStrictFsync = false
});
```

Lock acquisition has a timeout. The default is 30 seconds. Use a shorter timeout in tests when you want lock contention to fail quickly.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory,
    LockAcquireTimeoutMs = 5000
});
```

Planner and vector knobs are available for advanced experiments. Most users should keep the defaults until they are investigating a specific query shape.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    EnableVectorPredicateFastPath = true,
    VectorPredicateFastPathMinRows = 128,
    VectorPredicateFastPathCandidateMultiplier = 3
});
```

Volcano execution settings exist but should be treated as advanced planner controls in v0.1. Leave `EnableVolcanoExecution` off unless you are deliberately testing planner behavior.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory,
    EnableVolcanoExecution = false,
    EnableVolcanoSpillGuardrails = true
});
```

## Configuration Support

| Feature | Status | Notes |
| --- | --- | --- |
| `StorageMode.InMemory` | Supported | Best for tests, examples, and ephemeral workloads. |
| `StorageMode.Disk` | Supported | File-backed storage with WAL options. |
| `StorageMode.Lsm` | Supported | WAL-covered LSM storage with strict or relaxed durability. |
| `DiskStoragePath` | Supported | Required for predictable persisted file placement. |
| `WalEnabled` | Supported | Defaults to enabled for disk mode unless explicitly set. |
| `SyncDiskWrites` | Supported | Enables fsync-like behavior for disk writes. |
| `LsmStrictFsync` | Supported | Strict mode waits for WAL durability before acknowledging writes. |
| `LockAcquireTimeoutMs` | Supported | Defaults to 30000 ms; `-1` waits indefinitely. |
| Vector predicate fast-path knobs | Supported | Advanced controls for vector predicate routing. |
| Volcano planner knobs | Planned | Present for limited planner experiments; not the default public execution path. |
