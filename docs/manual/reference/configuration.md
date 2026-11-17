# Configuration Reference

`DataVoConfig` holds every setting. Most applications set three things: the storage mode, where persisted data lives, and the durability contract. Everything else has a default that is right unless you are investigating a specific behaviour.

## Choosing storage and durability

In-memory storage keeps the database inside the process; it disappears when the last context is disposed. Use it for tests, caches, and data you rebuild at startup.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});
```

For persisted data use the LSM engine. With `LsmStrictFsync = true`, the default, a commit is acknowledged only after its commit-log record has been flushed to stable storage, so it survives power loss. Each commit then costs one device flush, which is about 4 ms on the Apple M4 Pro measured on the [results page](/manual/performance/benchmarks#durability-what-an-fsync-costs) and far less on drives with power-loss protection.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

`LsmStrictFsync = false` (Relaxed) acknowledges once the record is in the operating system's file cache. A process crash loses nothing that was acknowledged; a power failure or kernel crash can lose the most recent commits. Use it for caches and rebuildable data, and when comparing with SQLite's `synchronous=NORMAL`, which makes the same trade.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_cache",
    LsmStrictFsync = false
});
```

`StorageMode.Disk`, the earlier paged engine, is legacy. It still opens existing data, with `WalEnabled` (on by default) and `SyncDiskWrites` controlling its log and flushes, but it receives no new work. Do not choose it for new data.

## Storage settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `StorageMode` | `InMemory` | `InMemory`, `Lsm`, `Disk` (legacy), `Wasm` (browser playground), or `Custom` with `CustomStorageEngine`. |
| `DiskStoragePath` | none | The database directory for `Lsm` and `Disk`. One process may open it at a time (`datavo.lock`). |
| `LsmStrictFsync` | `true` | Flush the commit log to stable storage before acknowledging a commit. |
| `LsmReadMode` | `Auto` | `Auto` memory-maps SSTables where supported (64-bit, not the browser); `PositionalRead` uses file reads. Memory mapping avoids a copy and a system call per read, and lets a table exceed the managed heap. |
| `ClusteredPrimaryKeys` | `true` | New LSM tables whose primary key is one `INT` or `GUID` column store rows under that key. Decided per table at `CREATE TABLE`. |
| `LockAcquireTimeoutMs` | `30000` | How long a lock acquisition waits before failing; `-1` waits indefinitely. Shorten it in tests that expect contention. |
| `EnableZeroAllocCompiledUpdate` | `true` | Lets narrow compiled single-row updates patch stored bytes in place. |
| `WalEnabled`, `SyncDiskWrites`, `WalFilePath`, `WalCheckpointThreshold`, `WalCheckpointIntervalMs` | see source | Legacy `Disk` mode only. |

The LSM MemTable flush threshold (32 MB) is an internal constant, not a setting.

## Vector search settings

These control how a filtered nearest-neighbour query or a distance threshold in `WHERE` uses a vector index. The defaults were chosen on the hybrid-vector benchmark; change them only after measuring.

| Setting | Default | Meaning |
| --- | --- | --- |
| `EnableVectorPredicateFastPath` | `true` | Use the vector index for `WHERE v <=> @q < threshold` and filtered `ORDER BY` distance queries. |
| `VectorPredicateFastPathMinRows` | `128` | Below this table size a distance predicate is evaluated without the index. |
| `VectorPredicateFastPathCandidateMultiplier` | `3` | Multiplier from the estimated number of qualifying rows to the number of candidates requested from the index. |
| `VectorPredicateFastPathMaxTopKRatio` | `0.6` | When the candidate request would exceed this share of the table, the predicate is evaluated by plain filtering instead. |
| `VectorPredicateFastPathMaxTopK` | `20000` | Absolute cap on candidates requested from the index. |
| `VectorPredicateFastPathMaxExpansionPasses` | `2` | How many times a filtered search may enlarge its candidate request before it switches to a filter-first exact scan. |
| `VectorPredicateFastPathExpansionFactor` | `2` | Growth of the candidate request per pass. |
| `EnableHybridOrderByAdaptiveInitialTopK` | `true` | Size the first candidate request from the filter's estimated selectivity (about `LIMIT / selectivity`). |

HNSW graph parameters (`m`, `ef_construction`, `ef_search`, the diversity heuristic) are set per index in SQL; see [Vector Search and Indexes](/manual/sql-language/vector-search-syntax).

## Security settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `EnableAuthorization` | `false` | Enforce users, roles and grants. |
| `Username`, `Password` (or `PasswordHash` + `PasswordSalt`), `Role`, `Roles` | empty | The principal a context opens as. |
| `AllowAnonymousSession`, `AnonymousRole` | `true`, `ReadOnly` | What an unauthenticated session may do when authorization is on. |
| `AuthorizationUsers` | empty | Users to provision at open. |

## Planner experiments

The `Volcano*` settings (`EnableVolcanoExecution` and its spill, external-sort, aggregate-partitioning and join-feedback options) control an alternative operator pipeline used for planner experiments and spill tests. It is off by default and is not used by any benchmark in this manual. Leave it off unless you are testing the planner itself.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory,
    EnableVolcanoExecution = false
});
```

Telemetry switches (`EnableVectorPredicateFastPathTelemetry`, `EnableHybridRoutingTelemetryCounters`, `EnableHybridRoutingPerQueryTelemetry`, `HybridRoutingTelemetrySnapshotIntervalQueries`) record which route each vector query took; the benchmarks read them.
