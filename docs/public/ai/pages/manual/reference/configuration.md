# Configuration Reference

> Source route: /manual/reference/configuration
> Source file: manual/reference/configuration.md

`DataVoConfig` configures every aspect of an embedded `DataVoContext`. Most applications only set `StorageMode`, `DiskStoragePath`, and `LsmStrictFsync`; every other property ships with sensible defaults.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./game_data",
    LsmStrictFsync = true
});
```

## Storage & Durability

| Property | Default | Description |
| --- | --- | --- |
| `StorageMode` | `StorageMode.InMemory` | Storage backend: `InMemory`, `Lsm`, `Disk` (legacy), `Wasm` (browser playground), or `Custom`. |
| `DiskStoragePath` | `null` | Directory for persisted `Lsm` and `Disk` files. Guarded by a single-process `datavo.lock`. |
| `CustomStorageEngine` | `null` | Custom `IStorageEngine` instance when `StorageMode = StorageMode.Custom`. |
| `WasmStorageEngine` | `null` | Browser storage engine when `StorageMode = StorageMode.Wasm`. |
| `LockAcquireTimeoutMs` | `30000` | Maximum milliseconds to wait for a lock before failing (`-1` waits indefinitely). |
| `WalEnabled` | `true` (`Disk`), `false` (other) | Enables WAL in legacy `Disk` mode. |
| `SyncDiskWrites` | `false` | Forces fsync on legacy `Disk` mode writes. |
| `WalFilePath` | `"datavo.wal"` | Legacy `Disk` WAL file name or path. |
| `WalCheckpointThreshold` | `1000` | Legacy `Disk` WAL entry threshold before checkpointing. |
| `WalCheckpointIntervalMs` | `1000` | Background WAL checkpoint interval (ms) for `IoSchedulerMode.GroupCommit`. |
| `TransactionIdStateFilePath` | `"datavo.txid"` | Persisted MVCC transaction ID watermark file. |
| `CheckpointStateFilePath` | `"datavo.ckpt"` | Persisted binary WAL checkpoint LSN watermark file. |

## LSM & I/O

| Property | Default | Description |
| --- | --- | --- |
| `LsmStrictFsync` | `true` | `true` flushes the LSM commit log to stable storage before acknowledging writes; `false` (Relaxed) acknowledges in OS page cache. |
| `LsmReadMode` | `LsmReadMode.Auto` | `Auto` memory-maps SSTables on 64-bit platforms; `PositionalRead` uses standard file reads. |
| `ClusteredPrimaryKeys` | `true` | Stores new LSM tables with single-column `INT` or `GUID` primary keys clustered under that key. |
| `IoSchedulerMode` | `IoSchedulerMode.Off` | Disk I/O scheduler mode (`Off`, pooled positioned I/O, or `GroupCommit`). |
| `EnableZeroAllocCompiledUpdate` | `true` | Allows eligible fixed-width single-row compiled `UPDATE`s to byte-patch stored rows in place. |

The LSM MemTable flush threshold (32 MB) is an internal engine constant rather than a `DataVoConfig` property.

## Execution & Planner

### Vector & Hybrid Query Routing

| Property | Default | Description |
| --- | --- | --- |
| `EnableVectorPredicateFastPath` | `true` | Uses the vector index for `WHERE v <=> @q < threshold` and filtered distance `ORDER BY`. |
| `VectorPredicateFastPathMinRows` | `128` | Minimum table row count before invoking the vector index fast path. |
| `VectorPredicateFastPathCandidateMultiplier` | `3` | Multiplier from estimated qualifying rows to initial index candidates. |
| `VectorPredicateFastPathMaxTopKRatio` | `0.6` | Falls back to plain filtering when candidate requests exceed this share of the table. |
| `VectorPredicateFastPathMaxTopK` | `20000` | Hard cap on candidates requested from the vector index. |
| `VectorPredicateFastPathMaxExpansionPasses` | `2` | Maximum candidate-expansion retries before switching to a filter-first exact scan. |
| `VectorPredicateFastPathExpansionFactor` | `2` | Candidate growth multiplier per expansion pass. |
| `EnableHybridOrderByAdaptiveInitialTopK` | `true` | Sizes the initial candidate request from estimated filter selectivity (`LIMIT / selectivity`). |

HNSW graph parameters (`m`, `ef_construction`, `ef_search`) are configured per index in SQL. See [Vector Search](/manual/fundamentals/vector-search).

### Experimental Volcano & Telemetry Switches

`EnableVolcanoExecution` (default `false`) and its `Volcano*` spill, external-sort, aggregate-partitioning, and join-feedback properties control an experimental operator pipeline used for planner testing; leave `EnableVolcanoExecution = false` in production.

Telemetry flags (`EnableVectorPredicateFastPathTelemetry`, `EnableHybridRoutingTelemetryCounters`, `EnableHybridRoutingPerQueryTelemetry`, `HybridRoutingTelemetrySnapshotIntervalQueries`) record hybrid vector routing diagnostics for benchmarks.

## Security & Auth

| Property | Default | Description |
| --- | --- | --- |
| `EnableAuthorization` | `false` | Enforces users, roles, and SQL `GRANT`/`REVOKE` checks. |
| `Username`, `Password` | `""` | Credentials for the default session principal (or supply `PasswordHash` + `PasswordSalt`). |
| `Role`, `Roles` | `ReadWrite`, `[]` | Built-in role and custom role names assigned to the session principal. |
| `AllowAnonymousSession` | `true` | Permits unauthenticated sessions when `EnableAuthorization` is `true`. |
| `AnonymousRole` | `DatabaseRole.ReadOnly` | Role granted to anonymous sessions. |
| `AuthorizationUsers` | `[]` | List of `DataVoAuthUser` principals provisioned at startup. |

## See Also

- [Storage Modes](/manual/fundamentals/storage-modes): Comparing `InMemory`, `Lsm`, and legacy `Disk`
- [LSM & Durability](/manual/fundamentals/lsm-and-durability): `LsmStrictFsync` trade-offs and recovery guarantees
- [ADO.NET Provider](/manual/client-interfaces/ado-net): Connection string keys (`StorageMode`, `DataSource`, `WalEnabled`)
