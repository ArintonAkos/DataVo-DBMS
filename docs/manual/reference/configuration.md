---
title: Configuration Reference
description: Complete DataVoConfig reference covering storage modes, LSM durability, I/O, query planner, vector search, and authorization.
---

# Configuration Reference

`DataVoConfig` configures every aspect of an embedded `DataVoContext`. Most applications only set `StorageMode`, `DataDirectory`, and `LsmStrictFsync`; every other property ships with sensible defaults.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DataDirectory = "./game_data",
    LsmStrictFsync = true
});
```

## Storage & Durability

| Property | Default | Description |
| --- | --- | --- |
| `StorageMode` | `StorageMode.InMemory` | Storage mode: `InMemory` or `Lsm` (`Wasm` and `Custom` plug in an `IStorageEngine` you supply). |
| `DataDirectory` | `null` (`./datavo_lsm_data`) | Directory for persisted `Lsm` files. Guarded by a single-process `datavo.lock`. |
| `CustomStorageEngine` | `null` | Custom `IStorageEngine` instance when `StorageMode = StorageMode.Custom`. |
| `WasmStorageEngine` | `null` | Browser storage engine when `StorageMode = StorageMode.Wasm`. |
| `LockAcquireTimeoutMs` | `30000` | Maximum milliseconds to wait for a lock before failing (`-1` waits indefinitely). |
| `TransactionIdStateFilePath` | `"datavo.txid"` | Persisted MVCC transaction ID watermark file. |

## LSM & I/O

| Property | Default | Description |
| --- | --- | --- |
| `LsmStrictFsync` | `true` | `true` flushes the LSM commit log to stable storage before acknowledging writes; `false` (Relaxed) acknowledges in OS page cache. |
| `LsmReadMode` | `LsmReadMode.Auto` | `Auto` memory-maps SSTables on 64-bit platforms; `PositionalRead` uses standard file reads. |
| `LsmBlockCacheSizeBytes` | `67108864` (64 MiB) | Byte budget of the sharded (32-shard, SIEVE-evicted) block cache that serves positional SSTable reads. Every block read from disk is CRC32C-verified before it is served or cached; warm hits allocate 0 B. `0` disables the cache. Ignored when SSTables are memory-mapped. |
| `ClusteredPrimaryKeys` | `true` | Stores new LSM tables with single-column `INT` or `GUID` primary keys clustered under that key. |
| `EnableZeroAllocCompiledUpdate` | `true` | Lets eligible fixed-width single-row compiled `UPDATE`s skip dictionary materialization: `Lsm` byte-patches the stored row, `InMemory` appends a patched copy and re-points the primary key. `false` forces the dictionary path (A/B measurement, safety fallback). |

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
| `AllowAnonymousSession` | `true` | Permits unauthenticated sessions when `EnableAuthorization` is `true`. |
| `AnonymousRole` | `DatabaseRole.ReadOnly` | Role granted to anonymous sessions. |
| `AuthorizationUsers` | `[]` | List of `DataVoAuthUser` principals provisioned at startup. |

### `DataVoAuthUser`

Each entry in `AuthorizationUsers` is a `DataVoAuthUser` (these are not `DataVoConfig` properties):

| Property | Default | Description |
| --- | --- | --- |
| `Username` | `""` | Unique login name. |
| `Password` | `""` | Plaintext bootstrap password; startup converts it to a hash and salt and clears it. |
| `PasswordHash`, `PasswordSalt` | `""` | Pre-computed credentials, as an alternative to `Password`. |
| `Role` | `DatabaseRole.ReadWrite` | Primary built-in role. |
| `Roles` | `[]` | Additional custom role memberships. |

```csharp
using DataVo.Core;
using DataVo.Core.Runtime.Security;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    EnableAuthorization = true,
    AllowAnonymousSession = false,
    AuthorizationUsers =
    [
        new DataVoAuthUser { Username = "admin", Password = "change-me", Role = DatabaseRole.Admin },
    ],
});
```

## See Also

- [Storage Modes](/manual/fundamentals/storage-modes): Comparing `InMemory` and `Lsm`
- [LSM & Durability](/manual/fundamentals/lsm-and-durability): `LsmStrictFsync` trade-offs and recovery guarantees
- [ADO.NET Provider](/manual/client-interfaces/ado-net): Connection string keys (`StorageMode`, `DataSource`, `Database`)
