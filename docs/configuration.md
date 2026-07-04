# Configuration

DataVo runtime behavior is controlled by `DataVoConfig` in `DataVo.Core.StorageEngine.Config`.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true,
    EnableZeroAllocCompiledUpdate = true
});
```

For v0.1 Alpha, the recommended server-side launch modes are `InMemory`, `Disk`, and `Lsm`. The enum has additional integration modes for WASM/browser storage and custom storage injection.

## Storage Modes

| Mode | Backend | Default paths / requirements | Best fit |
| --- | --- | --- | --- |
| `InMemory` | `InMemoryStorageBackend` | No path. | Tests, demos, ephemeral workloads, and benchmarks that isolate engine overhead from disk I/O. |
| `Disk` | `DiskStorageBackend` | `DiskStoragePath`, defaulting to `./datavo_data`. | Local persisted tables with optional WAL and optional synchronous disk flushes. |
| `Lsm` | `LsmStorageBackend` | `DiskStoragePath`, defaulting to `./datavo_lsm_data`. | Append-oriented persisted workloads using WAL-covered MemTables, SSTables, manifests, and compaction. |
| `Wasm` | `WasmStorageBackend` | `WasmStorageEngine`; falls back to memory if null. | Browser/WASM scenarios, intended for OPFS-capable engines. |
| `Custom` | User-provided `IStorageEngine` | `CustomStorageEngine` is required. | Embedding DataVo over an external or test-specific storage implementation. |

## Durability Modes

### Disk Mode

`StorageMode.Disk` writes table files under `DiskStoragePath` or `./datavo_data`.

| Setting | Default | Effect |
| --- | ---: | --- |
| `WalEnabled` | `true` when `StorageMode == Disk` | Enables write-ahead-log recovery for disk mode. If explicitly set, that value overrides the storage-mode-derived default. |
| `WalFilePath` | `datavo.wal` | Relative paths resolve under `DiskStoragePath`; absolute paths are used as-is. |
| `SyncDiskWrites` | `false` | Row appends, tombstones, and compaction are flushed to the OS cache but not forced to physical storage. This is usually process-crash safe but not power-loss safe. |
| `SyncDiskWrites = true` | opt-in | Forces row appends, tombstones, and compaction with fsync-like behavior. The code comments compare this posture to SQLite `synchronous=FULL`. |
| `WalCheckpointThreshold` | `1000` | Number of WAL entries that can accumulate before checkpoint cleanup is considered. |
| `WalCheckpointIntervalMs` | `1000` | Background checkpoint interval used by `IoSchedulerMode.GroupCommit`. |
| `TransactionIdStateFilePath` | `datavo.txid` | Persists the MVCC transaction-id high-water mark. |
| `CheckpointStateFilePath` | `datavo.ckpt` | Persists the binary WAL checkpoint LSN watermark. |

### LSM Mode

`StorageMode.Lsm` uses the LSM storage engine and maps `LsmStrictFsync` to the internal WAL durability mode.

| Setting | Internal mode | Meaning |
| --- | --- | --- |
| `LsmStrictFsync = true` | `LsmWalDurabilityMode.StrictFsync` | Append the WAL mutation and synchronously force it to stable storage before acknowledging the write. This is the production durability default. |
| `LsmStrictFsync = false` | `LsmWalDurabilityMode.RelaxedOsBuffer` | Append the WAL mutation to the OS buffer without a synchronous durable flush. This is much faster in the benchmark harness, but recent writes can be lost on power or kernel failure. |

The benchmark harness names these modes `DataVo (LSM Production)` and `DataVo (LSM Relaxed)`. Do not present relaxed-mode throughput as equivalent to strict durability.

### I/O Scheduler

| `IoSchedulerMode` | Meaning |
| --- | --- |
| `Off` | Legacy synchronous disk path. This is the default. |
| `PoolingOnly` | Reuses file handles and positioned I/O while keeping legacy synchronous locking. |
| `GroupCommit` | Reserved for the WAL-as-commit-point implementation and related checkpoint interval settings. |

## LSM MemTable Flushes

The active LSM MemTable rotates into the background flush pipeline when it crosses 32 MB:

```csharp
internal long FlushThresholdBytes { get; set; } = 32L << 20;
```

The crossing writer freezes the current MemTable, swaps in a new active MemTable and WAL segment, then hands the frozen generation to a single background worker. Writers normally continue against the new generation; they backpressure only when the frozen-generation queue reaches its cap.

Each generation owns a WAL segment. The segment is removed only after the generation's SSTable and manifest edit are durable, so recovery can replay surviving WAL segments in order.

## Authorization

| Setting | Default | Effect |
| --- | ---: | --- |
| `EnableAuthorization` | `false` | Enables runtime authorization checks. |
| `AllowAnonymousSession` | `true` | Allows sessions without explicit login. |
| `AnonymousRole` | `DatabaseRole.ReadOnly` | Role assigned to anonymous sessions. |
| `AuthorizationUsers` | empty list | Bootstrap credential entries consumed by `AuthenticateSession`. Plaintext passwords are converted to hash+salt at runtime initialization. |

## Locking

| Setting | Default | Effect |
| --- | ---: | --- |
| `LockAcquireTimeoutMs` | `30000` | Lock acquisition timeout in milliseconds. Use `-1` for infinite wait. |

## Compiled Update Hot Path

| Setting | Default | Effect |
| --- | ---: | --- |
| `EnableZeroAllocCompiledUpdate` | `true` | Lets eligible fixed-width single-row compiled `UPDATE` plans use the byte-patch fast path in `Disk` or `Lsm` mode. Disable it for A/B benchmarking or as a safety fallback. |

Eligibility is intentionally narrow: the update must resolve through an integer primary-key fast lane, storage must be `Disk` or `Lsm`, reactive change capture must be disabled, and each assigned cell must be fixed-width and non-null for byte patching. Otherwise DataVo falls back to the dictionary/materialization path.

## Volcano Planner Knobs

`EnableVolcanoExecution` is off by default for compatibility. The spill and feedback settings are present so large plan shapes can be guarded before pushing work into streaming Volcano operators.

| Setting | Default | Effect |
| --- | ---: | --- |
| `EnableVolcanoExecution` | `false` | Enables eligible Volcano-style streaming operators. |
| `EnableVolcanoSpillGuardrails` | `true` | Prevents large estimated intermediates from being pushed into in-memory Volcano sort/aggregate paths. |
| `VolcanoSortSpillThresholdRows` | `20000` | Estimated-row threshold for sort pushdown. `<= 0` disables this guardrail. |
| `VolcanoAggregateSpillThresholdRows` | `20000` | Estimated-row threshold for aggregate pushdown. `<= 0` disables this guardrail. |
| `VolcanoNestedLoopJoinThresholdRows` | `128` | Right-side row-count threshold for nested-loop inner joins; above this, hash join is preferred. |
| `EnableVolcanoExternalSortSpill` | `false` | Allows sort operators to spill to temporary runs and merge them. |
| `VolcanoExternalSortThresholdRows` | `50000` | Row threshold for external sort run generation. |
| `VolcanoExternalSortRunSizeRows` | `5000` | Run size used during external sort generation. |
| `VolcanoExternalSortTempDirectory` | `null` | Optional temp directory; null or empty uses the process temp directory. |
| `EnableVolcanoExternalAggregateSpill` | `false` | Allows hash aggregate to spill to partition files and merge/reduce them. |
| `VolcanoExternalAggregateThresholdRows` | `50000` | Row threshold for external aggregate spill. |
| `VolcanoExternalAggregatePartitionCount` | `16` | Hash partition count for aggregate spill. |
| `VolcanoExternalAggregateTempDirectory` | `null` | Optional aggregate spill temp directory. |
| `VolcanoExternalAggregateAdaptivePartitioning` | `true` | Adapts partition count based on observed volume. |
| `VolcanoExternalAggregateTargetRowsPerPartition` | `4096` | Target rows per partition for adaptive partitioning. |
| `VolcanoExternalAggregateMaxPartitionCount` | `128` | Maximum adaptive aggregate partition count. |
| `EnableVolcanoJoinCardinalityFeedback` | `true` | Learns join-cardinality feedback in memory. |
| `EnableVolcanoJoinCardinalityFeedbackPersistence` | `false` | Persists learned join feedback across restarts when enabled. |
| `VolcanoJoinCardinalityFeedbackPersistenceFile` | `null` | Optional persistence file path. |
| `VolcanoJoinCardinalityFeedbackMaxEntries` | `4096` | Maximum feedback entries retained. |

## Vector Predicate And Hybrid Routing Knobs

These settings control the planner's vector predicate prefilter and hybrid route telemetry.

| Setting | Default | Effect |
| --- | ---: | --- |
| `EnableVectorPredicateFastPath` | `true` | Lets selected vector distance predicates use an ANN prefilter path. |
| `VectorPredicateFastPathMinRows` | `128` | Minimum base-table rows before vector fast path is considered. |
| `VectorPredicateFastPathMaxTopKRatio` | `0.6` | Rejects fast path when computed top-K exceeds this fraction of the table. |
| `VectorPredicateFastPathCandidateMultiplier` | `3` | Expands estimated qualifying rows into ANN candidate top-K. |
| `VectorPredicateFastPathMaxTopK` | `20000` | Absolute cap for computed ANN candidate top-K. |
| `EnableVectorPredicateFastPathTelemetry` | `true` | Logs vector fast-path accept/reject decisions. |
| `VectorPredicateFastPathMaxExpansionPasses` | `2` | Extra candidate expansion passes when `LIMIT` is present and post-filtering returns too few rows. |
| `VectorPredicateFastPathExpansionFactor` | `2` | Geometric growth factor for expansion passes. |
| `EnableHybridRoutingTelemetryCounters` | `true` | Tracks aggregate hybrid vector route counters. |
| `EnableHybridRoutingPerQueryTelemetry` | `true` | Emits per-query hybrid routing telemetry. |
| `HybridRoutingTelemetrySnapshotIntervalQueries` | `25` | Emits periodic aggregate telemetry snapshots; `<= 0` disables periodic snapshots. |
| `EnableHybridOrderByAdaptiveInitialTopK` | `true` | Uses selectivity-informed initial top-K sizing for hybrid `ORDER BY` routes. |

## Path Resolution

Relative WAL, transaction-id, and checkpoint paths resolve under `DiskStoragePath` only when `StorageMode == Disk`; otherwise they resolve under the current process directory. Absolute paths are used unchanged.

```csharp
var config = new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./data",
    WalFilePath = "datavo.wal"
};

string walPath = config.ResolveWalFilePath(); // ./data/datavo.wal
```

## Recommended Starting Points

For local development, use `InMemory` until you need persistence. For persisted alpha experiments, use `Lsm` with `LsmStrictFsync = true` unless the workload explicitly tolerates recent-write loss. Use `LsmStrictFsync = false` only for benchmarking, caches, or rebuildable data.
