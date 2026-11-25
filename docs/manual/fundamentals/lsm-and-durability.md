---
title: LSM & Durability
description: How DataVo's LSM storage engine handles WAL crash atomicity, strict vs. relaxed fsync, clustered primary keys, and reopening directories.
---

# LSM & Durability

`StorageMode.Lsm` is DataVo's durable storage engine, built on a log-structured merge tree with a single directory-wide commit log (`commit.wal`), arena-backed MemTables, and memory-mapped immutable SSTables.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true,          // default: wait for stable storage on commit
    ClusteredPrimaryKeys = true     // default: store single INT/GUID PK tables clustered
});

db.Execute("CREATE DATABASE App; USE App;");
db.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR, score INT)");
db.Execute("INSERT INTO players (id, name, score) VALUES (1, 'Ada', 250)");
```

## Write Path and SSTables

Every statement (or explicit `COMMIT`) appends one checksummed record to `commit.wal`, then applies mutations to each table's arena-backed active MemTable. When a MemTable reaches 32 MB, DataVo freezes it, swaps in a fresh MemTable, and flushes the frozen generation on a background thread into an immutable `.sst` file.

<div class="sketch-diagram">
  <img class="light-only" src="/manual/fig-02-lsm-write-path-light.svg" alt="LSM write path from commit.wal to MemTable, background SSTable flush, manifest edit, and compaction" />
  <img class="dark-only" src="/manual/fig-02-lsm-write-path-dark.svg" alt="LSM write path from commit.wal to MemTable, background SSTable flush, manifest edit, and compaction" />
</div>

Each SSTable is written sequentially to a temporary file, fsynced, atomically renamed, and published in one manifest edit before older `commit.wal` records are pruned:

- **4 KiB data blocks + Bloom filters:** Sorted entries with a sparse block index and per-file blocked Bloom filter let point lookups skip absent files and touch at most one block on a hit.
- **Memory-mapped reads (`LsmReadMode`):** `LsmReadMode.Auto` (default) memory-maps `.sst` files on 64-bit platforms so reads hit the OS page cache with no syscall per lookup. Use `LsmReadMode.PositionalRead` to force standard file reads.
- **Positional reads + block cache (`LsmBlockCacheSizeBytes`):** with `LsmReadMode.PositionalRead`, data blocks are read with `RandomAccess.Read` into a sharded user-space block cache (32 shards, SIEVE eviction, 64 MiB by default). Every physical read verifies the block's CRC32C before the bytes are served or cached, and a truncated or damaged file surfaces as a managed `InvalidDataException` on the reading thread, never as a `SIGBUS`. Bytes that rot on disk after a block was evicted are caught on the next read. Warm hits allocate nothing.

  | 200,000-row SSTable, Apple M4 Pro | Point lookup | Full scan | Engine `ReadRow` | Allocated per lookup |
  | --- | ---: | ---: | ---: | ---: |
  | `Auto` (mmap) | 179 ns | 1.74 ms | 172 ns | 0 B |
  | `PositionalRead`, no cache | 5,963 ns | 4.47 ms | 6,074 ns | 0 B |
  | `PositionalRead` + 64 MiB block cache | 204 ns | 1.76 ms | 203 ns | 0 B |

  Measured with `benchmarks/DataVo.Benchmarks` (`--filter '*LsmReadMode*' --job short`); `ReadRow` includes its 64 B row copy. Choose `PositionalRead` when you want checksummed reads and clean I/O errors at near-mmap speed (network or removable volumes, files other processes might truncate); keep `Auto` for the last ~15 % of point-read latency.
- **Out-of-core columnar segments (v8):** Tables with a `USING COLUMNAR` (`[Columnar]`) index store 4,096-row columnar chunks, NULL/NaN masks, tombstone runs, and per-chunk min/max zone maps inside each `.sst` file.

## Durability Settings

| Setting | Default | Behavior | When to Use |
| --- | --- | --- | --- |
| `LsmStrictFsync = true` | Yes | Forces `commit.wal` to stable storage before acknowledging writes; concurrent commits share fsyncs | Production data that must survive power loss or OS crash |
| `LsmStrictFsync = false` | No | Acknowledges after OS-buffered WAL append; recent writes can be lost on power/kernel failure | Caches, rebuildable data, and throughput ceiling benchmarks |
| `ClusteredPrimaryKeys` | `true` | Stores single `INT` or `GUID` PK tables directly under their key (no separate PK index) | Keep `true` for fast PK seeks and range scans (set at `CREATE TABLE`) |

> When reporting or comparing write throughput, always pair the number with its durability setting (for example, `LSM Strict` vs. `LSM Relaxed`).

## Crash Atomicity and Visibility

In `StorageMode.Lsm`, every auto-commit statement and every explicit transaction `COMMIT` is written to `commit.wal` as a **single checksummed record**:

- **All-or-nothing recovery:** On startup after a crash, recovery replays each complete record atomically and ignores any truncated tail. Multi-row `UPDATE`s and multi-table transactions are never half-applied.
- **Durable visibility:** Row changes, secondary index updates, and reactive notifications become visible to other sessions only after the commit record is durable. Write locks are held until publish, and any failed statement writes no record and leaves no partial state.

## Reopening a Directory

Closing and reopening a `DataVoContext` on the same `DiskStoragePath` restores the catalog (`Catalog.xml`), SSTables, commit log, and transaction-ID watermark:

```csharp
var config = new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data"
};

using (var db = new DataVoContext(config))
{
    db.Execute("CREATE DATABASE App; USE App;");
    db.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR)");
    db.Execute("INSERT INTO players VALUES (1, 'Ada')");
}

using var reopened = new DataVoContext(config);
reopened.Execute("USE App");
var row = reopened.Execute("SELECT name FROM players WHERE id = 1")[0].Data[0]; // "Ada"
```

### Index Recovery on Open

| Index Type | Behavior on Reopen |
| --- | --- |
| **Clustered PK** (`INT`/`GUID`) | Stored directly as the SSTable row key; zero rebuild |
| **B-tree secondary indexes** | Stored as keyed SSTables under `.idx` and updated in the same commit record; zero rebuild |
| **`USING COLUMNAR`** | Stored inside `.sst` (v8 segments); only the unflushed tail stays in memory; zero rebuild |
| **`USING HNSW`** | Loads snapshot written on clean close and catches up from `commit.wal`; rebuilds if missing, corrupt, or >50% changed |
| **`USING FLAT`** | Rebuilt sequentially from table rows on open |

An LSM directory is owned by one process at a time via `datavo.lock`, while multiple contexts inside the same process share one engine (requiring identical settings). Before v1.0 the on-disk format can change between releases, and older directories are not upgraded: recreate them from your source data when you move to a new pre-release version.

## See Also

- [Storage Modes](/manual/fundamentals/storage-modes): The two storage modes, `InMemory` and `Lsm`, and the `Wasm`/`Custom` hooks
- [Transactions & MVCC](/manual/fundamentals/transactions-and-mvcc): Snapshot isolation and atomic multi-statement commits
- [Configuration Reference](/manual/reference/configuration): Full `DataVoConfig` reference
