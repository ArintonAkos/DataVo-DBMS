# LSM Mode

`StorageMode.Lsm` is DataVo's high-throughput persisted backend. It is the mode behind the headline write-path benchmark results.

Start with strict durability.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

Writes first go to the data directory's commit log (`commit.wal`, one record per statement or transaction) and then to each table's active MemTable. The active MemTable is designed around arena-backed storage so steady-state write allocation stays low.

```sql
CREATE TABLE Events (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  Value INT
);

INSERT INTO Events (Id, Name, Value)
VALUES (1, 'started', 100);
```

When the active MemTable crosses 32 MB, DataVo freezes that generation, swaps in a new MemTable, and lets a background worker flush the frozen generation to SSTables.

```text
write
  -> append commit-log record (one per statement or transaction)
  -> active MemTable
  -> freeze at 32 MB
  -> background SSTable flush
  -> manifest edit
  -> commit-log records covered by every table's SSTables are pruned
```

Relaxed LSM mode uses the same broad storage shape, but it does not wait for synchronous WAL fsync before acknowledging writes.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_relaxed",
    LsmStrictFsync = false
});
```

## What SSTables Do

An SSTable (Sorted String Table, `.sst`) is an immutable, page-aligned file produced when a 32 MB MemTable flushes or when background compaction merges older files:

- **Fast Primary-Key Point Lookups:** Entries are stored sorted by clustered `RowKey` inside 4 KiB page-aligned data blocks with a sparse block index and a per-file blocked Bloom filter. Point lookups avoid absent files in memory and read at most one block on a hit (`~223–236 ns` on 100K rows).
- **Predictable Sequential I/O & Crash Safety:** Because SSTables are never modified in place, writes go sequentially to a temp file, `fsync` before atomic rename, and become visible in one manifest version edit alongside commit-log pruning.
- **Out-of-Core Columnar Segments (v8 format):** On tables with a `[Columnar]` index (`CREATE INDEX ... USING COLUMNAR`), flush and compaction also write 4,096-row aligned columnar chunks, NULL/NaN masks, deleted-key (tombstone) runs, and per-chunk min/max zone maps directly inside each `.sst` file, without touching row blocks or point-lookup speed.

## Reopening a Database

An LSM directory keeps its schema (`Catalog.xml`), its data (SSTables plus the commit log), any active `COLUMNAR` index spec, and its transaction-id high-water mark. Opening a new context on the same directory restores all of it.

```csharp
using (var db = new DataVoContext(config))
{
    db.Execute("CREATE DATABASE App");
    db.Execute("USE App");
    db.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Name VARCHAR(80))");
    db.Execute("INSERT INTO Events (Id, Name) VALUES (1, 'started')");
}

using var reopened = new DataVoContext(config);
reopened.Execute("USE App");
reopened.Execute("SELECT * FROM Events WHERE Id = 1"); // returns the row
```

What happens to each kind of index on open:

- **Primary-key and secondary B-tree indexes** are stored as their own key spaces ("index tables") of `(key, row id)` entries in keyed SSTables under the table's `.idx` directory. An index change is staged in the same write unit as the row change, so it reaches the commit log in the same record, and rows and indexes recover together after a crash. Opening a database does not rebuild them. A clustered table's primary key is not stored at all: the rows are keyed by it.
- **HNSW graphs** are written to a snapshot file on a clean close, stamped with the commit-log sequence number they reflect. On open, the snapshot is loaded and only rows written after that number are inserted into or removed from the graph; if more than half the rows changed, or the snapshot is missing or fails its checksum, the graph is rebuilt from the rows. After a crash the graph is therefore rebuilt or caught up, never trusted beyond what the rows say.
- **FLAT vector indexes** are rebuilt from the rows on open. They hold only the vectors, so the rebuild is a sequential read.
- **Columnar indexes (`[Columnar]` / `USING COLUMNAR`)** live inside the SSTables themselves (format version 8). Only the unflushed tail stays in memory, so nothing is rebuilt: covered filters and aggregates stream 4,096-row chunks from the memory-mapped `.sst` segments and skip chunks whose min/max zone maps rule out the predicate. Opening a database and answering a first `SUM` over 1,000,000 flushed rows took 13.7 ms with the files in the OS page cache.

## Opening the Same Directory Twice

Contexts and ADO.NET connections in one process that open the same directory share a single engine. The engine stays open until the last of them is disposed. They must use equivalent configuration, otherwise the second open throws `DatabaseInUseException` naming the setting that differs.

A directory can be open in only one process at a time. DataVo holds a `datavo.lock` file while the engine is open, and a second process gets `DatabaseInUseException`.

## LSM Support Summary

The LSM engine provides an arena-backed MemTable, clustered `INT` and `GUID` primary keys, a single commit log per data directory (records are pruned once every table has flushed them; directories written by earlier versions with per-table WAL files are migrated automatically on first open), a 32 MB MemTable flush threshold (an internal constant, not a public tuning knob), a background flush pipeline that runs outside the foreground write path, background SSTable compaction, memory-mapped SSTable reads (`LsmReadMode.Auto`; `PositionalRead` uses file reads instead), secondary indexes persisted as index tables in the same commit as their rows, HNSW graph snapshots caught up from the commit log on open, and persistent format-v8 columnar SSTable segments with per-chunk zone-map pruning. The on-disk format does not yet carry a long-term cross-version compatibility guarantee prior to 1.0.
