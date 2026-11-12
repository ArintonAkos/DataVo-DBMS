# LSM Mode

> Source route: /manual/storage-engine/lsm-mode
> Source file: manual/storage-engine/lsm-mode.md

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

## Reopening a Database

An LSM directory keeps its schema (`Catalog.xml`), its data (SSTables plus the commit log), and its transaction-id high-water mark. Opening a new context on the same directory restores all of it.

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

Indexes are not stored on disk in LSM mode. They are rebuilt from the durable rows when the engine opens, so they always match the data, even after a crash. The trade-off is that opening a database takes time proportional to its row count (about 0.5–0.8 s for 200,000 rows on a laptop).

## Opening the Same Directory Twice

Contexts and ADO.NET connections in one process that open the same directory share a single engine. The engine stays open until the last of them is disposed. They must use equivalent configuration, otherwise the second open throws `DatabaseInUseException` naming the setting that differs.

A directory can be open in only one process at a time. DataVo holds a `datavo.lock` file while the engine is open, and a second process gets `DatabaseInUseException`.

## LSM Support Summary

The LSM engine provides an arena-backed MemTable, a single commit log per data directory (records are pruned once every table has flushed them; directories written by earlier versions with per-table WAL files are migrated automatically on first open), a 32 MB MemTable flush threshold (an internal v0.1 value, not a public tuning knob), a background flush pipeline that runs outside the foreground write path, and compaction (present, with tuning still in alpha). The on-disk format is **not** guaranteed stable — v0.1 storage files may change before a stable release.
