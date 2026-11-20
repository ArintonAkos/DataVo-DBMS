# What Is DataVo?

> Source route: /manual/preface/what-is-datavo
> Source file: manual/preface/what-is-datavo.md

DataVo is an embedded database engine written in C# for .NET 10. It runs inside your process: you create a `DataVoContext`, choose where the data lives, and run SQL or compiled LINQ against it. There is no server to deploy, no native library to load, and no wire protocol between your code and the storage engine.

The smallest DataVo program creates an in-memory database, defines a table, inserts a row, and reads it back.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE Demo");
db.Execute("USE Demo");

db.Execute("""
CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(80)
);
""");

db.Execute("INSERT INTO Users (Id, Name) VALUES (1, 'Ada')");

var result = db.Execute("SELECT Id, Name FROM Users WHERE Id = 1")[0];
Console.WriteLine(result.Data[0]["Name"]);
```

For persisted data, the same API switches to the log-structured storage engine:

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

## Versions

The latest published NuGet packages are **0.4.0-rc.1** (17 November 2026), which ship both the **v0.3** and **v0.4** milestones described in this manual. The [Roadmap](/manual/preface/roadmap) lists what each milestone added and what v0.5 plans.

## How the engine is put together

DataVo has two storage engines that are under active development, and one legacy engine.

- **InMemory** keeps each table's rows in a packed row store (`FlatRowStore`): fixed-width cells in contiguous buffers rather than one object per row. An integer primary key is a hash map from key to row slot. For key ranges, the map builds a sorted copy of its keys on the first range query and keeps it current with a log of later inserts. Nothing survives the process.
- **LSM** is a log-structured merge tree. A write is appended to the data directory's commit log (`commit.wal`), applied to an arena-backed MemTable, and acknowledged; with `LsmStrictFsync = true` the acknowledgement waits for the log to reach stable storage. At 32 MB a MemTable is frozen and flushed by a background thread to an immutable SSTable, and SSTables are later merged by compaction. Tables whose primary key is a single `INT` or `GUID` column are clustered: rows are stored under their key, so a key lookup or a key range is a seek, not an index probe followed by a row fetch. SSTables are memory-mapped, secondary indexes are themselves keyed SSTables written in the same commit as the rows, and HNSW graphs are saved on close and caught up on open.
- **Disk** is the earlier paged file engine. It is kept so that existing data stays readable and receives no new work. Use LSM for persistent data.

On top of the storage engines sits one SQL implementation. A query is first offered to a set of **typed paths** that read cells directly from stored rows: a single-table scan, a hash join over integer keys, a columnar index, an index-driven join, and a vector-search planner. A typed path either produces exactly the result the general evaluator would, or declines. A query that no typed path accepts runs on the **general path**, which builds a dictionary per row and handles every supported construct, at a cost of kilobytes of allocation per row read. Most of DataVo's performance work is about widening the typed paths so that common queries never fall back. [Query Planner And Fast Paths](/manual/storage-engine/query-planner-fast-paths) lists what each path covers.

Vector columns (`VECTOR(n)`) are ordinary columns. A `FLAT` index compares the query with every stored vector using SIMD kernels; an `HNSW` index searches a navigable small-world graph and switches to an exact scan when a filtered query needs so many candidates that the scan is cheaper. Both answer `ORDER BY v <=> @q LIMIT k` in SQL, with or without filters and joins, so embeddings and the records they describe live in one engine and one transaction log.

```sql
CREATE TABLE Documents (
  Id INT PRIMARY KEY,
  Title VARCHAR(200),
  Embedding VECTOR(1536)
);

CREATE INDEX idx_documents_embedding
ON Documents (Embedding)
USING HNSW;
```

## Ways to call it

| Package | What it gives you |
| --- | --- |
| `DataVo.Core` | The engine and `DataVoContext`: SQL text, prepared statements, typed tables (`DataVoTable<T>`), reactive queries. |
| `DataVo.Generators` | A Roslyn source generator. `[DataVoQuery]` methods and `DataVoTable<T>` lambdas are compiled at build time into code that reads cells from the stored bytes. It has no dependency on Entity Framework Core and is Native AOT compatible. |
| `DataVo.Data` | An ADO.NET provider (`DbConnection`, `DbCommand`, `DbDataReader`), which also makes Dapper work. |
| `DataVo.EntityFrameworkCore` | A bridge that attaches DataVo storage to an EF Core context. It is not a standalone EF provider and has no migrations. |

`DataVo.Core` and `DataVo.Data` build with the trim and AOT analyzers as errors, so an application that uses them can be published with `PublishAot=true`. A browser build (`DataVo.Browser`) runs the engine in WebAssembly for this site's playground; it is not published on npm. [Choosing an Interface](/manual/getting-started/choosing-an-interface) compares the options.

## What DataVo is good at, and what it is not

DataVo was built for local-first .NET applications, game tooling, simulations, and retrieval workloads that keep embeddings next to relational data. On a single machine, with data in the operating system's page cache, the v0.4 engine is faster than SQLite on primary-key and secondary-index lookups, low-cardinality `GROUP BY`, inner and `LEFT` joins on integer keys, aggregates a columnar index can answer, and filtered vector search. SQLite is still faster on `GROUP BY` over a key it can read in index order, on full scans with `OR` or `LIKE`, and on an unindexed `ORDER BY ... LIMIT`; DuckDB is one to three orders of magnitude faster than both on whole-table analytics. [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite) gives the numbers and the reasons.

It is not a client/server database. One process owns a database directory at a time (`datavo.lock` enforces it), there is no network protocol, no replication, no stored procedures or triggers, and transactions do not provide serializable isolation for arbitrary multi-row predicates. Its SQL is its own dialect, not PostgreSQL's or SQLite's. The [SQL compatibility matrix](/manual/sql-language/sql-compatibility), [Limits](/manual/reference/limits) and [Unsupported Features](/manual/reference/unsupported-features) state these boundaries precisely.

## Commercial Add-On: DataVo Sync

While DataVo itself is an MIT-licensed embedded database, teams needing **offline-first bidirectional synchronization with PostgreSQL** can license **DataVo Sync**, a separate proprietary commercial add-on. It connects PostgreSQL logical replication (`pgoutput`) to embedded DataVo clients over WebSocket/HTTP using wire v2 + Brotli, Z-set delta compaction, per-column conflict resolution, and crash-recoverable write-back.

In end-to-end Linux x64 CI benchmarks (20 November 2026, PostgreSQL 17.12, 2 vCPU):

- **100k warm initial sync:** **605 ms** (`InMemory`) and **706–726 ms** (`Lsm`) vs **702–909 ms** (SQLite/JSON) and **1,167 ms** (ElectricSQL `1.8.1`).
- **Wire footprint & allocations:** **45.5 B/row** (8.0× smaller than ElectricSQL's `362.7 B/row`) and **36.4–49.1 MB** client allocations (5.1–6.9× lower than ElectricSQL's `251.4 MB`).
- **Concurrency:** sustains **1,000 concurrent WebSocket clients** (`12,754 rows/s`) on a 2-vCPU VM, with **4.06–5.19 µs** local point lookups and **81–243 ms** offline-burst convergence across PostgreSQL and peers.

For licensing or early-access inquiries, reach out to `@ArintonAkos` on GitHub.
