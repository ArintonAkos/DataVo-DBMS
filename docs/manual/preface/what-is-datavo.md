# What Is DataVo?

DataVo is an embedded database engine for .NET applications. You add it to a process, create a `DataVoContext`, and run SQL against local storage. There is no separate database service to deploy for the core engine.

The simplest mental model is SQLite-style deployment with a C#-native implementation. DataVo is not trying to look like PostgreSQL in v0.2 Release Candidate (`v0.2.0-rc.3`). It is designed to make local storage fast and ergonomic for .NET 10 workloads that benefit from in-process execution, allocation-aware hot paths, vector search, and locked Native AOT compatibility.

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

For persisted data, the same API switches to disk-backed storage. The high-throughput path is `StorageMode.Lsm`, which uses WAL-covered MemTables and SSTables.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

DataVo also has SQL support for vector columns. That makes it possible to keep application records and embeddings in the same embedded engine.

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

DataVo v0.2 Release Candidate (`v0.2.0-rc.3`) is built for local-first .NET 10 applications, game tooling, simulations, vector-search workloads, and high-throughput embedded persistence. It should not be presented as a client/server RDBMS or a PostgreSQL-compatible server.

## v0.2 Positioning

In v0.2 (`v0.2.0-rc.3`), DataVo provides an embedded C# storage engine (`DataVoContext` and `DataVoConnection` running in-process), `InMemory` and `Lsm` storage (`Disk` paged mode is legacy), its own compact SQL dialect for DDL/DML/queries, zero-allocation in-memory point lookups (`56 B`, ~`405 ns`), and SIMD vector columns and indexes (`VECTOR(n)`, `USING HNSW`, `USING FLAT`, `<=>`, `<->`). Native AOT (`DataVo.Core` + `DataVo.Data`) is locked with trim and AOT analyzers enforced as build errors. DataVo is deliberately **not** a full client/server RDBMS — it is not PostgreSQL, SQLite, or SQL Server wire-compatible — and its client/server tooling bridge (`DataVo.Server`) is planned for the v0.3+ roadmap. See the [SQL compatibility matrix](/manual/sql-language/sql-compatibility) for the authoritative feature status.
