# What Is DataVo?

> Source route: /manual/preface/what-is-datavo
> Source file: manual/preface/what-is-datavo.md

DataVo is an embedded database engine for .NET applications. You add it to a process, create a `DataVoContext`, and run SQL against local storage. There is no separate database service to deploy for the core engine.

The simplest mental model is SQLite-style deployment with a C#-native implementation. DataVo is not trying to look like PostgreSQL in v0.1 Alpha. It is trying to make local storage fast and ergonomic for .NET workloads that benefit from in-process execution, allocation-aware hot paths, vector search, and Native AOT-friendly code.

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

The project is early. DataVo v0.1 Alpha is useful for experiments, local-first applications, game tooling, simulations, vector-search prototypes, and performance research. It should not be presented as a mature RDBMS, a SQLite replacement for production data, or a PostgreSQL-compatible server.

## v0.1 Positioning

| Feature | Status | Notes |
| --- | --- | --- |
| Embedded C# storage engine | Supported | Applications instantiate `DataVoContext` directly inside the host process. |
| In-memory, disk, and LSM storage | Supported | `InMemory`, `Disk`, and `Lsm` are the public launch modes. |
| SQL-style DDL, DML, and queries | Supported | DataVo has its own compact SQL dialect. |
| Vector columns and indexes | Supported | `VECTOR(n)`, `USING HNSW`, `USING FLAT`, `<=>`, and `<->` are documented public features. |
| Source-generated AOT-sensitive paths | Supported | Source-generated and typed paths avoid reflection-heavy execution where possible. |
| Full RDBMS compatibility | Not Supported | DataVo v0.1 is not PostgreSQL, SQLite, or SQL Server compatible. |
| Client/server protocol | Planned | PostgreSQL-wire compatibility would require a future `DataVo.Server`. |
