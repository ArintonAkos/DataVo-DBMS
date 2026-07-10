# v0.1 Alpha Scope

DataVo v0.1 Alpha is a public engine preview. It is meant to show the architecture, performance direction, and core developer experience before the project claims production database maturity.

The supported path starts with direct embedding. Create a context, choose a storage mode, and execute SQL from your application.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE AlphaDemo");
db.Execute("USE AlphaDemo");
db.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Name VARCHAR(80))");
db.Execute("INSERT INTO Events (Id, Name) VALUES (1, 'created')");
```

The alpha also includes persisted storage modes. For durability-sensitive experiments, prefer LSM strict mode so acknowledged writes wait for WAL durability.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./alpha_data",
    LsmStrictFsync = true
});
```

The SQL surface is intentionally documented as a subset. You can create tables, insert rows, update rows, delete rows, query rows, build indexes, and use vector search.

```sql
CREATE TABLE Products (
  Id INT PRIMARY KEY,
  Name VARCHAR(120),
  Price FLOAT,
  Embedding VECTOR(3)
);

INSERT INTO Products (Id, Name, Price, Embedding)
VALUES (1, 'Example', 12.5, '[1,0,0]');

SELECT Id, Name
FROM Products
WHERE Price > 10
ORDER BY Price DESC
LIMIT 10;
```

What makes this an alpha is the boundary around the edges. Package polish, storage-format stability, EF provider completeness, migrations, server tooling, and PostgreSQL compatibility are not finished. The documentation calls those limits out directly so early users can make a practical decision.

## Unity Evaluation Status

Unity is an evaluation target, not a supported runtime in v0.1. The public `DataVo.Core` package contains only a `net10.0` asset. An experimental portable target remains internal until the same final artifact passes a tracked Unity Editor and player proof.

| Environment or boundary | Status | Meaning |
| --- | --- | --- |
| Unity Editor | Unverified until Stage 3 | No tracked Unity project has imported and executed the candidate package yet. |
| Windows x64 IL2CPP | Unverified until Stage 3 | A built and executed IL2CPP player is required; compilation alone is insufficient. |
| Direct Burst calls | Unsupported by design | DataVo uses managed engine types and services and is not a Burst job payload. |
| Job-to-managed batch bridge | Planned proof | A Burst-compatible job may produce POD commands that managed code drains after completion. |
| In-memory mode | Candidate scope | This is the only storage mode included in the first Unity proof. |
| Disk/LSM persistence | Unsupported until separately validated | Do not use DataVo for Unity save data until durability is tested on each advertised platform. |

## v0.1 Scope Summary

| Feature | Status | Notes |
| --- | --- | --- |
| In-process embedded deployment | Supported | The core product shape is library-first, not server-first. |
| Public curated manual | Supported | The manual is the public documentation boundary for v0.1. |
| SQL execution through `DataVoContext` | Supported | Core SQL commands and query forms are documented. |
| Entity Framework bridge | Supported | Useful for basic `DbSet` workflows and query previews with explicit operator limits, not a full provider. |
| Roslyn source generators | Supported | `[DataVoQuery]` supports selected compiled SELECT, INSERT, and UPDATE shapes. |
| NuGet launch packaging | Supported | The public Core package currently contains only `net10.0`; portable/Unity packaging is not supported. |
| Stable storage format | Planned | Alpha storage files may change before a stable release. |
| Production support contract | Not Supported | v0.1 is early software with expected API and behavior changes. |
| Full RDBMS feature set | Not Supported | No stored procedures, triggers, replication, or PostgreSQL-compatible server in v0.1. |
