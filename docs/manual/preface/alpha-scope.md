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

The alpha also includes persisted storage modes. For durability-sensitive experiments, use LSM strict mode so acknowledged writes wait for WAL durability. Disk mode is legacy and receives no further durability work. Both modes currently share one known crash limitation for `UPDATE`, described in [WAL And Durability](../storage-engine/wal-and-durability.md#known-limitations).

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

## Preview SQL DATE Contract

SQL `DATE` now materializes as `DataVo.Core.DataVoDate` in preview row APIs. `CellValue.AsDate()` and compiled `GetDate` return `DataVoDate`; modern .NET callers retain explicit `FromDateOnly`, `ToDateOnly`, `AsDateOnly`, and `GetDateOnly` adapters. Existing persisted `DATE` bytes are unchanged.

## Unity Evaluation Status

Unity is an evaluation target, not a supported runtime in v0.1. The public `DataVo.Core` package contains only a `net10.0` asset. `DataVoEnablePortableTarget=true` creates an experimental local `netstandard2.1` candidate only. A passing .NET-host verifier by itself is not Unity Editor, Mono, IL2CPP, Jobs, or Burst proof.

| Environment or boundary | Status | Meaning |
| --- | --- | --- |
| Unity 6000.5.4f1 Editor | Verified (narrow smoke) | The tracked project imports the generated candidate and its EditMode smoke suite passes. |
| macOS IL2CPP (6000.5.4f1) | Verified (narrow smoke) | A Medium-stripping Player builds, launches, performs the Unity type-binding scenario, writes a passing report, and exits zero. |
| Windows x64 IL2CPP | Unverified | Windows requires its own built-and-executed Player proof. |
| Direct Burst calls | Unsupported by design | DataVo uses managed engine types and services and is not a Burst job payload. |
| Job-to-managed batch bridge | Planned proof | A Burst-compatible job may produce POD commands that managed code drains after completion. |
| In-memory mode | Candidate scope | This is the only storage mode included in the Stage 3 Unity candidate proof. |
| Disk/LSM persistence | Verified (narrow smoke) | Save, close and reopen pass for both modes in the Unity 6000.5.4f1 macOS Editor and a macOS IL2CPP Player. Other platforms and power-loss behaviour are unverified. |

## v0.1 Scope Summary

| Feature | Status | Notes |
| --- | --- | --- |
| In-process embedded deployment | Supported | The core product shape is library-first, not server-first. |
| Public curated manual | Supported | The manual is the public documentation boundary for v0.1. |
| SQL execution through `DataVoContext` | Supported | Core SQL commands and query forms are documented. |
| Entity Framework bridge | Supported | Useful for basic `DbSet` workflows and query previews with explicit operator limits, not a full provider. |
| Roslyn source generators | Supported | `[DataVoQuery]` supports selected compiled SELECT, INSERT, and UPDATE shapes. |
| NuGet launch packaging | Supported | The public Core package remains `net10.0`-only; the opt-in portable target is an experimental local candidate, not a published or supported Unity asset. |
| Stable storage format | Planned | Alpha storage files may change before a stable release. |
| Production support contract | Not Supported | v0.1 is early software with expected API and behavior changes. |
| Full RDBMS feature set | Not Supported | No stored procedures, triggers, replication, or PostgreSQL-compatible server in v0.1. |
