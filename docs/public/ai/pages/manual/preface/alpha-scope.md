# Release Scope & Boundaries (v0.2 Release Candidate)

> Source route: /manual/preface/alpha-scope
> Source file: manual/preface/alpha-scope.md

DataVo v0.2 Release Candidate (v0.2.0-rc.3) defines the feature-complete verification boundary for the 0.2 release line, backed by 2,358 passing tests across the engine, storage, and client suites. It establishes a clear contract between release-candidate core subsystems and explicit engine boundaries.

The supported path starts with direct embedding. Create a context, choose a storage mode, and execute SQL or typed queries from your .NET 10 application. In-memory mode includes a zero-allocation primary-key point lookup fast path (`56 B` ADO.NET reader wrapper, ~`405 ns`).

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE RcDemo");
db.Execute("USE RcDemo");
db.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Name VARCHAR(80))");
db.Execute("INSERT INTO Events (Id, Name) VALUES (1, 'created')");
```

For persisted workloads, `StorageMode.Lsm` with `LsmStrictFsync = true` is the release-candidate storage engine, using WAL-backed MemTables, SSTables, and clustered primary keys so acknowledged writes wait for WAL durability. Disk paged mode (`StorageMode.Disk`) is legacy and receives no further durability work. See [WAL And Durability](../storage-engine/wal-and-durability.md#known-limitations) for durability notes and legacy disk mode limitations.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./rc_data",
    LsmStrictFsync = true
});
```

The SQL surface is documented as a verified subset. You can create tables, insert rows, update rows, delete rows, query rows, build scalar and vector indexes, and run SIMD-accelerated vector search (`HNSW` and `FLAT` with cosine `<=>` and L2 `<->` distance). Native AOT (`DataVo.Core` + `DataVo.Data`) is locked with trim and AOT analyzers enforced as build errors.

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

## Explicit Boundaries

DataVo v0.2 Release Candidate (v0.2.0-rc.3) enforces strict architectural boundaries so adopters can evaluate the engine with accurate expectations:

- **No arbitrary multi-row serializable MVCC:** Transactions provide session-bound `BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK`, row-level write coordination, and MVCC snapshot visibility, but do not guarantee serializable isolation or phantom-read prevention across arbitrary multi-row predicates.
- **EF Core migrations unsupported:** `DataVo.EntityFrameworkCore` operates as an integration bridge over a host EF provider for bounded `DbSet` and `QueryFromDataVo` workflows, not a standalone EF Core provider with migration scaffolding.
- **Disk paged mode is legacy:** `StorageMode.Disk` is retained only for compatibility and receives no further durability engineering. All persistent workloads should use `StorageMode.Lsm`.
- **WebAssembly is an experimental playground lane:** `DataVo.Browser` powers the interactive documentation playground (`docs/src/DataVoClient.ts`), but is **not** a production-ready target and is **not** published on npm.

## Preview SQL DATE Contract

SQL `DATE` now materializes as `DataVo.Core.DataVoDate` in preview row APIs. `CellValue.AsDate()` and compiled `GetDate` return `DataVoDate`; modern .NET callers retain explicit `FromDateOnly`, `ToDateOnly`, `AsDateOnly`, and `GetDateOnly` adapters. Existing persisted `DATE` bytes are unchanged.

## Unity Evaluation Status

Unity is an evaluation target, not a supported runtime in DataVo v0.2 Release Candidate (v0.2.0-rc.3). The public `DataVo.Core` package contains only a `net10.0` asset. `DataVoEnablePortableTarget=true` creates an experimental local `netstandard2.1` candidate only. A passing .NET-host verifier by itself is not Unity Editor, Mono, IL2CPP, Jobs, or Burst proof.

| Environment or boundary | Status | Meaning |
| --- | --- | --- |
| Unity 6000.5.4f1 Editor | Verified (narrow smoke) | The tracked project imports the generated candidate and its EditMode smoke suite passes. |
| macOS IL2CPP (6000.5.4f1) | Verified (narrow smoke) | A Medium-stripping Player builds, launches, performs the Unity type-binding scenario, writes a passing report, and exits zero. |
| Windows x64 IL2CPP | Unverified | Windows requires its own built-and-executed Player proof. |
| Direct Burst calls | Unsupported by design | DataVo uses managed engine types and services and is not a Burst job payload. |
| Job-to-managed batch bridge | Planned proof | A Burst-compatible job may produce POD commands that managed code drains after completion. |
| In-memory mode | Candidate scope | This is the only storage mode included in the Stage 3 Unity candidate proof. |
| Disk/LSM persistence | Verified (narrow smoke) | Save, close and reopen pass for both modes in the Unity 6000.5.4f1 macOS Editor and a macOS IL2CPP Player. Other platforms and power-loss behaviour are unverified. |

## v0.2 Release Candidate Scope Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Core embedded API (`DataVoContext`) | Release Candidate | Primary in-process embedded interface (`net10.0`). |
| LSM storage with strict fsync | Release Candidate | WAL-backed MemTables, SSTables, and clustered primary keys (`LsmStrictFsync = true`). |
| Zero-alloc in-memory point lookup | Release Candidate | Direct PK fast path (`56 B` reader allocation, ~`405 ns` lookup). |
| SIMD vector search (`HNSW` & `FLAT`) | Release Candidate | `VECTOR(n)` columns with cosine (`<=>`) and Euclidean/L2 (`<->`) operators. |
| Native AOT (`DataVo.Core` + `DataVo.Data`) | Locked & Passing | Trim and AOT analyzers (`IL2026`/`IL3050`) enforced as build errors. |
| ADO.NET & Dapper (`DataVo.Data`) | Release Candidate | Standard `DbConnection`, `DbCommand`, `DbDataReader` provider surface. |
| Roslyn source generators (`DataVo.Generators`) | Release Candidate | `[DataVoQuery]` compiled query plans and typed row mappers. |
| Entity Framework Core bridge | Preview Bridge | Supports bounded `DbSet` and `QueryFromDataVo` workflows; EF migrations are unsupported. |
| Disk paged storage (`StorageMode.Disk`) | Legacy | Retained for compatibility; receives no further durability work. |
| Browser / WebAssembly (`DataVo.Browser`) | Experimental / Playground Lane | Powers the interactive docs playground; not a production target and not published on npm. |
| Arbitrary multi-row serializable MVCC | Not Supported | Snapshot visibility and row-level write locks only; no predicate-range locking. |
| Full RDBMS server features | Not Supported | No stored procedures, triggers, replication, or PostgreSQL wire server in v0.2. |
