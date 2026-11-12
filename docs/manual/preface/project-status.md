# Project Status & Stability

**Current release: v0.2.0-rc.3 (Release Candidate).**

DataVo is a C#-native embedded database engine. The 0.2 line is feature-complete for its documented scope and is in release verification: benchmark runs, crash-recovery testing and package publishing checks are ongoing before a stable tag. This page is the single place that states what that means in practice.

## What "Release Candidate" means here

- The documented API surface and SQL dialect are the surface being verified for 0.2. Changes to it between release candidates are called out in the release notes.
- Behavior outside the documented surface is not covered by that expectation. [Limits](/manual/reference/limits) and [Unsupported Features](/manual/reference/unsupported-features) list those boundaries.
- Release candidates are published to nuget.org as prerelease packages. Add `--prerelease` when installing. See [Installation & Packages](/manual/getting-started/installation).
- The on-disk storage format is not yet covered by a long-term compatibility guarantee. Keep a way to rebuild important data (an export, seed data or an import script) until a stable release states one.

## Subsystem status

| Subsystem | Status | Notes |
| --- | --- | --- |
| Embedded API (`DataVoContext`) | Release candidate | The primary interface. |
| SQL dialect (DDL, DML, queries) | Release candidate | DataVo's own dialect, not PostgreSQL-compatible. See the [compatibility matrix](/manual/sql-language/sql-compatibility). |
| In-Memory storage | Release candidate | Ephemeral by design. |
| LSM-Tree storage | Release candidate | The recommended persistent engine: WAL, MemTable, SSTables, strict fsync by default. |
| Disk storage (paged) | Legacy | Kept for existing data. It receives no further durability work. Prefer LSM. |
| Transactions and MVCC | Release candidate | No serializable guarantee for arbitrary multi-row predicates. See [Transactions](/manual/storage-engine/transactions-acid-mvcc). |
| Compiled LINQ and source generators | Release candidate | Supported shapes are listed in [Compiled LINQ](/manual/client-interfaces/source-generators). Other shapes fall back to the interpreter. |
| Vector search (HNSW and FLAT) | Release candidate | Cosine and L2 distance. See [Vector Search Syntax](/manual/sql-language/vector-search-syntax). |
| ADO.NET provider (`DataVo.Data`) | Release candidate | Standard `System.Data.Common` types. See [ADO.NET Provider](/manual/client-interfaces/ado-net). |
| Dapper | Works through ADO.NET | No separate package. See [Dapper](/manual/client-interfaces/dapper). |
| Entity Framework Core integration | Bridge | Attaches DataVo to an EF context with a host EF provider. It is not a standalone provider and has no migrations. See [Entity Framework Core](/manual/client-interfaces/entity-framework). |
| Native AOT | Supported | `DataVo.Core` and `DataVo.Data` build with the trim and AOT analyzers as errors. |
| Browser / WebAssembly (`DataVo.Browser`) | Experimental / Playground Lane | Interactive documentation playground (`docs/src/DataVoClient.ts`); not a production target and not published on npm. |
| PostgreSQL replication (DataVo-Sync) | Separate product | Not part of the NuGet packages. Proprietary replication bridge to PostgreSQL. |
| Unity | Evaluation target | See below. |

## Preview contracts

SQL `DATE` materializes as `DataVo.Core.DataVoDate` in the row APIs. `CellValue.AsDate()` and compiled `GetDate` return `DataVoDate`; modern .NET callers keep explicit `FromDateOnly`, `ToDateOnly`, `AsDateOnly` and `GetDateOnly` adapters. Existing persisted `DATE` bytes are unchanged.

## Unity Evaluation Status

Unity is an evaluation target, not a supported runtime. The public `DataVo.Core` package contains only a `net10.0` asset. `DataVoEnablePortableTarget=true` creates an experimental local `netstandard2.1` candidate only. A passing .NET-host verifier by itself is not Unity Editor, Mono, IL2CPP, Jobs, or Burst proof.

| Environment or boundary | Status | Meaning |
| --- | --- | --- |
| Unity 6000.5.4f1 Editor | Verified (narrow smoke) | The tracked project imports the generated candidate and its EditMode smoke suite passes. |
| macOS IL2CPP (6000.5.4f1) | Verified (narrow smoke) | A Medium-stripping Player builds, launches, performs the Unity type-binding scenario, writes a passing report, and exits zero. |
| Windows x64 IL2CPP | Unverified | Windows requires its own built-and-executed Player proof. |
| Direct Burst calls | Unsupported by design | DataVo uses managed engine types and services and is not a Burst job payload. |
| Job-to-managed batch bridge | Planned proof | A Burst-compatible job may produce POD commands that managed code drains after completion. |
| In-memory mode | Candidate scope | This is the only storage mode included in the Stage 3 Unity candidate proof. |
| Disk/LSM persistence | Verified (narrow smoke) | Save, close and reopen pass for both modes in the Unity 6000.5.4f1 macOS Editor and a macOS IL2CPP Player. Other platforms and power-loss behaviour are unverified. |

## Scope summary

| Feature | Status | Notes |
| --- | --- | --- |
| In-process embedded deployment | Supported | The core product shape is library-first, not server-first. |
| Public curated manual | Supported | The manual is the public documentation boundary. |
| SQL execution through `DataVoContext` | Supported | Core SQL commands and query forms are documented. |
| ADO.NET provider | Supported | `DataVoConnection`, `DataVoCommand`, `DataVoDataReader`; Dapper works through it. |
| Entity Framework bridge | Supported | Basic `DbSet` workflows and query previews with explicit operator limits, not a full provider. |
| Roslyn source generators | Supported | `[DataVoQuery]` and typed table queries compile at build time. |
| NuGet packaging | Supported | Four packages. The public Core package is `net10.0`-only; the opt-in portable target is an experimental local candidate, not a published or supported Unity asset. |
| Stable storage format | Planned | Requires compatibility tests and migration guidance. |
| Production support contract | Not Supported | No production support contract is offered. |
| Stored procedures, triggers, PostgreSQL-compatible server | Not Supported | See [Unsupported Features](/manual/reference/unsupported-features). |
