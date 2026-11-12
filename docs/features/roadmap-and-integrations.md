# Roadmap and Integrations

This page explains the integration status and roadmap across NuGet, Entity Framework, vector search, Unity/Godot evaluation, and the experimental WebAssembly playground lane for DataVo v0.2 Release Candidate (`v0.2.0-rc.3`).

DataVo is a C#-native embedded database engine for .NET 10. The core embedded packages (`DataVo.Core`, `DataVo.Data`, `DataVo.EntityFrameworkCore`, and `DataVo.Generators`) are published as prerelease Release Candidate packages (`v0.2.0-rc.3`) on NuGet. WebAssembly exists strictly as an `Experimental / Playground Lane` runtime for interactive documentation (`DataVo.Browser` / `docs/src/DataVoClient.ts`) and is **not** a production-ready target or published on npm.

## Feature & Subsystem Matrix

| Subsystem / Target | Status | Notes |
| :----------------- | :----- | :---- |
| .NET 10 Embedded Core (`DataVo.Core`) | Release Candidate (`v0.2.0-rc.3`) | Primary in-process SQL, LSM storage, and zero-alloc point lookup engine. |
| ADO.NET Provider (`DataVo.Data`) | Release Candidate (`v0.2.0-rc.3`) | `DataVoConnection`, `DataVoCommand`, `DataVoDataReader`, and Dapper compatibility. |
| Native AOT (`DataVo.Core` + `DataVo.Data`) | Locked & Passing | Trim and AOT analyzers enforced as build errors (`IL2026`/`IL3050`). |
| Roslyn Source Generators (`DataVo.Generators`) | Release Candidate (`v0.2.0-rc.3`) | `[DataVoQuery]` compiled query plans and typed row mappers. |
| SIMD Vector Search (`HNSW` & `FLAT`) | Release Candidate (`v0.2.0-rc.3`) | Cosine (`<=>`) and Euclidean/L2 (`<->`) distance operators. |
| Entity Framework Core (`DataVo.EntityFrameworkCore`) | Preview Bridge | Supports bounded `DbSet` and `QueryFromDataVo` workflows; migrations unsupported. |
| Browser / WebAssembly (`DataVo.Browser`) | `Experimental / Playground Lane` | Powers the interactive docs playground (`docs/src/DataVoClient.ts`); not production-ready and not published on npm. |
| Unity & Godot | Evaluation Target | Narrow macOS Editor/IL2CPP smoke recorded for Unity 6000.5.4f1; not a supported runtime. |

## Available Today (v0.2.0-rc.3)

### NuGet Prerelease & Local Packaging

The four .NET packages are published to nuget.org on release tags via NuGet Trusted Publishing (`--prerelease`) and can also be packed locally:

- `DataVo.Core`
- `DataVo.Data`
- `DataVo.EntityFrameworkCore`
- `DataVo.Generators`

### Direct Embedding & ADO.NET

The primary production-oriented integration path is direct embedding through `DataVoContext` or standard ADO.NET via `DataVoConnection` on .NET 10.

### Security and Auth Command Surface

Principal, grant, and session auth SQL command families (`CREATE USER`, `CREATE ROLE`, `GRANT`, `REVOKE`, `LOGIN`, `LOGOUT`, `SHOW GRANTS`) are implemented in the core engine. See [Security and Authentication](./security-and-authentication.md).

## WebAssembly Support (`Experimental / Playground Lane`)

The repository includes an experimental browser/WASM build (`DataVo.Browser`) and a TypeScript client (`docs/src/DataVoClient.ts`) used to power interactive SQL snippets in the documentation site.

- **Status:** `Experimental / Playground Lane`
- **Production Readiness:** **Not** a production target. Contrast this directly with the .NET 10 embedded engine (`DataVo.Core`), which is at Release Candidate status (`v0.2.0-rc.3`).
- **npm Publication:** **Not** published on npm. There is no public `@datavo/wasm` package.

Local docs playground workflow:

```bash
bash ./scripts/deploy-browser-wasm.sh
cd docs
npm run docs:dev
```

See [WebAssembly and npm](./wasm-and-npm.md).

## Unity and Godot Evaluation

Unity is an evaluation target, not an available runtime integration. A narrow in-memory managed boundary has passed in Unity 6000.5.4f1 Editor and macOS IL2CPP; Windows x64, Burst, Jobs, vector/reactive workloads, and persistence require separate validation. Godot requires a separate validation pass.

See [Unity and Godot](./unity-and-godot.md) for the current support matrix and proof boundary.

## Entity Framework Support

Entity Framework support is shipped as a preview integration bridge (`DataVo.EntityFrameworkCore`).

- Attaches DataVo storage to an EF `DbContext` alongside a host provider.
- Supports basic `DbSet` queries, `Add` + `SaveChanges`, `LoadFromDataVo`, and `QueryFromDataVo` with explicit operator validation.
- Does **not** support EF Core migrations, shadow properties, or arbitrary complex LINQ joins.

See [Entity Framework Integration](./entity-framework.md).

## Active v0.3 Roadmap

With the v0.2 milestones completed (LSM stability, Native AOT lock, SIMD vector search, and zero-alloc in-memory point lookups), active engineering for **v0.3** focuses on:

1. **Columnar Indexes for HTAP:** Hybrid transactional/analytical processing with columnar scan projections and SIMD aggregations to close the analytical scan gap with DuckDB while preserving fast OLTP point lookups.
2. **Compact In-Memory Row Layout:** Reducing per-row memory overhead and improving CPU cache locality for large in-memory tables.
3. **PostgreSQL-Wire Tooling Bridge (`DataVo.Server`):** A companion server process implementing a PostgreSQL wire-protocol subset so external database tools (DBeaver, DataGrip, pgAdmin) can inspect tables and run queries.

## Distribution Status

| Distribution Mode        | Status                           |
| :----------------------- | :------------------------------- |
| Local .NET packaging     | Available (`dotnet pack`)        |
| Public NuGet publication | Published (`v0.2.0-rc.3` prerelease) |
| Browser WASM runtime     | `Experimental / Playground Lane` (docs only) |
| Public npm publication   | Not Published                    |
