# Setup and Packaging

This page gives end users a clear install path for the .NET 10 embedded engine and explains the boundaries for Unity evaluation and the experimental browser playground.

## Distribution Channels

DataVo distributes its core .NET packages via NuGet (prerelease) and local source builds:

- **Public NuGet feed:** `v0.2.0-rc.3` Release Candidate packages published via NuGet Trusted Publishing.
- **Local package workflow:** `dotnet pack` directly from the repository.
- **Browser / WebAssembly (`DataVo.Browser`):** `Experimental / Playground Lane` for the documentation site only — **not** published on npm and **not** a production target.

## NuGet (.NET 10)

### Public Release Candidate Install Flow

Install the `v0.2.0-rc.3` Release Candidate packages from nuget.org using `--prerelease`:

```bash
dotnet add package DataVo.Core --prerelease
dotnet add package DataVo.Data --prerelease
dotnet add package DataVo.EntityFrameworkCore --prerelease
dotnet add package DataVo.Generators --prerelease # optional: source-generated compiled queries
```

### Local Install Flow

```bash
dotnet pack DataVo.sln -c Release
dotnet add package DataVo.Core --source ./artifacts/packages
dotnet add package DataVo.Data --source ./artifacts/packages
dotnet add package DataVo.EntityFrameworkCore --source ./artifacts/packages
dotnet add package DataVo.Generators --source ./artifacts/packages
```

### Current Target-Framework Boundary

The public DataVo.Core package currently contains only a net10.0 asset. `DataVoEnablePortableTarget=true` opts a local build or pack into an experimental `netstandard2.1` candidate; it does not change the normal package or tag-publish path. On that candidate, Disk and LSM are rejected and `StorageMode.InMemory` is the only Stage 3 candidate mode. Passing the portable-asset .NET-host verifier alone proves neither Unity Editor nor Mono, IL2CPP, Jobs, or Burst compatibility. A separate narrow Unity 6000.5.4f1 macOS Editor/IL2CPP smoke gate is recorded; it is not general Unity runtime support.

### SQL DATE Preview Migration

SQL `DATE` now materializes as `DataVo.Core.DataVoDate` in preview row APIs. `CellValue.AsDate()` and compiled `GetDate` return `DataVoDate`. Modern .NET callers retain explicit `FromDateOnly`, `ToDateOnly`, `AsDateOnly`, and `GetDateOnly` adapters. Existing persisted `DATE` bytes are unchanged, so this API change does not require rewriting stored rows.

## WebAssembly (`Experimental / Playground Lane`)

DataVo is **not** published on npm (`@datavo/wasm` does not exist on the public registry), and the WebAssembly build (`DataVo.Browser` / `docs/src/DataVoClient.ts`) is **not** a production-ready target.

To build and deploy the interactive WASM playground for the local documentation site:

```bash
bash ./scripts/deploy-browser-wasm.sh
cd docs
npm install
npm run docs:dev
```

## Package & Target Map

| Package / Target           | Status                           | Purpose                                             |
| :------------------------- | :------------------------------- | :-------------------------------------------------- |
| `DataVo.Core`              | Release Candidate (`v0.2.0-rc.3`) | Core SQL runtime, LSM storage, indexing, transactions |
| `DataVo.Data`              | Release Candidate (`v0.2.0-rc.3`) | ADO.NET provider surface (`DbConnection`, Dapper)   |
| `DataVo.Generators`        | Release Candidate (`v0.2.0-rc.3`) | Roslyn source-generated compiled-query package      |
| `DataVo.EntityFrameworkCore` | Preview Bridge (`v0.2.0-rc.3`) | Entity Framework Core bridge (no migrations)        |
| `DataVo.Browser` (WASM)    | `Experimental / Playground Lane` | Interactive docs playground runtime; not on npm     |

## Minimal .NET Embedding Sample

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE Demo");
db.Execute("USE Demo");
db.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50))");
db.Execute("INSERT INTO Users VALUES (1, 'Alice')");
var result = db.Execute("SELECT * FROM Users ORDER BY Id");
```

## End-User Guidance by Stack

- **.NET 10 app teams:** Use `DataVo.Core` and `DataVo.Data` (`v0.2.0-rc.3`). Both packages enforce Native AOT and trim analyzers as errors.
- **Unity and Godot teams:** Evaluation only; do not use the current packages as a shipped runtime or save-data dependency.
- **Browser / JS teams:** Treat `DataVo.Browser` strictly as an interactive documentation playground, not a production database or npm dependency.
- **EF Core teams:** Adopt `DataVo.EntityFrameworkCore` in bounded integration slices where migrations are not required.

## Related Pages

- [WebAssembly and npm](./wasm-and-npm.md)
- [Unity and Godot](./unity-and-godot.md)
- [Entity Framework Integration](./entity-framework.md)
- [Roadmap and Integrations](./roadmap-and-integrations.md)
