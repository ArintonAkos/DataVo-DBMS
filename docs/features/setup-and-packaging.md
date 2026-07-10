# Setup and Packaging

This page gives end users a clear install path for .NET and JavaScript ecosystems.

## Distribution channels

DataVo has two packaging tracks:

- local package workflows available now
- public feed workflows (NuGet and npm) in deployment preparation

## NuGet

### Planned public install flow

When public packages are published:

```bash
dotnet add package DataVo.Core
dotnet add package DataVo.Data
dotnet add package DataVo.EntityFrameworkCore
dotnet add package DataVo.Generators # optional: source-generated compiled queries
```

### Local install flow available today

```bash
dotnet pack DataVo.sln -c Release
dotnet add package DataVo.Core --source ./artifacts/packages
dotnet add package DataVo.Data --source ./artifacts/packages
dotnet add package DataVo.EntityFrameworkCore --source ./artifacts/packages
dotnet add package DataVo.Generators --source ./artifacts/packages # optional: source-generated compiled queries
```

### Current target-framework boundary

The public DataVo.Core package currently contains only a net10.0 asset. The experimental `netstandard2.1` target is quarantined and is not a supported or distributed Unity artifact.

## npm

### Planned public install flow

```bash
npm install @datavo/wasm
```

### Browser runtime flow available today

```bash
bash ./scripts/deploy-browser-wasm.sh
cd docs
npm install
npm run docs:dev
```

This is the current customer-ready path for browser-based DataVo experiences while npm publication is finalized.

## Package map

| Package                    | Purpose                                             |
| :------------------------- | :-------------------------------------------------- |
| DataVo.Core                | Core SQL runtime, storage, indexing, transactions   |
| DataVo.Data                | Data-access integration surface                     |
| DataVo.EntityFrameworkCore | Entity Framework integration path                   |
| DataVo.Generators          | Source-generated compiled-query analyzer package    |
| @datavo/wasm (planned)     | JavaScript/TypeScript distribution for WASM runtime |

## Minimal .NET embedding sample

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

## End-user guidance by stack

- .NET app teams: start with DataVo.Core and DataVo.Data
- Unity and Godot teams: evaluation only; do not use the current packages as a shipped runtime or save-data dependency.
- The planned first Unity proof is limited to `StorageMode.InMemory`; Disk and LSM require separate platform durability validation.
- For deterministic tests and simulations, prefer `StorageMode.InMemory` plus `CreateSnapshot()` / `RestoreSnapshot(...)`.
- Browser teams: deploy WASM runtime assets and follow npm rollout updates
- EF teams: adopt DataVo.EntityFrameworkCore in bounded integration slices

## Related pages

- [WebAssembly and npm](./wasm-and-npm.md)
- [Unity and Godot](./unity-and-godot.md)
- [Entity Framework Integration](./entity-framework.md)
- [Roadmap and Integrations](./roadmap-and-integrations.md)
