# Installation & Packages

> Source route: /manual/getting-started/installation
> Source file: manual/getting-started/installation.md

DataVo ships as four NuGet packages on `nuget.org`, with `DataVo.Core` as the foundation. The current release is **0.4.0-rc.1 (release candidate)**, so pass `--prerelease` when installing.

```bash
dotnet add package DataVo.Core --prerelease
dotnet add package DataVo.Generators --prerelease
dotnet add package DataVo.Data --prerelease
dotnet add package DataVo.EntityFrameworkCore --prerelease
```

## Package Reference

| Package | Install When You Need | Includes |
| --- | --- | --- |
| `DataVo.Core` | Embedded SQL, typed tables, vector search, reactive views | `DataVoContext`, `InMemory`/`Lsm`/`Disk` engines, HNSW/FLAT/columnar indexes |
| `DataVo.Generators` | Build-time compiled LINQ and `[DataVoQuery]` methods | Roslyn source generator & interceptors (build-time only, no runtime dependency) |
| `DataVo.Data` | Standard ADO.NET or Dapper micro-ORM | `DataVoConnection`, `DataVoCommand`, `DataVoDataReader`, `DataVoTransaction`, `DataVoParameter` |
| `DataVo.EntityFrameworkCore` | Experimental EF Core `DbContext` bridge | `UseDataVo(...)`, `EnsureDataVoCreated()`, `DataVoVectorDbFunctions` |

## Recommended Combinations

| Workload | Packages to Add |
| --- | --- |
| Embedded SQL & `DataVoTable<T>` | `DataVo.Core` |
| Zero-allocation compiled LINQ & AOT | `DataVo.Core` + `DataVo.Generators` |
| ADO.NET or Dapper | `DataVo.Data` *(transitively brings `DataVo.Core`)* |
| Existing EF Core models | `DataVo.EntityFrameworkCore` *(transitively brings `DataVo.Core`)* |

## Requirements

- **`.NET 10` (`net10.0`):** Published packages target `net10.0`. Multi-target packages (`net8.0`, `net9.0`, `net10.0`, `netstandard2.1`) are planned for v0.5; see [Status & Roadmap](/manual/getting-started/roadmap#unity-evaluation-status) for Unity evaluation details.
- **Native AOT:** `DataVo.Core`, `DataVo.Data`, and `DataVo.Generators` build with trim and AOT analyzers (`IL2026`, `IL3050`) as errors. `DataVo.EntityFrameworkCore` is excluded from Native AOT.
- **C# Interceptors:** `DataVo.Generators` ships MSBuild `.props` that automatically register `DataVo.Generated` in `InterceptorsNamespaces`.
- **Building from source:** Run `dotnet pack DataVo.sln -c Release` to produce local `.nupkg` files in `artifacts/packages`.

## Verify Your Installation

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Smoke");
db.Execute("USE Smoke");
db.Execute("CREATE TABLE players (Id INT PRIMARY KEY, Name VARCHAR(80))");
db.Execute("INSERT INTO players (Id, Name) VALUES (1, 'Ada')");

Console.WriteLine(db.Execute("SELECT Id, Name FROM players")[0].Data.Count); // 1
```

## See Also

- [Quickstart](/manual/getting-started/quickstart): Build a complete in-memory and persistent LSM example.
- [Choosing an Interface](/manual/getting-started/choosing-an-interface): Decide between `DataVoContext`, Compiled LINQ, ADO.NET, Dapper, and EF Core.
- [Native AOT](/manual/client-interfaces/native-aot): Publishing trimmed, single-file native binaries.
