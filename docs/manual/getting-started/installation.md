---
title: Installation & Packages
description: Install DataVo v0.5 (0.5.1) NuGet packages for .NET 8, 9 and 10, including Core, Generators, ADO.NET, and EF Core.
---

# Installation & Packages

DataVo ships as five NuGet packages on `nuget.org`, with `DataVo.Core` as the foundation. The current release is **0.5.1**.

```bash
dotnet add package DataVo.Core
dotnet add package DataVo.Generators
dotnet add package DataVo.Data
dotnet add package DataVo.EntityFrameworkCore
dotnet add package DataVo.VectorData
```

## Package Reference

| Package | Install When You Need | Includes |
| --- | --- | --- |
| `DataVo.Core` | Embedded SQL, typed tables, vector search, reactive views | `DataVoContext`, `InMemory`/`Lsm` engines, HNSW/FLAT/columnar indexes |
| `DataVo.Generators` | Build-time compiled LINQ and `[DataVoQuery]` methods | Roslyn source generator & interceptors (build-time only, no runtime dependency) |
| `DataVo.Data` | Standard ADO.NET or Dapper micro-ORM | `DataVoConnection`, `DataVoCommand`, `DataVoDataReader`, `DataVoTransaction`, `DataVoParameter` |
| `DataVo.EntityFrameworkCore` | Experimental EF Core `DbContext` bridge | `UseDataVo(...)`, `EnsureDataVoCreated()`, `DataVoVectorDbFunctions` |
| `DataVo.VectorData` | `Microsoft.Extensions.VectorData` / Microsoft.Extensions.AI RAG pipelines | `DataVoVectorStore`, `DataVoVectorStoreCollection<TKey, TRecord>`, `AddDataVoVectorStore(...)` ([guide](/manual/client-interfaces/vector-data)) |

## Recommended Combinations

| Workload | Packages to Add |
| --- | --- |
| Embedded SQL & `DataVoTable<T>` | `DataVo.Core` |
| Zero-allocation compiled LINQ & AOT | `DataVo.Core` + `DataVo.Generators` |
| ADO.NET or Dapper | `DataVo.Data` *(transitively brings `DataVo.Core`)* |
| Existing EF Core models | `DataVo.EntityFrameworkCore` *(transitively brings `DataVo.Core`)* |
| Microsoft.Extensions.VectorData / RAG pipelines | `DataVo.VectorData` *(transitively brings `DataVo.Core`)* |

## Requirements

| Package | Target Frameworks | Native AOT |
| --- | --- | --- |
| `DataVo.Core` | `net8.0`, `net9.0`, `net10.0`, `netstandard2.1` | Yes: trim/AOT analyzers (`IL2026`, `IL3050`) are build errors |
| `DataVo.Data` | `net8.0`, `net9.0`, `net10.0`, `netstandard2.1` | Yes: same analyzer gate |
| `DataVo.VectorData` | `net8.0`, `net9.0`, `net10.0` | Yes: same analyzer gate |
| `DataVo.Generators` | `netstandard2.0` (Roslyn analyzer, build-time only) | Generated code is AOT-safe |
| `DataVo.EntityFrameworkCore` | `net10.0` | No: excluded from Native AOT |

- **Unity:** evaluation builds use the `netstandard2.1` asset of `DataVo.Core`; see [Status & Roadmap](/manual/getting-started/roadmap#unity-evaluation-status).
- **C# Interceptors:** `DataVo.Generators` ships MSBuild `.targets` (under `build/` and `buildTransitive/`) that automatically register `DataVo.Generated` in `InterceptorsNamespaces`.
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
