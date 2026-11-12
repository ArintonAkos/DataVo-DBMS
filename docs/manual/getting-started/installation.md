# Installation & Packages

DataVo ships as four NuGet packages. Install only the ones you need: `DataVo.Core` is the engine and everything else builds on it.

The current release is **v0.2.0-rc.3 (Release Candidate)**, so include `--prerelease` when you add a package. See [Project Status & Stability](/manual/preface/project-status) for what that means.

## Packages

| Package | Install it when you want | What it adds |
| --- | --- | --- |
| `DataVo.Core` | The embedded engine and SQL through `DataVoContext`. | Parser, planner, storage engines (In-Memory, LSM-Tree, legacy Disk), vector indexes, typed table API. |
| `DataVo.Data` | An ADO.NET provider, or a micro-ORM such as Dapper. | `DataVoConnection`, `DataVoCommand`, `DataVoDataReader`, `DataVoTransaction`, `DataVoParameter`. |
| `DataVo.EntityFrameworkCore` | An EF Core `DbContext` over DataVo. | `UseDataVo(...)`, `DataVoDbContext`, query-translation diagnostics. |
| `DataVo.Generators` | Compiled LINQ queries and `[DataVoQuery]` methods. | A Roslyn source generator and analyzer. It is a build-time reference, not a runtime dependency. |

```bash
dotnet add package DataVo.Core --prerelease
dotnet add package DataVo.Data --prerelease
dotnet add package DataVo.EntityFrameworkCore --prerelease
dotnet add package DataVo.Generators --prerelease
```

## Which packages for which job

| Job | Packages |
| --- | --- |
| Embedded SQL and the typed table API | `DataVo.Core` |
| Compiled LINQ scans and aggregates | `DataVo.Core` and `DataVo.Generators` |
| ADO.NET or Dapper | `DataVo.Data` (brings `DataVo.Core`) |
| EF Core | `DataVo.EntityFrameworkCore` (brings `DataVo.Core`) |

## Requirements

- **.NET 10.** The published `DataVo.Core` package contains a `net10.0` asset only. Other targets, including Unity, are not supported. See [Project Status](/manual/preface/project-status#unity-evaluation-status).
- **Native AOT** is supported by `DataVo.Core` and `DataVo.Data`. See [Native AOT Deployment](/manual/client-interfaces/native-aot).
- **C# interceptors** are used by `DataVo.Generators`. The package's build props register the `DataVo.Generated` interceptors namespace for you, so no manual `InterceptorsNamespaces` entry is needed.

## Verify the install

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Smoke");
db.Execute("USE Smoke");
db.Execute("CREATE TABLE T (Id INT PRIMARY KEY)");
db.Execute("INSERT INTO T (Id) VALUES (1)");

Console.WriteLine(db.Execute("SELECT Id FROM T")[0].Data.Count); // 1
```

Continue with the [Quickstart](/manual/tutorial/quickstart), or pick an API in [Choosing an Interface](/manual/getting-started/choosing-an-interface). See also [Project Status & Scope](/manual/preface/project-status).
