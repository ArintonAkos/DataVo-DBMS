# Setup and Packaging

This page explains how to consume `DataVo` today from a .NET project.

## Current distribution model

`DataVo` is currently intended to be consumed through **local packages built from this repository**.

At the moment:

- packaging is ready locally
- packages are produced from the solution
- public NuGet publication is planned, but not enabled yet

## What gets packaged today

The local packaging workflow currently produces:

| Package       | Purpose                                                                              |
| :------------ | :----------------------------------------------------------------------------------- |
| `DataVo.Core` | Core engine runtime, parser, storage, indexing, transactions, and execution pipeline |
| `DataVo.Data` | Data-access-facing layer and the natural foundation for ADO.NET-style integration    |

Applications and non-library projects are intentionally not packaged:

- test project
- server app
- frontend app

## Build local packages

From the repository root:

```bash
dotnet pack DataVo.sln -c Release
```

Generated artifacts are written to:

- `artifacts/packages`

Typical output files:

- `DataVo.Core.<version>.nupkg`
- `DataVo.Core.<version>.snupkg`
- `DataVo.Data.<version>.nupkg`
- `DataVo.Data.<version>.snupkg`

## Install into another .NET project

### Option 1: local package source

Suppose your consuming project is outside this repository.

1. Build the packages.
2. Point NuGet at the local package folder.
3. Add the package reference.

Example `NuGet.Config`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="local-datavo" value="/absolute/path/to/DataVo-DBMS/artifacts/packages" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
```

Then add the package:

```bash
dotnet add package DataVo.Core --source /absolute/path/to/DataVo-DBMS/artifacts/packages
```

## Minimal embedded usage in a .NET app

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var context = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

context.Execute("CREATE DATABASE Demo");
context.Execute("USE Demo");
context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50))");
context.Execute("INSERT INTO Users VALUES (1, 'Alice')");

var results = context.Execute("SELECT * FROM Users");
```

## ADO.NET status

ADO.NET is part of the product direction, but the polished provider story is still evolving.

The repository already contains `DataVo.Data`, which is the natural home for the ADO.NET-facing surface, but the project should currently be viewed as:

- **packaged locally today**
- **provider ergonomics still evolving**
- **full public documentation for the ADO.NET experience still in progress**

So the recommended setup today is:

- use `DataVo.Core` directly for embedding
- treat `DataVo.Data` as part of the integration surface under active development

## VS Code task shortcuts

The repository also defines task runner entries:

| Task         | What it does                               |
| :----------- | :----------------------------------------- |
| `pack`       | Packs both `DataVo.Core` and `DataVo.Data` |
| `pack: core` | Packs only `DataVo.Core`                   |
| `pack: data` | Packs only `DataVo.Data`                   |

## Recommended developer workflow today

| Step                         | Command                               |
| :--------------------------- | :------------------------------------ |
| Build and test               | `dotnet test DataVo.sln`              |
| Produce packages             | `dotnet pack DataVo.sln -c Release`   |
| Inspect packages             | `ls artifacts/packages`               |
| Consume from another project | `dotnet add package ... --source ...` |
