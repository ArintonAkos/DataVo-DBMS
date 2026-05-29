# Tasks 7-10 Source Generator Report

## What I implemented

- Added the `DataVo.Generators` source-generator project targeting `netstandard2.0`.
- Added the `DataVo.Generators.Tests` test project and wired the generator as an analyzer.
- Implemented `DATAVOQ001` for unsupported SQL and `DATAVOQ002` for missing SQL-to-method parameter matches.
- Implemented `DataVoQueryShapeParser` for the V1 supported SQL shapes:
  - `SELECT <cols> FROM <table> WHERE <col> = @param`
  - `INSERT INTO <table> (<cols>) VALUES (@params...)`
  - `UPDATE <table> SET <col> = @param[, ...] WHERE <col> = @param`
- Implemented Roslyn incremental generation for static partial methods annotated with `[DataVoQuery]`.
- Emitted generated partial method bodies that consume `DataVoCompiledQueryPlan`, `DataVoCompiledQuery`, and `DataVoCompiledQueryParameter` from `DataVo.Core.CompiledQueries`.
- Added analyzer reference integration to `DataVo.Tests`.
- Added source-generated E2E tests for generated `SELECT`, `INSERT`, and `UPDATE` flows.
- Added both generator projects to `DataVo.sln`.

## Tests and outputs

### Focused generator parser run

Command:

```bash
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj --filter ShapeParser
```

Result:

```text
Passed!  - Failed: 0, Passed: 2, Skipped: 0, Total: 2
```

### Generator test suite

Command:

```bash
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj
```

Result:

```text
Passed!  - Failed: 0, Passed: 5, Skipped: 0, Total: 5
```

### Source-generated E2E tests

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter SourceGeneratedCompiledQueryTests
```

Result:

```text
Passed!  - Failed: 0, Passed: 3, Skipped: 0, Total: 3
```

### Compiled-query regression slice

Command:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "CompiledQueryRuntimeTests|SourceGeneratedCompiledQueryTests|RuntimeDiagnosticsTests"
```

Result:

```text
Passed!  - Failed: 0, Passed: 21, Skipped: 0, Total: 21
```

## TDD evidence RED/GREEN

### RED

1. Initial `dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj`
   - failed on a project compatibility issue: `DataVo.Generators` referenced `DataVo.Core` despite targeting `netstandard2.0`.
2. After removing that dependency edge, the next red run exposed generator-project compatibility issues:
   - `IsExternalInit` missing from the `record`-based query model on `netstandard2.0`.
3. After replacing the record with a class and fixing the xUnit/test harness setup, the next red run reached the intended behavior failures:
   - parser tests returned `false`
   - generator emitted no sources
   - expected diagnostics were absent

### GREEN

1. Implemented parser support and verified:
   - `dotnet test ... --filter ShapeParser` passed.
2. Implemented generator scanning/emission/diagnostics and verified:
   - `dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj` passed.
3. Added analyzer reference + E2E generated-query tests and verified:
   - `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter SourceGeneratedCompiledQueryTests` passed.
4. Ran compiled-query regression coverage and verified:
   - `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "CompiledQueryRuntimeTests|SourceGeneratedCompiledQueryTests|RuntimeDiagnosticsTests"` passed.

## Files changed

- `DataVo.Generators/DataVo.Generators.csproj`
- `DataVo.Generators/DataVoQueryGenerator.cs`
- `DataVo.Generators/Diagnostics/DataVoGeneratorDiagnostics.cs`
- `DataVo.Generators/Properties/AssemblyInfo.cs`
- `DataVo.Generators/Sql/GeneratedQueryModel.cs`
- `DataVo.Generators/Sql/DataVoQueryShapeParser.cs`
- `DataVo.Generators.Tests/DataVo.Generators.Tests.csproj`
- `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`
- `DataVo.Tests/DataVo.Tests.csproj`
- `DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs`
- `DataVo.sln`

## Self-review findings

- The generator stays within the V1 boundary: only single-statement `SELECT`, `INSERT`, and `UPDATE` are supported.
- Unsupported SQL shapes, including joins, fail at build time with `DATAVOQ001`.
- The generated fast path constructs `DataVoCompiledQueryPlan` objects directly and executes through compiled-query runtime helpers without runtime lexing/parsing for supported source-generated paths.
- The generator project has no runtime-project reference, which preserves the required `netstandard2.0` target compatibility.
- DTO mapper generation prefers constructor-parameter type inference when a safe positional constructor match exists; otherwise it falls back to the plan’s allowed simple column-name heuristic.

## Concerns

- `SELECT` generation for collection-returning methods is implemented via `SelectMany`, but this milestone’s E2E coverage only exercises generated `SelectSingle`, `Insert`, and `Update`. Collection-returning source-generated `SELECT` currently has generator-level coverage, not dedicated runtime E2E coverage in this task.
