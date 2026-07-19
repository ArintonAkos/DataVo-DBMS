# Unity Portable Contract Candidate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the first coherent slice of Stage 2 by replacing the cross-target SQL `DATE` surface with `DataVoDate`, removing public BCL impersonation types, rejecting unverified durable storage on the experimental portable target, and proving an exact locally packed `netstandard2.1` candidate from a clean consumer.

**Architecture:** `DataVoDate` in the root `DataVo.Core` namespace is the canonical SQL `DATE` value on every target. Common engine code stores and materializes this type; target-specific whole files add modern `DateOnly` conveniences without changing the portable contract. Existing DATE bytes remain unchanged by converting through midnight `DateTime` at the serialization boundary. A small platform policy rejects Disk and LSM before backend construction on `netstandard2.1`. A dedicated candidate verifier packs both Core assets, checks their shape and public type ownership, compiles a clean portable consumer, and executes a .NET-host smoke test against the exact extracted portable assembly.

**Tech Stack:** C#/.NET 10 and .NET Standard 2.1, xUnit, Roslyn source generators, MSBuild/NuGet package validation, Bash.

## Global Constraints

- Implement only Stage 2A from `docs/superpowers/specs/2026-07-10-unity-compatibility-reset-design.md`. Do not restore unrelated modern hot paths, split HNSW/SIMD files, change HNSW defaults, add a Unity project/UPM package, or publish the portable asset.
- Keep the normal build, normal package, and tag-publish path `net10.0`-only. `DataVoEnablePortableTarget=true` remains required for the candidate.
- The public type is exactly `DataVo.Core.DataVoDate`. It stores an `int` day number, implements value equality and total ordering, validates the range supported by `DateTime`, and formats invariantly as `yyyy-MM-dd`.
- `DataVoDate` is the canonical type returned by `CellValue.AsDate()`, `CellValue.ToObject()`, compiled `GetDate` readers, dictionaries, typed rows, and generated `DataVoDate` projections on all targets.
- Modern `DateOnly` APIs are explicit adapters: `DataVoDate.FromDateOnly`, `DataVoDate.ToDateOnly`, `CellValue.From(DateOnly)`, `CellValue.AsDateOnly()`, and compiled `GetDateOnly` readers. Do not add implicit conversions.
- Do not declare public compatibility types under `System`. Retain only the internal compiler attributes still required by the portable compiler.
- Preserve the existing DATE wire representation byte-for-byte: midnight `DateTime.ToBinary()` written as the existing 64-bit payload. This slice does not introduce a storage migration.
- On `netstandard2.1`, `StorageMode.Disk` and `StorageMode.Lsm` throw `PlatformNotSupportedException` before a backend is constructed. `StorageMode.InMemory` remains usable. Modern storage behavior must not change.
- Candidate smoke output must call itself a portable-asset .NET-host check, never a Unity test or Unity benchmark.
- Use the exact `.nupkg` produced by the verifier. Give restore and package caches unique temporary directories so an installed/global package cannot satisfy the proof accidentally.
- Preserve unrelated user-owned untracked files. Use `apply_patch` for edits and stage exact paths rather than `git add .`.
- Baseline for this plan is commit `976a7e6`.

---

### Task 1: Introduce the canonical `DataVoDate` value type

**Files:**

- Create: `DataVo.Core/DataVoDate.cs`
- Create: `DataVo.Core/DataVoDate.Modern.cs`
- Create: `DataVo.Tests/Types/DataVoDateTests.cs`

**Task interface:**

```csharp
namespace DataVo.Core;

public readonly partial struct DataVoDate :
    IEquatable<DataVoDate>, IComparable<DataVoDate>, IComparable, IFormattable
{
    public int DayNumber { get; }
    public static DataVoDate FromDayNumber(int dayNumber);
    public static DataVoDate FromDateTime(DateTime value);
    public DateTime ToDateTime();
    public static DataVoDate Parse(string input);
    public static bool TryParse(string? input, out DataVoDate result);
    public static bool TryParse(
        string? input,
        IFormatProvider? provider,
        DateTimeStyles styles,
        out DataVoDate result);
}
```

The modern partial adds only:

```csharp
public static DataVoDate FromDateOnly(DateOnly value);
public DateOnly ToDateOnly();
```

- [ ] **Step 1: Write failing contract tests**

Cover the minimum/maximum day numbers (`0` and `3652058`), range failures, `DateTime` date-only normalization, equality, comparison interfaces and operators, stable hash codes, invariant default/`O` formatting, parsing, and explicit `DateOnly` round trips. Change the current culture inside the formatting test and restore it in `finally`.

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~DataVoDateTests"
```

Expected: compilation fails because `DataVoDate` does not exist.

- [ ] **Step 2: Implement the common value contract**

Store only `_dayNumber`. Validate through explicit bounds so both targets throw `ArgumentOutOfRangeException` consistently. Implement `CompareTo(object?)` with normal value-type semantics: null sorts before a date and any non-`DataVoDate` value throws `ArgumentException`.

Use `DateTime` only at parse/format conversion boundaries. `ToString()`, `ToString("O", provider)`, and `ToString("yyyy-MM-dd", provider)` must produce invariant `yyyy-MM-dd`; other valid `DateTime` formats may delegate to the midnight `DateTime` representation.

- [ ] **Step 3: Add modern adapters in a whole target file**

Guard the complete contents of `DataVoDate.Modern.cs` once with `#if NET10_0_OR_GREATER`. Do not place target checks inside the common struct's methods.

- [ ] **Step 4: Verify both target builds and the focused tests**

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~DataVoDateTests"

dotnet build DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
```

Expected: the focused tests and both builds pass. The portable build may still use the existing public polyfills at this checkpoint.

- [ ] **Step 5: Commit the value contract**

```bash
git add \
  DataVo.Core/DataVoDate.cs \
  DataVo.Core/DataVoDate.Modern.cs \
  DataVo.Tests/Types/DataVoDateTests.cs
git commit -m "feat: add cross-target DataVoDate contract"
```

---

### Task 2: Make reactive and compiled row APIs canonical across targets

**Files:**

- Modify: `DataVo.Core/Runtime/Reactive/CellValue.cs`
- Create: `DataVo.Core/Runtime/Reactive/CellValue.Modern.cs`
- Modify: `DataVo.Core/CompiledQueries/CompiledRowReader.cs`
- Create: `DataVo.Core/CompiledQueries/CompiledRowReader.Modern.cs`
- Create: `DataVo.Core/Compat/DateObjectAdapter.Modern.cs`
- Create: `DataVo.Core/Compat/DateObjectAdapter.Portable.cs`
- Modify: `DataVo.Generators/DataVoQueryGenerator.cs`
- Modify: `DataVo.Tests/Reactive/CellValueDateTests.cs`
- Modify: `DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs`
- Modify: `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`

**Task interface:**

```csharp
public static CellValue From(DataVoDate value);
public DataVoDate AsDate();

public DataVoDate GetDate(string column);
public DataVoDate GetDate(int ordinal);
public DataVoDate? GetDateOrNull(string column);
public DataVoDate? GetDateOrNull(int ordinal);
```

The modern partials retain explicit conveniences with `DateOnly` in their names. A private target adapter lets `CellValue.From(object?)` recognize a boxed `DateOnly` on modern .NET without mentioning `DateOnly` in common code.

- [ ] **Step 1: Change the tests to the approved contract and observe the red state**

Require:

- `CellValue.From(DataVoDate)` stores `CellType.Date` and round-trips through `AsDate()`;
- `ToObject()` returns a boxed `DataVoDate`;
- modern `From(DateOnly)` and `AsDateOnly()` round-trip through the canonical cell;
- generated `DataVoDate` and `DataVoDate?` properties call `GetDate`/`GetDateOrNull`;
- generated `DateOnly` and `DateOnly?` properties call `GetDateOnly`/`GetDateOnlyOrNull`.

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~CellValueDateTests|FullyQualifiedName~SourceGeneratedCompiledQueryTests"

dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj -c Release \
  --filter "FullyQualifiedName~DataVoQueryGeneratorTests"
```

Expected: compilation or assertions fail against the old `DateOnly`-canonical API.

- [ ] **Step 2: Convert `CellValue` and its documentation**

Make `CellValue` a `partial` struct. Store `DataVoDate.DayNumber`, make `AsDate()` return `DataVoDate.FromDayNumber`, and make `ToObject()` return that same canonical value. Add `DataVoDate` before the target adapter in `From(object?)`.

Put `DateOnly` overloads in `CellValue.Modern.cs`; do not use conditional compilation inside the common object switch. Keep the date cell allocation-free.

- [ ] **Step 3: Convert compiled readers and the generator mapping**

Make `CompiledRowReader` partial. Change `GetDate`/`GetDateOrNull` to the common `DataVoDate` contract. Add `GetDateOnly`/`GetDateOnlyOrNull` in the modern partial.

Update generator getter selection exactly as follows:

| Property type | Emitted reader |
|---|---|
| `DataVo.Core.DataVoDate` | `GetDate` |
| `DataVo.Core.DataVoDate?` | `GetDateOrNull` |
| `System.DateOnly` | `GetDateOnly` |
| `System.DateOnly?` | `GetDateOnlyOrNull` |

- [ ] **Step 4: Run focused tests and target builds**

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~CellValueDateTests|FullyQualifiedName~SourceGeneratedCompiledQueryTests"

dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj -c Release \
  --filter "FullyQualifiedName~DataVoQueryGeneratorTests"

dotnet build DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
```

Expected: all commands pass.

- [ ] **Step 5: Commit the canonical public row API**

```bash
git add \
  DataVo.Core/Runtime/Reactive/CellValue.cs \
  DataVo.Core/Runtime/Reactive/CellValue.Modern.cs \
  DataVo.Core/CompiledQueries/CompiledRowReader.cs \
  DataVo.Core/CompiledQueries/CompiledRowReader.Modern.cs \
  DataVo.Core/Compat/DateObjectAdapter.Modern.cs \
  DataVo.Core/Compat/DateObjectAdapter.Portable.cs \
  DataVo.Generators/DataVoQueryGenerator.cs \
  DataVo.Tests/Reactive/CellValueDateTests.cs \
  DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs \
  DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs
git commit -m "refactor: make DataVoDate the row contract"
```

---

### Task 3: Preserve DATE storage while removing public `System` polyfills

**Files:**

- Modify: `DataVo.Core/Compat/NetStandard21Polyfills.cs`
- Modify: `DataVo.Core/Models/Catalog/Column.cs`
- Modify: `DataVo.Core/StorageEngine/Serialization/RowSerializer.cs`
- Modify: `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`
- Modify: `DataVo.Core/Parser/DML/InsertRowService.cs`
- Modify: `DataVo.Core/Serialization/PolymorphicJsonWriter.cs`
- Modify: `DataVo.Core/Runtime/Reactive/DistinctReactiveQuery.cs`
- Modify: `DataVo.Core/Parser/DQL/Select.WindowHaving.cs`
- Modify: `DataVo.Core/Parser/DQL/UnionSelect.cs`
- Modify: `DataVo.Core/Parser/DDL/AlterTableModifyColumn.cs`
- Modify: `DataVo.Data/DataVoDataReader.cs`
- Modify: `DataVo.EntityFrameworkCore/Infrastructure/Internal/DataVoEntityMaterializer.cs`
- Modify: `DataVo.Tests/Storage/RowSerializerAllTypesRoundTripTests.cs`
- Modify: `DataVo.Tests/Storage/RowSerializerSpanTests.cs`
- Modify: `DataVo.Tests/Storage/RowSerializerTypedParityTests.cs`
- Modify: `DataVo.Tests/Storage/TypedReadParityTests.cs`
- Modify: `DataVo.Tests/Storage/IndexKeyEncoderTypedTests.cs`
- Modify: date assertions in `DataVo.Tests` discovered by `rg -n '\bDateOnly\b' DataVo.Tests`

- [ ] **Step 1: Add a legacy-byte compatibility test and canonical materialization assertions**

Before changing serialization, capture a representative pre-change DATE payload using the existing serializer and assert the exact bytes in a regression test. The new test must prove all three facts:

1. serializing the equivalent `DataVoDate` produces the same bytes;
2. dictionary and typed deserialization materialize `DataVoDate`;
3. reserializing the materialized value is byte-identical.

Also change date-bearing query and storage tests to expect `DataVoDate`. Keep separate modern adapter assertions for accepted `DateOnly` input.

Run the date/storage subset and confirm it fails against the old materialized type:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~RowSerializer|FullyQualifiedName~TypedReadParity|FullyQualifiedName~IndexKeyEncoderTypedTests|FullyQualifiedName~InsertTypedTests|FullyQualifiedName~AlterTableTests|FullyQualifiedName~ExpressionAggregateTests"
```

- [ ] **Step 2: Convert every common DATE path**

Use `DataVoDate` for parsing, comparison, formatting, dictionary values, compiled keys, JSON output, and typed rows. At the wire boundary, serialize with `dataVoDate.ToDateTime().ToBinary()` and deserialize with `DataVoDate.FromDateTime(DateTime.FromBinary(bits))`.

Keep accepting `DateOnly` on modern input through an explicit adapter, but never materialize it as the common row value. Update `DataVoDataReader.GetDateTime` to recognize `DataVoDate`. Update the EF materializer so a `DataVoDate` source converts correctly to `DataVoDate`, `DateOnly`, `DateTime`, and `DateTimeOffset` targets.

- [ ] **Step 3: Remove the BCL impersonation types**

Delete the public `namespace System` block containing `DateOnly` and `TimeOnly` from `NetStandard21Polyfills.cs`. Retain the internal `System.Runtime.CompilerServices` attributes. Remove the now-unused globalization import.

Prove no common Core source mentions the unavailable types:

```bash
rg -n '\b(DateOnly|TimeOnly)\b' DataVo.Core \
  --glob '!DataVoDate.Modern.cs' \
  --glob '!CellValue.Modern.cs' \
  --glob '!CompiledRowReader.Modern.cs' \
  --glob '!DateObjectAdapter.Modern.cs'
```

Expected: no matches. Any legitimate modern-only adapter found elsewhere must be moved into a clearly named whole target file and added to the exclusion list deliberately.

- [ ] **Step 4: Run the storage, consumer, EF, and portable build gates**

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~RowSerializer|FullyQualifiedName~TypedReadParity|FullyQualifiedName~IndexKeyEncoderTypedTests|FullyQualifiedName~InsertTypedTests|FullyQualifiedName~AlterTableTests|FullyQualifiedName~ExpressionAggregateTests"

dotnet build DataVo.Data/DataVo.Data.csproj -c Release
dotnet build DataVo.EntityFrameworkCore/DataVo.EntityFrameworkCore.csproj -c Release

dotnet build DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
```

`DataVo.Tests` references both consumer projects, so the focused test command compiles their API usage; the two direct builds make that dependency explicit.

- [ ] **Step 5: Commit storage/API parity**

Stage only the files changed for this task, including all migrated tests, then commit:

```bash
git commit -m "refactor: canonicalize SQL DATE materialization"
```

---

### Task 4: Reject unverified portable durable storage explicitly

**Files:**

- Create: `DataVo.Core/Compat/PortableStoragePolicy.Modern.cs`
- Create: `DataVo.Core/Compat/PortableStoragePolicy.Portable.cs`
- Modify: `DataVo.Core/StorageEngine/StorageContext.cs`
- Modify: `DataVo.Tests/StorageEngine/LsmStorageBackendConfigTests.cs`

**Task interface:**

```csharp
internal static class PortableStoragePolicy
{
    internal static void ThrowIfUnsupported(StorageMode mode);
}
```

- [ ] **Step 1: Preserve modern behavior with focused tests**

Keep the existing assertions that modern Disk and LSM modes resolve their current backends. Add an InMemory assertion if one does not already exist.

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~LsmStorageBackendConfigTests"
```

Expected: current modern tests pass. The portable rejection is not yet provable in this test project and will be exercised from the exact asset in Task 5.

- [ ] **Step 2: Add a statically selected platform policy**

Each policy file is guarded once at its file boundary. The modern method is a no-op. The portable method throws for Disk and LSM with a stable message naming the requested mode, the experimental `netstandard2.1` target, and `StorageMode.InMemory` as the supported candidate scope. Other modes continue to the existing switch.

Call the policy as the first statement in `ResolveStorageEngine`, before any backend constructor or filesystem side effect.

- [ ] **Step 3: Verify modern behavior and both Core targets**

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~LsmStorageBackendConfigTests"

dotnet build DataVo.Core/DataVo.Core.csproj -c Release -f net10.0
dotnet build DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
```

- [ ] **Step 4: Commit the storage boundary**

```bash
git add \
  DataVo.Core/Compat/PortableStoragePolicy.Modern.cs \
  DataVo.Core/Compat/PortableStoragePolicy.Portable.cs \
  DataVo.Core/StorageEngine/StorageContext.cs \
  DataVo.Tests/StorageEngine/LsmStorageBackendConfigTests.cs
git commit -m "fix: reject portable durable storage modes"
```

---

### Task 5: Validate the exact portable candidate package

**Files:**

- Create: `tools/DataVo.PortablePackageSmoke/DataVo.PortablePackageSmoke.csproj`
- Create: `tools/DataVo.PortablePackageSmoke/Program.cs`
- Create: `tools/DataVo.PortableConsumer/DataVo.PortableConsumer.csproj`
- Create: `tools/DataVo.PortableConsumer/PortableConsumer.cs`
- Create: `scripts/verify-portable-candidate.sh`

- [ ] **Step 1: Record the current package-validation failure**

```bash
PROBE_DIR="$(mktemp -d)"
dotnet pack DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -p:DataVoEnablePortableTarget=true \
  -p:PackageVersion=0.0.0-stage2a-red \
  -o "$PROBE_DIR"
```

Expected before Tasks 1-3: pack exits non-zero with API compatibility errors for public `System.DateOnly` and `System.TimeOnly`. After Tasks 1-3, this command is the first green package-validation proof.

- [ ] **Step 2: Add the clean portable consumer**

The `netstandard2.1` class library references `DataVo.Core` by `$(DataVoCandidateVersion)` and compiles calls to `DataVoDate`, `CellValue.From(DataVoDate)`, `AsDate()`, and an in-memory configuration. It must not use a project reference.

- [ ] **Step 3: Add the exact-assembly smoke host**

The net10 host references `DataVo.Core.dll` only through a required `$(DataVoPortableAssembly)` hint path and declares the portable asset's managed package dependencies. It must fail unless that property names an existing extracted DLL.

At runtime it must:

- assert the loaded `DataVo.Core` assembly path is the extracted candidate path;
- reject any exported type whose namespace is `System` or starts with `System.`;
- round-trip `DataVoDate` through `CellValue` and require `ToObject()` to return `DataVoDate`;
- create and dispose an in-memory `StorageContext`;
- execute one small in-memory schema/insert/select flow using stable public APIs already covered by repository examples;
- execute one Flat vector lookup and one reactive-query update if those APIs can be reached without unrelated package layers; otherwise keep these two checks in the later Unity fixture and state that limit in output;
- require Disk and LSM construction to throw `PlatformNotSupportedException` before creating their paths;
- print `Portable candidate .NET-host smoke passed.` only after every assertion succeeds.

- [ ] **Step 4: Add the isolated verifier script**

`scripts/verify-portable-candidate.sh` must:

1. create one temporary root containing package output, extracted files, consumer restore cache, smoke restore cache, and test storage paths;
2. generate a unique prerelease package version;
3. pack Core with `DataVoEnablePortableTarget=true` and package validation enabled;
4. require exactly these Core DLL entries:

   ```text
   lib/net10.0/DataVo.Core.dll
   lib/netstandard2.1/DataVo.Core.dll
   ```

5. extract the portable DLL from that exact `.nupkg`;
6. restore/build `DataVo.PortableConsumer` from the temporary local package source and isolated cache;
7. build/run `DataVo.PortablePackageSmoke` against the extracted DLL and isolated cache;
8. clean the temporary root on exit.

Do not call `scripts/assert-core-package-targets.sh`; it correctly rejects portable assets on the public release path.

- [ ] **Step 5: Run candidate and public-package verification**

```bash
bash scripts/verify-portable-candidate.sh
bash scripts/verify-local-packages.sh
```

Expected:

- candidate pack passes API validation;
- consumer compiles against `netstandard2.1` from the exact local package;
- exported-type ownership and runtime smoke pass against the extracted portable DLL;
- normal package verification still reports `net10.0` only.

- [ ] **Step 6: Commit the candidate verifier**

```bash
git add \
  tools/DataVo.PortablePackageSmoke/DataVo.PortablePackageSmoke.csproj \
  tools/DataVo.PortablePackageSmoke/Program.cs \
  tools/DataVo.PortableConsumer/DataVo.PortableConsumer.csproj \
  tools/DataVo.PortableConsumer/PortableConsumer.cs \
  scripts/verify-portable-candidate.sh
git commit -m "test: verify exact portable candidate package"
```

---

### Task 6: Publish the preview migration contract without claiming Unity support

**Files:**

- Modify: `README.md`
- Modify: `Directory.Build.props`
- Modify: `docs/fast-paths.md`
- Modify: `docs/source-generators.md`
- Modify: `docs/manual/storage-engine/query-planner-fast-paths.md`
- Modify: `docs/manual/client-interfaces/source-generators.md`
- Modify: `docs/features/setup-and-packaging.md`
- Modify: `docs/manual/preface/alpha-scope.md`
- Modify: `docs/features/unity-and-godot.md`
- Regenerate: documentation AI artifacts only if their curated sources changed

- [ ] **Step 1: Write the migration note**

Document these exact user-visible points:

- SQL `DATE` now materializes as `DataVo.Core.DataVoDate` in preview row APIs;
- `CellValue.AsDate()` and compiled `GetDate` now return `DataVoDate`;
- modern callers can use `FromDateOnly`, `ToDateOnly`, `AsDateOnly`, and `GetDateOnly`;
- persisted DATE bytes are unchanged;
- `DataVoEnablePortableTarget=true` still creates an experimental local candidate only;
- portable Disk/LSM are rejected; in-memory is the only Stage 3 candidate mode;
- passing the .NET-host verifier is not Unity Editor, Mono, IL2CPP, Jobs, or Burst proof.

Keep the existing public package statement `net10.0`-only and keep Unity marked unverified.

- [ ] **Step 2: Update examples and generated documentation**

Replace examples that describe `DateOnly` as the cross-target row type. Preserve modern-only examples when explicitly labeled as adapters.

If curated manual sources changed, run:

```bash
cd docs
npm run docs:ai
```

Return to the repository root before verification.

- [ ] **Step 3: Verify documentation truthfulness and buildability**

```bash
bash scripts/check-unity-support-status.sh
bash scripts/check-doc-api-samples.sh
cd docs && npm run docs:build
```

Expected: all commands pass and no page claims verified Unity support.

- [ ] **Step 4: Commit documentation and migration notes**

Stage only the intended documentation, release-note, and generated AI files, then commit:

```bash
git commit -m "docs: explain DataVoDate portable migration"
```

---

### Task 7: Run the Stage 2A release-quality gate

**Files:**

- Modify only files required to fix failures caused by Tasks 1-6.

- [ ] **Step 1: Prove both Core targets and package boundaries**

```bash
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -f net10.0
dotnet build DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true

bash scripts/verify-local-packages.sh
bash scripts/verify-portable-candidate.sh
```

- [ ] **Step 2: Run the complete affected test surface**

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj -c Release
dotnet build DataVo.Data/DataVo.Data.csproj -c Release
dotnet build DataVo.EntityFrameworkCore/DataVo.EntityFrameworkCore.csproj -c Release
dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release
```

Record any pre-existing unrelated failure; do not weaken tests or skip an affected project.

- [ ] **Step 3: Audit the result against the approved contract**

```bash
rg -n '^namespace System([.;{]|$)' DataVo.Core
rg -n '\b(DateOnly|TimeOnly)\b' DataVo.Core
rg -n 'DataVoEnablePortableTarget|netstandard2\.1|Unity|Burst' \
  README.md Directory.Build.props docs .github scripts DataVo.Core/DataVo.Core.csproj
git diff --check
git status --short
```

Expected:

- the namespace audit finds only internal compiler support under `System.Runtime.CompilerServices`, never exported compatibility types;
- BCL date mentions are confined to clearly named modern adapter files and modern-facing documentation;
- the public package remains quarantined;
- no unrelated untracked file is staged or modified;
- the worktree has no tracked changes after task commits.

- [ ] **Step 4: Record Stage 2A completion without over-claiming Stage 2**

In the final handoff, state that Stage 2A is complete and list the exact verification commands and commit range. Explicitly state that the umbrella Stage 2 remains open for modern fast-path restoration, compatibility seam isolation, HNSW/SIMD restructuring, quality-qualified benchmarks, and the later real Unity proof.
