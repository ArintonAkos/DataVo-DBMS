# Unity Slice B1: UPM Value-Type Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the authoritative UPM source package for allocation-aware `Unity.Mathematics` and `UnityEngine` float-vector/quaternion conversion to DataVo `VECTOR` cells, without adding Unity dependencies to Core or making an unverified Unity support claim.

**Architecture:** The `com.datavo.unity.mathematics` source package has two separately compiled assemblies. `DataVo.Unity.Mathematics` references only `DataVo.Core` and `Unity.Mathematics`; `DataVo.Unity.EngineTypes` references `DataVo.Core` and retains UnityEngine access for ordinary MonoBehaviour code. Both adapters call Slice A's Core-owned span APIs. The package is statically contract-tested here; a pinned Unity 6000.5.3f1 Editor compiles and executes it in the later smoke-project proof.

**Tech Stack:** Unity Package Manager source package, Unity 6000.5.3f1 (external verification gate), Unity Mathematics 1.3.3, C# source, Bash/Ripgrep static contract checks, DataVo.Core portable candidate.

## Global Constraints

- This slice implements only `com.datavo.unity.mathematics`; it does **not** add the Jobs package, Collections APIs, a smoke project, candidate extraction, a NuGet artifact, or a publish step.
- The next Jobs slice must derive the exact `com.unity.collections` lock entry from a clean Unity `6000.5.3f1` project. Do not invent or float that dependency version here.
- `DataVo.Core` remains Unity-free and default-targeted to `net10.0`; Unity will later load only an explicit `DataVoEnablePortableTarget=true` candidate.
- UPM is authoritative. This source package must not reference the unofficial UnityMathematics NuGet mirror or add any NuGet dependency.
- Fixed-size input adapters use stack storage and `CellValue.FromVector`, producing exactly one owned managed vector allocation. Output uses stack storage and `CellValue.CopyVectorTo`, with no temporary managed vector array.
- Support exactly `float2`, `float3`, `float4`, `quaternion`, `Vector2`, `Vector3`, `Vector4`, and `Quaternion`. Do not add matrices, colors, bounds, integer/bool vectors, Sentis/InferenceEngine tensors, or Core SQL types.
- Component order is exactly `x,y`, `x,y,z`, or `x,y,z,w`. Quaternion conversion never normalizes, canonicalizes, or changes signs.
- `SearchNearest` extensions exist for ordinary 2-, 3-, and 4-component vectors only. Do not add quaternion nearest-neighbor overloads: ordinary L2/cosine distance is not angular quaternion distance.
- Wrong `CellValue` type preserves the Core mismatch exception. A wrong vector dimension throws `ArgumentException` stating expected and actual component counts.
- Static source checks prove layout and API intent but are **not** Unity compilation, Mono execution, IL2CPP execution, Burst validation, or a public support claim.
- Preserve unrelated user-owned files. Use `apply_patch` for edits and stage exact paths only.

## Package Layout

```text
integrations/unity/com.datavo.unity.mathematics/
  package.json
  README.md
  Runtime/Mathematics/
    DataVo.Unity.Mathematics.asmdef
    UnityMathematicsCellExtensions.cs
  Runtime/EngineTypes/
    DataVo.Unity.EngineTypes.asmdef
    UnityEngineCellExtensions.cs
  Documentation~/README.md
scripts/
  test-unity-mathematics-package.sh
```

The package intentionally has no `.meta` assets yet because it has not been opened in the pinned Editor. The later smoke-project slice generates and source-controls Unity-owned metadata.

## Task 1: Define and test the UPM package contract

**Files:**
- Create: `scripts/test-unity-mathematics-package.sh`
- Create: `integrations/unity/com.datavo.unity.mathematics/package.json`
- Create: `integrations/unity/com.datavo.unity.mathematics/Runtime/Mathematics/DataVo.Unity.Mathematics.asmdef`
- Create: `integrations/unity/com.datavo.unity.mathematics/Runtime/EngineTypes/DataVo.Unity.EngineTypes.asmdef`
- Create: `integrations/unity/com.datavo.unity.mathematics/README.md`
- Create: `integrations/unity/com.datavo.unity.mathematics/Documentation~/README.md`

- [ ] **Step 1: Write a failing static contract check**

Write `scripts/test-unity-mathematics-package.sh` before the package files. It must fail when the package is absent, then validate with `rg` and JSON parsing that:

  - package name is exactly `com.datavo.unity.mathematics`, version is `0.1.0-preview.1`, and its only declared dependency is `com.unity.mathematics` `1.3.3`;
  - Mathematics asmdef is named `DataVo.Unity.Mathematics`, references only `DataVo.Core` and `Unity.Mathematics`, and has `noEngineReferences: true`;
  - EngineTypes asmdef is named `DataVo.Unity.EngineTypes`, references only `DataVo.Core`, and keeps Unity engine references enabled;
  - both extension source files and package documentation exist;
  - Mathematics source never contains `UnityEngine`, and neither adapter declares a quaternion `SearchNearest` overload.

Run:

```bash
bash scripts/test-unity-mathematics-package.sh
```

Expected: failure because the package tree does not exist.

- [ ] **Step 2: Add the minimal UPM metadata and documentation**

Use a source-package manifest with `unity: "6000.0"`, the single exact Mathematics dependency, and a description declaring the package experimental and dependent on a prepared portable DataVo.Core candidate. Documentation must state:

  - pinned Unity Editor/IL2CPP verification has not yet run;
  - no DataVo call may occur from Burst-compiled code;
  - conversion mappings and one-owned-allocation input behavior;
  - quaternion distance caveat and deliberate lack of quaternion search;
  - Jobs/native containers belong to `com.datavo.unity.jobs`, not this package.

Do not add a `DataVo.Core` DLL placeholder or asmdef; candidate preparation owns that generated fixture later.

- [ ] **Step 3: Make the package layout test green**

Run:

```bash
bash scripts/test-unity-mathematics-package.sh
```

Expected: static layout contract passes once source-API checks are added by Tasks 2 and 3.

- [ ] **Step 4: Commit the UPM skeleton**

Run `git add` with exactly the six Task 1 files, then commit:

```bash
git commit -m "feat: scaffold Unity Mathematics package"
```

## Task 2: Implement Unity Mathematics adapters

**Files:**
- Modify: `scripts/test-unity-mathematics-package.sh`
- Create: `integrations/unity/com.datavo.unity.mathematics/Runtime/Mathematics/UnityMathematicsCellExtensions.cs`

**Public surface:**

```csharp
namespace DataVo.Unity.Mathematics;

public static class UnityMathematicsCellExtensions
{
    public static CellValue ToDataVoCell(this float2 value);
    public static CellValue ToDataVoCell(this float3 value);
    public static CellValue ToDataVoCell(this float4 value);
    public static CellValue ToDataVoCell(this quaternion value);
    public static float2 AsFloat2(this CellValue value);
    public static float3 AsFloat3(this CellValue value);
    public static float4 AsFloat4(this CellValue value);
    public static quaternion AsQuaternion(this CellValue value);
    public static List<Dictionary<string, object?>> SearchNearest(
        this DataVoContext context, string tableName, string indexName, float2 query, int topK = 10);
    // matching float3 and float4 overloads only
}
```

- [ ] **Step 1: Extend the static test with failing Mathematics API assertions**

Require all eight conversion signatures, three ordinary-vector search signatures, stack allocation use, `CellValue.FromVector`, `CellValue.CopyVectorTo`, and `VectorLength` exact-dimension validation. Require explicit `x,y,z,w` mapping. Require that `quaternion` does not occur in a `SearchNearest` declaration.

Run:

```bash
bash scripts/test-unity-mathematics-package.sh
```

Expected: failure because Mathematics source does not exist.

- [ ] **Step 2: Implement direct Mathematics conversions and search**

Use `stackalloc float[2]`, `[3]`, or `[4]` fixed-size buffers for input, fill explicit component order, then call `CellValue.FromVector`. For output, call a private exact-length helper that reads `value.VectorLength`, throws `ArgumentException($"Expected VECTOR({expected}) but received VECTOR({actual}).", nameof(value))` for a dimension mismatch, copies into stack storage with `CopyVectorTo`, and constructs the requested type.

Each `SearchNearest` overload checks `context` for null, stack-copies its vector, and calls `DataVoContext.SearchNearest(tableName, indexName, ReadOnlySpan<float>, topK)`. Quaternion is conversion-only.

- [ ] **Step 3: Run the static contract and Core regression tests**

```bash
bash scripts/test-unity-mathematics-package.sh
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-build --no-restore --filter "FullyQualifiedName~CellValueVectorTests|FullyQualifiedName~VectorContextTests"
```

Expected: static check and existing Core vector regressions pass. The Core run proves imported API behavior, not Unity compilation.

- [ ] **Step 4: Commit the Mathematics adapters**

Stage exactly the script and Mathematics extension source, then:

```bash
git commit -m "feat: add Unity Mathematics vector adapters"
```

## Task 3: Implement UnityEngine adapters

**Files:**
- Modify: `scripts/test-unity-mathematics-package.sh`
- Create: `integrations/unity/com.datavo.unity.mathematics/Runtime/EngineTypes/UnityEngineCellExtensions.cs`

**Public surface:** the same conversion API for `UnityEngine.Vector2`, `Vector3`, `Vector4`, and `Quaternion`, plus `SearchNearest` extensions for `Vector2`, `Vector3`, and `Vector4` only.

- [ ] **Step 1: Extend the static test with failing EngineTypes API assertions**

Require `using UnityEngine;`, all eight conversion signatures, the three ordinary-vector search signatures, direct `CellValue.FromVector` and `CopyVectorTo` use, exact dimension validation, explicit x/y/z/w mapping, and no `SearchNearest(... Quaternion ...)`.

Run:

```bash
bash scripts/test-unity-mathematics-package.sh
```

Expected: failure because EngineTypes source does not exist.

- [ ] **Step 2: Implement direct EngineTypes conversions and search**

Mirror the Mathematics implementation in the EngineTypes namespace. Keep the helper assembly-local and do not reference the Mathematics assembly: the two adapters must remain independently importable.

- [ ] **Step 3: Run static, portable, and support-status verification**

```bash
bash scripts/test-unity-mathematics-package.sh
dotnet restore DataVo.Core/DataVo.Core.csproj -p:DataVoEnablePortableTarget=true
dotnet build DataVo.Core/DataVo.Core.csproj --no-restore -p:DataVoEnablePortableTarget=true
bash scripts/verify-portable-candidate.sh
bash scripts/check-unity-support-status.sh
```

Expected: static package and portable-Core checks pass. Existing Core nullable warnings may be recorded, but no new warnings are accepted. Support status must remain unverified pending Editor and player execution.

- [ ] **Step 4: Commit the EngineTypes adapters**

Stage exactly the script and EngineTypes extension source, then:

```bash
git commit -m "feat: add UnityEngine vector adapters"
```

## Task 4: Review and record the external Unity gate

**Files:**
- Modify: `docs/superpowers/plans/2026-07-20-unity-slice-b1-value-adapters.md`

- [ ] **Step 1: Inspect the working tree and static test**

```bash
git diff --check HEAD
git status --short
bash scripts/test-unity-mathematics-package.sh
```

Expected: no whitespace errors; only intended tracked work and pre-existing user-owned untracked files are present.

- [ ] **Step 2: Record completion boundaries**

Update this plan to mark implemented steps complete and record commands/results. Explicitly record that Unity Editor 6000.5.3f1, Mono, Windows x64 IL2CPP, Burst, and package import execution remain external gates.

- [ ] **Step 3: Commit the verified plan**

```bash
git add -f docs/superpowers/plans/2026-07-20-unity-slice-b1-value-adapters.md
git commit -m "docs: record Unity value adapter verification"
```

## Deferred Follow-up: Slice B2

Only after the pinned Unity 6000.5.3f1 Editor is available, derive and source-control exact Collections/Burst/Test Framework lock entries. Then implement `com.datavo.unity.jobs`, candidate preparation, a smoke project, Editor tests, and executed Windows x64 IL2CPP verification. Do not claim that work from this slice.
