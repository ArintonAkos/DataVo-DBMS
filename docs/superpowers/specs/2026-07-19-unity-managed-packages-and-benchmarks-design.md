# Unity Managed Packages and Benchmarks Design

## Status

Approved in conversation on 2026-07-19. The Unity baseline was amended in
conversation on 2026-07-21 to use the locally installed `6000.5.4f1` Editor.

This design refines Stage 3 of
`docs/superpowers/specs/2026-07-10-unity-compatibility-reset-design.md`.
It does not undo the portable-asset quarantine or authorize public Unity support.
It splits the Unity proof so adapter, Jobs, reactive-query, and Flat-index work can
start while the HNSW structure/default/quality work remains queued for Stage 2C.
The original Stage 3 exit criteria are not complete until the later HNSW proof and
all required Unity environments pass.

## Purpose

Build an honest managed Unity integration around the exact experimental
`netstandard2.1` DataVo artifact, add small adapters for the Unity value types game
developers commonly use, prove the Burst-to-managed boundary, and record real
Unity performance without adding Unity dependencies to `DataVo.Core`.

The primary Unity distribution is UPM. A separate experimental
`DataVo.Unity.Mathematics` NuGet artifact is derived from the assembly compiled and
tested by Unity; it is not compiled against or dependent on the unofficial
`UnityMathematics` NuGet mirror.

## Governing Decisions

- `DataVo.Core` remains `net10.0` by default. Unity consumes only the explicit
  `DataVoEnablePortableTarget=true` candidate.
- The normal Core package and public publish workflow remain `net10.0`-only.
- Unity dependencies never enter `DataVo.Core`.
- `com.datavo.unity.mathematics` owns fixed-size Unity value conversion.
- `com.datavo.unity.jobs` owns native-container adapters and the explicit
  Burst-to-managed drain boundary.
- DataVo remains managed code. No DataVo type or method is used by Burst-compiled
  code.
- UPM is the authoritative Unity package form. The NuGet is a derived,
  Unity-hosted convenience artifact and remains unpublished during this proof.
- The first report covers adapters, native containers, Jobs, CRUD, snapshots,
  reactive queries, and Flat vector search. HNSW results are excluded until the
  deferred quality/determinism work is complete.
- Disk and LSM remain unsupported on the portable target and must reject before
  filesystem side effects.
- Host measurements of the portable assembly are labeled portable-asset .NET-host
  results, never Unity results.

## Goals

1. Import the exact Stage 2 portable artifact into a tracked Unity project.
2. Provide direct, allocation-aware adapters for the common Unity floating-point
   vector and quaternion types.
3. Support job-produced `NativeArray<float>` and `NativeSlice<float>` data without
   allowing Burst code to call DataVo.
4. Demonstrate a reusable unmanaged-command to managed-sink drain boundary.
5. Measure conversion, reactive maintenance, job drain, and Flat search in Unity
   Editor/Mono and an executed Windows x64 IL2CPP player.
6. Produce an experimental NuGet from the same Unity-compiled adapter assembly.
7. Preserve exact artifact provenance, dependency inventory, API ownership, and
   current support disclaimers.

## Non-Goals

- Calling DataVo from Burst.
- Supporting Sentis or `Unity.InferenceEngine.Tensor<T>` in this package.
- Adding matrices, colors, bounds, integer vectors, boolean vectors, or new SQL
  types in the first slice.
- Treating an integer Unity vector as a float vector and silently accepting
  precision loss above the exact float32 integer range.
- Defining quaternion angular distance or treating ordinary vector distance as
  rotation distance.
- Shipping Unity Disk/LSM save-data support.
- Publishing either Unity package to a public registry during the proof.
- Claiming Unity support from a .NET-host run, an Editor-only run, or an IL2CPP
  build that was not executed.
- Reporting HNSW performance before Stage 2C restores and qualifies its defaults.

## Pinned Environment

- Unity Editor: `6000.5.4f1`.
- Unity Mathematics: `com.unity.mathematics` `1.4.0`, resolved by the installed
  `6000.5.4f1` Editor as a built-in package.
- Burst: released `com.unity.burst` `1.8.26`.
- Test Framework: the core version fixed by Unity `6000.5.4f1`.
- Collections and other transitive Unity packages: resolve once from a clean
  `6000.5.4f1` project, commit the resulting `Packages/packages-lock.json`, and
  reject lock drift in verification.
- Editor proof: batch-mode Editor execution using Mono on macOS ARM64.
- Player proof: Windows x64 IL2CPP, Medium managed stripping, executed on Windows.

The exact project version and package lock are source-controlled. Package versions
do not float automatically.

## Package and Project Layout

```text
integrations/unity/
  com.datavo.unity.mathematics/
    package.json
    Runtime/Mathematics/DataVo.Unity.Mathematics.asmdef
    Runtime/Mathematics/UnityMathematicsCellExtensions.cs
    Runtime/EngineTypes/DataVo.Unity.EngineTypes.asmdef
    Runtime/EngineTypes/UnityEngineCellExtensions.cs
    Tests/Editor/
    Documentation~/
  com.datavo.unity.jobs/
    package.json
    Runtime/Burst/DataVo.Unity.Jobs.Burst.asmdef
    Runtime/Managed/DataVo.Unity.Jobs.Managed.asmdef
    Runtime/Managed/DataVoJobBridge.cs
    Tests/Editor/
    Documentation~/

tests/unity/DataVo.UnitySmoke/
  ProjectSettings/ProjectVersion.txt
  ProjectSettings/ProjectSettings.asset
  Packages/manifest.json
  Packages/packages-lock.json
  Assets/DataVoSmoke/Editor/
  Assets/DataVoSmoke/Runtime/
  Assets/DataVoSmoke/Jobs/
  Assets/DataVoSmoke/Benchmarks/

scripts/
  prepare-unity-candidate.sh
  run-unity-editor-tests.sh
  build-unity-windows-il2cpp.sh
  run-unity-windows-il2cpp.ps1
  pack-unity-mathematics-nuget.sh
```

The Unity `.gitignore` exceptions explicitly track source-package and smoke-project
`.meta` files, `Packages/manifest.json`, `Packages/packages-lock.json`, and the
required project settings. `Library`, `Temp`, `Logs`, build output, generated
candidate packages, extracted DLLs, and benchmark result files remain ignored.

## Artifact Preparation

`prepare-unity-candidate.sh` performs the following deterministic flow:

1. Restore and pack `DataVo.Core` with
   `DataVoEnablePortableTarget=true` into a fresh temporary directory.
2. Assert that the candidate package contains exactly the expected Core assets,
   including `lib/netstandard2.1/DataVo.Core.dll`.
3. Extract that exact portable assembly and its resolved managed dependencies into
   a generated local test-only Core UPM fixture.
4. Record package SHA-256, assembly SHA-256 values, assembly identities, package
   versions, and source commit in a machine-readable manifest.
5. Reject duplicate assembly identities, conflicting public framework types, or
   dependencies not present in the inventory.
6. Generate the candidate Core `.asmdef` references used by the smoke project.
7. Leave the normal public Core package and publish workflow unchanged.

The Unity project never substitutes a project reference, a workspace DLL, or a
different build for the exact prepared artifact.

## Core API Additions

The Unity adapters require small DataVo-owned APIs that are useful to all managed
consumers and do not mention Unity:

```csharp
public static CellValue FromVector(ReadOnlySpan<float> values);
public static CellValue TakeVectorOwnership(float[] values);
public int VectorLength { get; }
public void CopyVectorTo(Span<float> destination);

public List<Dictionary<string, object?>> SearchNearest(
    string tableName,
    string indexName,
    ReadOnlySpan<float> queryVector,
    int topK = 10);
```

Contracts:

- `FromVector` makes exactly one owned array copy and otherwise matches the
  existing `From(float[])` behavior.
- `TakeVectorOwnership` rejects `null`, stores the supplied array without cloning,
  and documents that the caller must never mutate the array after transfer.
- `VectorLength` requires a VECTOR cell.
- `CopyVectorTo` requires a VECTOR cell and a destination at least as large as the
  vector; it copies only `VectorLength` elements and never exposes the stored array.
- The span `SearchNearest` overload is synchronous and does not retain the span.
  Built-in Flat and HNSW indexes receive a span through an internal capability;
  a third-party index that lacks the capability receives a temporary array
  fallback without changing the public `IVectorIndex` interface.
- Existing array APIs remain source- and binary-compatible and forward to the new
  common implementation where practical.

These additions are test-first and verified on both Core targets before Unity
package work depends on them.

## Unity Value Adapters

### Unity Mathematics

`DataVo.Unity.Mathematics` provides:

```csharp
CellValue ToDataVoCell(this float2 value);
CellValue ToDataVoCell(this float3 value);
CellValue ToDataVoCell(this float4 value);
CellValue ToDataVoCell(this quaternion value);

float2 AsFloat2(this CellValue value);
float3 AsFloat3(this CellValue value);
float4 AsFloat4(this CellValue value);
quaternion AsQuaternion(this CellValue value);
```

It also provides `DataVoContext.SearchNearest` extension overloads for `float2`,
`float3`, and `float4`. It deliberately does not add a quaternion nearest-neighbor
overload.

### UnityEngine

`DataVo.Unity.EngineTypes` provides the equivalent direct API for:

- `UnityEngine.Vector2`;
- `UnityEngine.Vector3`;
- `UnityEngine.Vector4`;
- `UnityEngine.Quaternion`.

The direct API keeps ordinary MonoBehaviour code ergonomic even though Unity
Mathematics offers conversions for these types.

### Exact mappings

- two-component vectors: `x, y`;
- three-component vectors: `x, y, z`;
- four-component vectors: `x, y, z, w`;
- quaternions: `x, y, z, w`.

Quaternion conversion never normalizes, canonicalizes, or changes signs. The
documentation warns that `q` and `-q` represent the same rotation but are different
ordinary vectors, so cosine/L2 distance is not quaternion angular distance.

Fixed-size input adapters use stack storage plus `CellValue.FromVector`, producing
one owned vector allocation. Fixed-size output adapters stack-copy through
`CopyVectorTo` and create no temporary managed array. A wrong cell type retains the
Core mismatch error; a wrong vector dimension throws an `ArgumentException` that
reports expected and actual dimensions.

## Native Containers and Jobs Boundary

`com.datavo.unity.jobs` supports:

- `NativeArray<float>` to `CellValue` through its read-only span;
- `NativeSlice<float>` to `CellValue`, including strided slices;
- copying a vector cell into an existing `NativeArray<float>` or
  `NativeSlice<float>` of sufficient length;
- span-based nearest-neighbor search from contiguous native arrays;
- an unmanaged command batch produced by Jobs and drained by managed code.

Contiguous `NativeArray<float>` input reaches `CellValue.FromVector` without a
temporary managed array. A strided `NativeSlice<float>` is copied into one managed
array and transferred with `TakeVectorOwnership`; it never creates a second clone.

The reusable managed boundary is:

```csharp
public interface IManagedDataVoCommandSink<TCommand>
    where TCommand : unmanaged
{
    void Apply(in TCommand command);
}

public static int CompleteAndDrain<TCommand, TSink>(
    JobHandle handle,
    NativeList<TCommand> commands,
    ref TSink sink)
    where TCommand : unmanaged
    where TSink : struct, IManagedDataVoCommandSink<TCommand>;
```

The Burst assembly contains only unmanaged command definitions/helpers and has no
reference to DataVo. The managed assembly owns the sink interface and drain. It
calls `JobHandle.Complete()` before reading the list, applies commands in list
order, returns the number applied, and does not dispose or clear caller-owned
containers.

If command application fails, the original exception propagates immediately.
Commands before the failing index remain applied; later commands are not applied.
There is no hidden retry or transaction. Consumers needing atomic behavior must
use an explicit DataVo transaction or snapshot/restore policy.

## Experimental NuGet Artifact

After the Unity Editor compiles and tests the Mathematics and EngineTypes
assemblies, `pack-unity-mathematics-nuget.sh` packages those exact DLLs as:

```text
DataVo.Unity.Mathematics 0.1.0-preview.1
```

The package contains the two adapter assemblies, XML documentation, README,
license, Unity package/version requirements, and hashes linking them to the UPM
proof. It is a Unity-hosted adapter: Unity supplies `UnityEngine` and
`Unity.Mathematics`. It is not advertised as a general .NET package and declares no
dependency on the unofficial `UnityMathematics` NuGet package.

The Jobs package remains UPM-only in this stage because Burst and Collections are
Unity-hosted dependencies. Neither Unity artifact is added to the normal NuGet
publish workflow before the later promotion decision.

## Smoke Scenarios

The same prepared artifact and adapter source cover:

1. package import and dependency resolution;
2. in-memory `DataVoContext` creation/disposal;
3. database and table creation;
4. typed insert, update, delete, and select;
5. snapshot capture and deterministic restore;
6. reactive subscription, write, and caller-controlled notification drain;
7. Mathematics and UnityEngine fixed-vector round trips;
8. NativeArray and strided NativeSlice round trips;
9. Flat vector build and exact nearest-neighbor query;
10. Burst job production of unmanaged commands followed by managed DataVo drain;
11. repeated play-mode enter/exit cycles to expose stale static state and leaks;
12. explicit portable Disk/LSM rejection before directory creation;
13. Medium-stripping IL2CPP execution and machine-readable result output.

## Benchmark Method

Each timed scenario has an untimed correctness setup, warmup, five fresh process
runs, live checksums, and invariant JSON output. Elapsed time is informational;
correctness and ownership/allocation contracts are asserted without brittle
wall-clock thresholds.

### Adapter microbenchmarks

- Fixed vectors: 10,000 warmup conversions followed by 1,000,000 measured
  conversions per process.
- Native vector: 384 floats, 1,000 warmups, then 10,000 measured conversions per
  process.
- Compare the new adapter with the explicit legacy pattern
  `new float[] { ... }` followed by `CellValue.From(array)`.
- Record elapsed time, throughput, allocated bytes/op, GC collections, and a
  component checksum.

### Reactive inventory

- Seed 10,000 inventory rows with a fixed random seed.
- Run 50,000 single-row quantity mutations.
- Reactive path: maintain the selected aggregate/view and drain notifications.
- Polling path: rerun the equivalent full query after every mutation.
- Record mutation and read/drain time separately, p50/p95/p99/max, managed
  allocation, GC collections, and 60/120 Hz frame-budget misses.
- Both paths must produce identical final rows and aggregates.

### Jobs bridge

- Produce 1,000 unmanaged commands per batch for 100 batches.
- Record Burst scheduling/completion separately from managed command drain and
  database work.
- Compare with the same commands produced and applied entirely by managed code.
- Record command order checksum, rows applied, p50/p95/p99/max, allocation, and
  frame-budget misses.

### Flat vector search

- Build 10,000 deterministic `float3` positions.
- Run 1,000 deterministic top-10 queries.
- Verify every result against a scalar exact oracle.
- Record build time, query p50/p95/p99/max, allocation, metric, vector count,
  dimension, top-K, backend, and checksum.

HNSW is absent from the first benchmark report. It is added only after Stage 2C,
with recall@K against Flat, construction settings, sequential/parallel mode, build
time, query percentiles, and allocation.

## Measurement Environments and Labels

Three result profiles are distinct:

1. portable-asset .NET-host baseline;
2. Unity Editor/Mono on macOS ARM64;
3. executed Windows x64 IL2CPP player with Medium stripping.

Every JSON result records DataVo commit and package hash, Unity version, package
lock hash, scripting backend, managed stripping level, Burst enabled state, OS,
CPU, architecture, process bitness, configuration, scenario parameters, and UTC
timestamp.

This workstation has Unity `6000.5.4f1` on macOS ARM64. Host preparation,
portable-host baselines, and Editor/Mono evidence use that exact version. Windows
IL2CPP execution still requires a Windows runner with the pinned Editor/module and
an activated Unity license. Missing environment or licensing is reported as blocked
evidence, not converted into a support claim.

## Verification Gates

### Core

- Focused tests for every new vector ownership/span API run red then green.
- Full modern `DataVo.Tests` passes.
- Core builds for `net10.0` and explicit `netstandard2.1`.
- Exact portable package smoke remains green.
- Normal package remains `net10.0`-only.

### Package and adapter

- Exact nupkg and assembly hashes match the candidate manifest.
- Dependency inventory and duplicate identity/type scans pass.
- Every component/order mapping round-trips.
- Wrong type and wrong dimension behavior is deterministic.
- Source mutation cannot alter an owned `CellValue`.
- Fixed-size input performs one owned array allocation.
- Fixed-size output creates no temporary managed array.
- NativeArray and contiguous/strided NativeSlice cases preserve order and length.
- Quaternion tests prove no normalization or sign canonicalization occurs.

### Jobs

- Burst asmdefs contain no DataVo reference.
- Command types satisfy the unmanaged constraint.
- Drain occurs only after completion and preserves list order.
- Failure stops at the failing command with earlier commands visible.
- Caller-owned native containers are neither cleared nor disposed.

### Unity execution

- Clean Editor import and compilation pass.
- EditMode and PlayMode suites pass in batch mode.
- Repeated play-mode lifecycle passes without stale state.
- Windows x64 IL2CPP builds with Medium stripping.
- The generated player launches, runs the smoke/benchmark suite, writes valid JSON,
  and exits with success.
- Disk/LSM reject without creating their requested directories.

### Packaging

- UPM package manifests and asmdefs pass Unity validation.
- The derived NuGet DLL hashes equal the Unity-tested DLL hashes.
- The normal public NuGet workflow cannot include either experimental Unity
  package.
- Documentation continues to label Unity support unverified until all required
  proof gates pass.

## Failure Policy

- Dependency conflict, missing assembly, duplicate framework type, API mismatch,
  IL2CPP/AOT error, stripping error, or player execution failure blocks the proof.
- Tests do not disable stripping globally to hide linker problems.
- Performance regressions are reported with raw evidence and scenario context; the
  implementation is not tuned from a single noisy run.
- If full portable Core import proves unsuitable because of footprint, dependency
  graph, or IL2CPP behavior, portable publication stays disabled and a new smaller
  runtime design is required.
- If the adapter NuGet cannot be reproduced from the Unity-tested DLLs, only the
  UPM artifact remains; no substitute DLL is published.

## Delivery Slices

### Slice A: Core vector interop API

Implement and verify the DataVo-owned span, copy, ownership-transfer, and search
overloads without any Unity dependency.

### Slice B: UPM packages and exact candidate preparation

Create the Mathematics/EngineTypes package, Jobs package, candidate preparation,
dependency manifest, smoke project, source-control exceptions, and Unity tests.

### Slice C: Editor proof and adapter NuGet

Install/use the pinned Editor, run clean Editor import/tests/benchmarks, then derive
the experimental NuGet from the exact Unity-tested adapter DLLs.

### Slice D: Windows IL2CPP proof

Build on Windows, execute the player, validate JSON evidence, and compare Editor
and player results without turning them into cross-platform promises.

### Slice E: HNSW completion

After Stage 2C, add quality-qualified HNSW recall/build/query evidence to both Unity
environments and reassess portable promotion.

## Exit Criteria

This design's initial implementation is complete when Slices A through D pass or
when an unavailable external Unity environment is recorded as the only remaining
blocked evidence after all repository work is complete. In the latter case Unity
support remains unverified and no experimental package is published.

The broader Stage 3 proof remains open until Slice E and the original HNSW exit
criteria pass. Public portable/Core or Unity package promotion remains a separate,
explicit Stage 4 decision.

## Authoritative References

- Unity .NET profile support:
  https://docs.unity3d.com/6000.0/Documentation/Manual/dotnet-profile-support.html
- Unity custom package guidance:
  https://docs.unity3d.com/Manual/CustomPackages.html
- Unity Mathematics `1.4.0`:
  https://docs.unity3d.com/Packages/com.unity.mathematics@1.4/manual/index.html
- Unity Burst compilation boundary:
  https://docs.unity3d.com/Manual/script-compilation-burst.html
- Unity `6000.5.4f1` release:
  https://unity.com/releases/editor/whats-new/6000.5.4f1
- Sentis/Inference tensor behavior deferred from this design:
  https://docs.unity3d.com/Packages/com.unity.ai.inference@2.4/manual/tensor-fundamentals.html
