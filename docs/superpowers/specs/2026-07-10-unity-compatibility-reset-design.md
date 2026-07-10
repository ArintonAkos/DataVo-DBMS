# Unity Compatibility Reset and Proof-First Portable Runtime Design

## Status

Approved on 2026-07-10.

This design supersedes `docs/superpowers/plans/2026-07-06-core-netstandard21-unity-compat.md` as the active direction for Unity and `netstandard2.1` work. The earlier plan remains useful historical context, but it must not be executed further in its current form.

## Purpose

DataVo should remain a modern, allocation-aware .NET database engine while developing a credible Unity integration path. Unity compatibility must be demonstrated with the final managed artifacts inside an actual Unity Editor and player build; compiling a `netstandard2.1` assembly or loading it from a .NET 10 benchmark host is not sufficient evidence.

The immediate goal is therefore to quarantine the unverified portable asset, restore the modern engine path, prove a deliberately small Unity use case, and only then decide whether to publish one multi-target Core package or design a smaller Unity-specific runtime.

## Current-State Findings

The current repository has several release blockers and design risks:

- `DataVo.Core` targets `net10.0;netstandard2.1`, but the `netstandard2.1` asset has not been imported into a tracked Unity project or exercised under Mono or IL2CPP.
- SDK package validation fails because the portable assembly publicly defines its own `System.DateOnly` and `System.TimeOnly` types while the modern assembly uses the BCL types.
- The portable file-overwrite fallback deletes the destination before moving the replacement, which does not preserve the atomic-publication contract used by durable storage.
- Compatibility work lowered some shared `net10.0` paths to a portable common denominator, including CRC32C, scalar-key formatting, and span-based decimal extraction.
- Target-specific implementations and preprocessor branches are mixed into large algorithm files, most visibly the HNSW and SIMD implementations.
- HNSW diversity was changed to default-off for build throughput, while the recall tests explicitly enable diversity and the end-to-end vector benchmark does not report recall.
- Batch HNSW insertion silently chooses parallel construction, which is a poor default for deterministic behavior and game-loop integration.
- The current Unity documentation describes an integration direction, but the repository has no Unity project, UPM package, assembly definition, player test, or validated dependency bundle.
- DataVo is a managed database engine built around classes, strings, dictionaries, arrays, locks, exceptions, and storage services. The whole engine is not a Burst-compatible job payload.

These findings do not invalidate the Unity product direction. They show that support claims and optimization work have moved ahead of the required integration proof.

## Goals

1. Prevent the current `netstandard2.1` asset from entering the next public package.
2. Restore `net10.0` as the primary, readable, optimized engine path.
3. Validate a final portable candidate in one pinned Unity project under Editor execution and a Windows x64 IL2CPP player.
4. Define an honest managed boundary between Unity Jobs/Burst code and DataVo.
5. Re-enable portable packaging only after public API, behavioral, packaging, Unity, and performance gates pass.
6. Preserve useful recent algorithmic work when evidence supports it; avoid a blanket revert.

## Non-Goals

- Making the complete SQL/database engine callable from Burst-compiled code.
- Replacing Unity's Job System with `Task`, `Parallel.For`, or DataVo-owned worker scheduling.
- Supporting production save data in Unity before disk durability is validated on each advertised platform.
- Adding arbitrary user-defined SQL storage types.
- Supporting consoles, mobile, Web, or every Unity version in the first proof.
- Creating a separate `DataVo.Unity` engine assembly before evidence shows that the full Core assembly is unsuitable.
- Adding new SQL syntax or unrelated storage-engine features.
- Optimizing the portable path from .NET-host benchmark results before Unity profiling exists.

## Delivery Slicing

This is an umbrella design with four sequential delivery stages, not one monolithic implementation plan. Each stage receives its own implementation plan, verification evidence, and review checkpoint. Stage 1 is planned and executed first. A later stage cannot start until the preceding stage's exit criteria are met or this design is explicitly revised.

## Governing Decisions

### Release quarantine, not deletion

The default build and public pack path returns to `net10.0` only. The portable target remains available through an explicit experimental build property so it can be repaired and tested, but normal `dotnet pack` and the NuGet publishing workflow must not include it.

The exact property name will be `DataVoEnablePortableTarget`. Its default is `false`. When `true`, the project also builds `netstandard2.1` for local compatibility checks and generation of non-published Unity candidate artifacts.

This preserves the work in one source tree without presenting an unverified asset to package consumers.

### Modern code is not lowered to the portable denominator

The `net10.0` implementation keeps modern BCL APIs, `TensorPrimitives`, explicit intrinsics where benchmarks justify them, Native AOT analyzer fences, and allocation-aware formatting and parsing.

Portable implementations live behind small DataVo-owned boundaries. Algorithms call those boundaries without containing interleaved target-framework branches. Where a boundary is used on a hot path, it must be statically bound or inlinable; virtual dispatch must not be introduced merely to hide conditional compilation.

### Public contracts are owned by DataVo

DataVo must not ship public replacement types in the `System` namespace. Compiler-only compatibility attributes may remain internal when required by the C# compiler.

SQL `DATE` will use a cross-target `DataVoDate` value type as its canonical public representation. It will store a day number, support equality, ordering, and invariant ISO `yyyy-MM-dd` formatting, and expose `FromDayNumber`/`DayNumber`. The modern target may offer conversions and convenience overloads for BCL `DateOnly`, but `DataVoDate` remains the stable cross-target contract.

All cross-target date-bearing APIs, including `CellValue`, compiled row readers, materialized row values, and generated mappers, use `DataVoDate`. `CellValue.ToObject()` returns `DataVoDate` for SQL `DATE` on every target. Modern `DateOnly` overloads are adapters into that contract, not an alternative stored or materialized type.

This is an intentional preview-stage API correction. Documentation and release notes must include the migration from public `DateOnly`-based row APIs.

### Unity support begins as a managed integration

DataVo runs outside Burst jobs. A Burst job may produce blittable commands or rows in a Unity native container. After its `JobHandle` completes, managed integration code drains the batch and calls DataVo.

The first proof supports `StorageMode.InMemory` only. On the experimental `netstandard2.1` target, disk and LSM modes must fail explicitly with `PlatformNotSupportedException` until their durability and platform contracts are separately validated. This rule does not depend on detecting Unity at runtime.

### Performance claims include quality and platform context

HNSW build time is not meaningful without recall. Every reported HNSW comparison must include recall@K, build time, query percentiles, allocation, vector count, dimension, construction settings, and whether construction is sequential or parallel.

Results produced by a .NET 10 host using the portable assembly must be labeled as portable-asset .NET-host measurements. They must not be described as Unity performance.

## Stage 1: Quarantine the Portable Asset

### Build and package behavior

- Default `DataVo.Core` build and pack output contains `net10.0` only.
- `DataVoEnablePortableTarget=true` adds the experimental `netstandard2.1` build.
- The tag-triggered NuGet workflow never sets this property.
- A package-content assertion verifies that public release packages contain no `lib/netstandard2.1` asset until Stage 4 is approved.
- `EnablePackageValidation` is enabled for pack operations.
- The manual netstandard benchmark lane is disabled or clearly marked experimental while the asset is quarantined.

### Documentation behavior

Documentation describes Unity as an evaluation target, not a supported runtime. The support table must explicitly say:

- Unity Editor: unverified until Stage 3
- Windows x64 IL2CPP: unverified until Stage 3
- Direct Burst calls: unsupported by design
- Job-to-managed batch bridge: planned proof
- In-memory mode: candidate scope
- Disk/LSM persistence: unsupported until separately validated

### Stage 1 exit criteria

- A normal package contains only `net10.0` Core assets.
- The publish workflow cannot accidentally include the portable target.
- Existing documentation no longer implies tested Unity or portable persistence support.
- The experimental target remains reproducibly buildable through its explicit property, or any remaining build failures are recorded as Stage 2 work rather than suppressed.

## Stage 2: Stabilize the Modern Path and the Experimental Boundary

### Restore modern fast paths

The implementation plan must audit the compatibility commit file by file and restore modern behavior where it was globally downgraded. At minimum it covers:

- the modern CRC32C implementation;
- allocation-aware scalar-key formatting;
- span-based decimal bit extraction;
- BCL guard APIs or a caller-name-preserving DataVo guard;
- modern `RandomAccess`, task cancellation, runtime detection, cryptography, locking, and formatting APIs;
- the existing `TensorPrimitives` and justified intrinsic paths.

Each restoration requires focused correctness coverage and an allocation or performance check proportional to the path's importance.

### Isolate compatibility implementations

Target-specific behavior moves into whole files selected by MSBuild or guarded once at the file boundary. The intended boundaries are:

- argument/disposal guards;
- monotonic clock access;
- task cancellation waiting;
- priority queue implementation;
- random-access file I/O and durable file publication;
- operating-system/runtime detection;
- password hashing and random-byte generation;
- CRC32C;
- vector distance and norm kernels.

DataVo-owned compatibility types must not be placed under BCL namespaces to impersonate unavailable framework types.

### HNSW structure and defaults

`HNSWIndex` becomes a partial class divided by responsibility without changing the public index abstraction:

- `HNSWIndex.cs`: public configuration, state, lifecycle, and entry points;
- `HNSWIndex.Construction.cs`: insertion, graph connection, pruning, and neighbor selection;
- `HNSWIndex.Search.cs`: greedy traversal, layer search, exact fallback, and distance routing;
- `HNSWIndex.Storage.cs`: capacity, pages, ordinals, vector storage, and graph storage;
- `HNSWIndex.Diagnostics.cs`: profiling-only counters and snapshots;
- `HNSWIndex.Platform.cs`: optional prefetch or platform-specific helpers.

Production builds must compile out construction diagnostics rather than execute a mutable enabled/disabled branch inside hot loops. Benchmark builds may opt in through an explicit MSBuild property.

The diagnostics property is `DataVoEnableHnswDiagnostics` and defaults to `false`.

The general HNSW default returns to the quality-preserving diversity behavior. A faster non-diverse build remains an explicit named configuration and must never be benchmarked under the unqualified `DataVo` label.

`InsertBatch` is deterministic and sequential by default. Parallel graph construction remains available only through an explicit API or option that records degree of parallelism in benchmark output.

### Vector kernel structure

`SimdDistanceKernels` becomes a small validation/routing facade. Modern and portable implementations live in separate files. Numerical parity tests cover cosine, dot product, and Euclidean distance, including zero vectors, non-multiple SIMD widths, and tolerance boundaries.

### Experimental portable API repair

The public `System.DateOnly` and `System.TimeOnly` replacements are removed before producing a Unity candidate. `DataVoDate` becomes the common SQL date contract. Package API validation must pass for any candidate `.nupkg` used by Stage 3.

The experimental target may remain slower, but it must preserve correctness contracts. Unsupported durable operations must fail explicitly; they must not silently implement weaker atomicity.

### Stage 2 exit criteria

- Full `net10.0` build and test suites pass.
- Targeted hot-path benchmarks show no unexplained regression from commit `2fb977d`, the direct pre-compatibility modern baseline.
- HNSW benchmark output includes recall@K and construction mode.
- The default HNSW recall@K is no worse than the previous diversity-enabled baseline by more than one percentage point on the fixed dataset.
- A candidate package built with `DataVoEnablePortableTarget=true` passes package API validation.
- No public compatibility type is declared under `System`.
- Target-framework branches do not remain interleaved through HNSW or distance algorithms.

## Stage 3: Prove the Managed Unity Integration

### Pinned smoke project

Add a tracked Unity smoke project pinned through `ProjectVersion.txt` to the latest available Unity 6.5 patch at implementation start. The pin must not float automatically.

The project consumes the exact experimental package artifact produced by Stage 2, not a project reference. A preparation script extracts the `netstandard2.1` DataVo assembly and its resolved managed dependencies into a local test-only UPM package with an assembly definition. This package is an integration fixture, not yet a public Unity distribution.

The fixture records every bundled dependency and fails preparation if two assemblies provide conflicting framework types or identities.

Unity-specific adapters, assembly definitions, job structs, and `Vector3`/`Quaternion` conversion code live in the test UPM fixture or a Unity bridge assembly. `DataVo.Core` does not take a dependency on Unity packages or `UnityEngine`.

### Required execution environments

The same smoke suite runs in:

1. Unity Editor execution, covering Unity's managed development environment.
2. A Windows x64 IL2CPP player with managed stripping set to Medium.

An Editor-only success is insufficient. An IL2CPP build without executing the generated player is also insufficient.

### Required smoke scenarios

The Unity proof covers:

- assembly import and dependency resolution;
- creation and disposal of an in-memory `DataVoContext`;
- database/table creation, typed insert, update, delete, and select;
- deterministic snapshot and restore;
- reactive subscription, write, and caller-controlled drain;
- Flat vector search;
- HNSW build and query with a small recall assertion;
- `Vector3` to `VECTOR(3)` and `Quaternion` to `VECTOR(4)` conversion through a thin adapter;
- repeated play-mode lifecycle to catch stale static state or undisposed resources;
- IL2CPP managed stripping/AOT behavior;
- baseline elapsed time and managed allocation capture in both Editor and player.

The first proof records measurements but does not create cross-platform performance promises from a single Windows player.

### Job System boundary proof

One smoke scenario uses a Burst-compatible job containing only blittable fields and Unity native containers. The job writes a batch of fixed-layout numeric/POD commands; it does not write strings or managed arrays. Managed code waits for completion, drains the batch, maps command fields to schema columns, converts Unity values, and calls DataVo.

No DataVo type is stored in the job struct, and no DataVo method is called from Burst-compiled code. This scenario defines the supported integration pattern for Jobs.

### Failure handling

- Missing or conflicting managed dependencies fail package preparation or Unity compilation.
- Unsupported storage modes throw `PlatformNotSupportedException` with the storage mode and runtime context.
- IL2CPP stripping or AOT failures block the proof; they are not worked around by disabling stripping globally.
- Unity test failures block portable publication.
- If the full assembly or dependency graph is the cause of failure, the team moves to a separate design for a smaller runtime rather than inserting more conditional behavior into common algorithms.

### Stage 3 exit criteria

- The tracked Editor suite passes from a clean project import.
- The Windows x64 IL2CPP player builds, launches, runs the smoke suite, and exits successfully.
- There are no missing assembly, type identity, reflection/AOT, stripping, or native binding failures in the supported in-memory scenarios.
- Flat and HNSW vector results meet their fixed correctness/recall assertions.
- The Job System bridge demonstrates that Burst produces data and managed code owns database calls.
- The measured allocations and latency are recorded with Unity version, backend, CPU, build configuration, and scenario parameters.

## Stage 4: Re-enable or Split Based on Evidence

### Preferred outcome: one multi-target Core

If Stage 3 passes without showing an unacceptable full-engine footprint, `DataVo.Core` returns to default multi-targeting with `net10.0;netstandard2.1`.

Before public release:

- whole-file platform adapters are the only target-specific implementation mechanism inside performance-sensitive subsystems;
- `DataVoDate` is documented as the cross-target SQL date type;
- both assets pass package API validation and consumer-compilation tests;
- the exact packed artifact is rerun through the Unity smoke project;
- the publish workflow requires the portable test matrix whenever the package includes `netstandard2.1`;
- package contents and transitive dependencies are recorded and reviewed;
- the support matrix names only environments and storage modes that were executed.

### Escalation outcome: a real runtime split

If Stage 3 demonstrates that the full Core assembly, disk/LSM surface, native bindings, or dependency graph is unsuitable for Unity, portable publication remains disabled. A new design must then define a genuine smaller runtime boundary.

A thin `DataVo.Unity` wrapper around the existing Core is not an acceptable split because it inherits the same assembly and dependencies. Any real split must define ownership of catalog, execution, storage, indexing, reactive queries, serialization, and public context APIs before implementation begins.

### Stage 4 exit criteria

One of two explicit outcomes is recorded:

1. `netstandard2.1` is re-enabled in the public package with all package and Unity gates green; or
2. it remains experimental, and a separately approved design scopes a smaller runtime.

There is no outcome where the target is published merely because it compiles.

## Artifact and Data Flow

The proof and release flow is:

1. Normal build produces and tests the `net10.0` engine.
2. An explicit experimental build produces the portable candidate.
3. Package validation compares public assets and rejects incompatible contracts.
4. The candidate `.nupkg` is placed in an isolated artifact directory.
5. The Unity preparation script extracts DataVo and resolved dependencies into the test UPM fixture.
6. Unity imports the fixture and runs Editor tests.
7. Unity builds the Windows x64 IL2CPP player from the same fixture.
8. The player executes smoke tests and writes machine-readable results.
9. Stage 4 either promotes the already-tested asset into the normal pack path or keeps it experimental.

No step substitutes a project reference, a different DLL, or a .NET-host benchmark result for the artifact used by Unity.

## Verification Strategy

### Modern engine verification

- full `DataVo.Tests` suite;
- Research benchmark correctness tests;
- Native AOT analyzer/build fence;
- targeted allocation tests for formatting, GUID cells, compiled queries, CRC, and vector kernels;
- HNSW recall/build/query benchmark with fixed seeds and recorded configuration;
- normal package-content assertion.

### Portable contract verification

- `netstandard2.1` build through `DataVoEnablePortableTarget=true`;
- package API validation;
- consumer compilation for a portable consumer and modern consumers that may select the portable asset;
- parity tests for dates, task cancellation, clocks, crypto, CRC, file operations, priority queues, and vector kernels;
- explicit unsupported-storage tests;
- dependency inventory and duplicate-type scan.

### Unity verification

- clean local package preparation;
- Unity Editor smoke tests;
- Windows x64 IL2CPP build and executed player tests;
- managed stripping set to Medium;
- machine-readable test, allocation, and timing artifacts;
- a checked-in support matrix generated from or manually reconciled with those results.

## Documentation and Benchmark Policy

Update the manual, README, package README, benchmark methodology, and Unity/Godot pages to distinguish:

- available package target;
- tested Unity version and backend;
- supported storage modes;
- direct Burst support versus the managed Jobs bridge;
- .NET-host portable measurements versus Unity player measurements;
- HNSW speed versus recall quality.

No documentation may use “Unity compatible,” “IL2CPP compatible,” “Burst compatible,” or “production save support” without the corresponding executed gate.

## Migration and Rollback

The latest published preview predates the current `netstandard2.1` work, so quarantining the asset does not remove a released portable contract.

Modern API corrections, including `DataVoDate`, are acceptable during preview but require release notes and compile-time migration examples.

If any stage fails:

- public packages remain `net10.0` only;
- the experimental property remains off by default;
- documentation retains evaluation wording;
- modern performance and correctness fixes remain independently releasable;
- Unity-specific work does not force weaker contracts into `net10.0`.

## Overall Success Criteria

- The next public package cannot contain the current unsafe portable asset.
- The modern engine regains its optimized APIs and has clearer compatibility boundaries.
- HNSW performance is reported with recall and explicit construction mode.
- A clean Unity project imports the exact candidate artifact and runs it in Editor and Windows x64 IL2CPP.
- The supported Jobs pattern is a Burst-to-managed data handoff, not direct database execution.
- Portable disk modes remain unavailable until their durability contracts are proven.
- Public multi-target packaging is restored only after API, behavior, artifact, Unity, and performance validation.
- A separate Unity runtime is considered only in response to measured footprint or compatibility failure.
