# Performance and Storage Design

This document explains the core performance direction of DataVo as a C#-native SQL engine.

## Goals

- Keep query execution predictable and profile-friendly.
- Keep storage layout efficient for both in-memory and disk modes.
- Keep vector and GIS workloads first-class, not plugin afterthoughts.

## Page-oriented storage model

DataVo follows a page-oriented design direction:

- fixed-size logical pages for table and index data,
- explicit serialization/deserialization boundaries,
- improved locality compared to object-heavy random allocations,
- easier durability and recovery primitives later.

Why this matters:

- fewer scattered allocations,
- better cache line utilization,
- clearer IO behavior for disk paths.

## Cache-aware execution (L1/L2 mindset)

Execution and data layout are being optimized to stay cache-friendly:

- reduce pointer chasing,
- prefer contiguous iteration patterns,
- keep hot metadata compact,
- avoid repeated name resolution in runtime loops (bind early, execute fast).

The practical target is to keep hot loops closer to L1/L2 cache characteristics and reduce stalls caused by random memory access patterns.

## SIMD-aware operations

DataVo is designed to adopt SIMD-friendly paths where useful:

- comparisons/filtering over numeric vectors,
- batch-style predicate checks,
- opportunities for vectorized distance and scoring primitives.

In .NET, this is expressed through JIT/AOT-generated native machine code and runtime intrinsics where appropriate.

## Why C#/.NET 10 here

DataVo is cross-platform by design (Windows/Linux/macOS) and benefits from modern .NET runtime improvements:

- high-performance managed runtime,
- native machine code generation via JIT and AOT pathways,
- strong tooling for diagnostics/profiling,
- predictable deployment model.

## Vector and GIS motivation

Using vector embeddings through SQLite in C# often depends on external extensions/plugins, which can make setup and debugging harder.

DataVo’s approach is to reduce this friction by building vector/GIS support into the engine architecture itself.

## Testing impact

Native in-memory and disk execution paths simplify automated testing:

- fewer external service dependencies,
- faster local and CI cycles,
- easier deterministic performance checks.

This is especially useful for embedding-related tests that otherwise depend on heavyweight external DB/container setup.
