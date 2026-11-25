---
title: Introduction
description: Architecture overview of DataVo v0.4, a C#-native embedded database engine for .NET 10 with SQL, LSM storage, vector search, and reactive views.
---

# Introduction

DataVo is an embedded database engine written in C# for `.NET 10`. It runs directly inside your application process with no external server, no native DLLs, and full Native AOT compatibility, combining SQL, LSM storage, columnar indexes, vector search, and live reactive views in one library.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Demo");
db.Execute("USE Demo");
db.Execute("CREATE TABLE players (Id INT PRIMARY KEY, Name VARCHAR(80), Score INT)");
db.Execute("INSERT INTO players (Id, Name, Score) VALUES (1, 'Ada', 950)");

var result = db.Execute("SELECT Name, Score FROM players WHERE Id = 1")[0];
Console.WriteLine($"{result.Data[0]["Name"]}: {result.Data[0]["Score"]}");
```

## Choose Your Path

<div class="path-grid">
  <a class="path-card" href="./quickstart">
    <strong>Embedded .NET Engine</strong>
    <span>Start with in-memory or crash-safe LSM storage and run SQL in 60 seconds.</span>
  </a>
  <a class="path-card" href="../client-interfaces/source-generators">
    <strong>Roslyn Native AOT</strong>
    <span>Compile typed LINQ and <code>[DataVoQuery]</code> methods at build time with zero reflection.</span>
  </a>
  <a class="path-card" href="../fundamentals/reactive-views">
    <strong>Live Z-Set Reactive Views</strong>
    <span>Maintain SQL and LINQ views incrementally with pull-drain delta notifications.</span>
  </a>
  <a class="path-card" href="../fundamentals/datavo-sync">
    <strong>DataVo-Sync</strong>
    <span>Offline-first bidirectional PostgreSQL synchronization (commercial add-on).</span>
  </a>
</div>

## Architecture Overview

Every client interface—`DataVoContext`, compiled LINQ, ADO.NET, Dapper, and the EF Core bridge—routes into the same parser, typed query planner, and storage engines.

<div class="sketch-diagram">
  <img class="light-only" src="/manual/fig-01-architecture-light.svg" alt="DataVo architecture showing client interfaces, SQL parser, typed fast-path planner, and InMemory and LSM storage engines" />
  <img class="dark-only" src="/manual/fig-01-architecture-dark.svg" alt="DataVo architecture showing client interfaces, SQL parser, typed fast-path planner, and InMemory and LSM storage engines" />
</div>

### Storage Engines

| Mode | Backing Engine | Best For |
| --- | --- | --- |
| `StorageMode.InMemory` | Packed contiguous row buffers (`FlatRowStore`) | Ephemeral caches, tests, simulations, game state |
| `StorageMode.Lsm` | Commit log (WAL), arena MemTables, memory-mapped SSTables, 4,096-row columnar segments | Production embedded persistence with `LsmStrictFsync = true` |

### Typed Fast Paths & Vector Search

Queries are first offered to **typed execution paths** (single-table scans, integer-key hash joins, columnar aggregates, index-driven joins, and filtered vector search) that read cells straight from stored bytes without building per-row dictionaries. If a query shape is outside the fast paths, it falls back to the general evaluator with identical semantics.

`VECTOR(n)` columns are first-class relational columns backed by SIMD-accelerated `FLAT` (exact) or `HNSW` (approximate graph with cost-based exact fallback) indexes, keeping embeddings and metadata in one transaction log.

## Packages at a Glance

| Package | Purpose | Native AOT |
| --- | --- | --- |
| `DataVo.Core` | Embedded engine, `DataVoContext`, storage engines, vector & reactive APIs | Yes |
| `DataVo.Generators` | Roslyn source generator for `[DataVoTable]`, `[DataVoQuery]`, and compiled LINQ | Yes |
| `DataVo.Data` | Standard ADO.NET provider (`DataVoConnection`, `DataVoCommand`, `DataVoDataReader`) + Dapper | Yes |
| `DataVo.EntityFrameworkCore` | Experimental EF Core bridge (`UseDataVo(...)`); not a standalone provider | No |

## Where DataVo Fits and Where It Doesn't

- **Strong fit:** Local-first `.NET 10` apps, game tooling, simulations, and retrieval workloads that combine relational filtering with vector search. On warm caches, v0.4 outperforms SQLite on key lookups, low-cardinality `GROUP BY`, integer-key joins, columnar aggregates, and filtered vector search.
- **Not a fit:** Client/server deployments, multi-process writes to one directory (`datavo.lock` enforces single-process ownership), stored procedures/triggers, or serializable predicate locking. For whole-table analytics over tens of millions of rows, DuckDB remains orders of magnitude faster.
- **Need multi-device PostgreSQL sync?** [DataVo-Sync](/manual/fundamentals/datavo-sync) is a separate commercial add-on providing offline-first bidirectional sync over WebSocket/HTTP with wire v2 + Brotli.

## See Also

- [Installation & Packages](/manual/getting-started/installation): NuGet packages (`0.4.0-rc.1`) and `.NET 10` requirements.
- [Quickstart](/manual/getting-started/quickstart): End-to-end C# walkthrough with `InMemory` and `Lsm`.
- [DataVo vs SQLite](/manual/performance/datavo-vs-sqlite): Oracle-verified benchmarks across 28 SQL shapes and vector queries.
- [Status & Roadmap](/manual/getting-started/roadmap): Release candidate guarantees, boundaries, and the v0.5 plan.
