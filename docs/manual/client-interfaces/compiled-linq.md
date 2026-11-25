---
title: Compiled LINQ & Spatial Queries
description: Zero-reflection compile-time LINQ interceptors, columnar aggregations, 3D spatial queries, and reactive bindings on DataVoTable<T>.
---

# Compiled LINQ & Spatial Queries

`DataVoTable<T>` lets you query `[DataVoTable]` models with C# lambdas that Roslyn compiles into prepared queries and zero-allocation storage scans at build time. No expression trees are constructed and no reflection runs at runtime, making compiled LINQ safe for Native AOT and 60/120 FPS game loops.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE game; USE game;");
db.EnsurePlayerTable();

DataVoTable<Player> players = db.Table<Player>();

// Pass values as state with a static lambda: zero closure allocation
Player? top = players.FirstOrDefault(1, static (p, id) => p.Id == id);
List<Player> leaderboard = players
    .Where(500, static (p, min) => p.Score >= min)
    .OrderByDescending(static p => p.Score)
    .Take(10)
    .ToList();
```

::: info DataVo Extension
Compile-time LINQ interceptors, in-storage zero-allocation aggregates, and 3D spatial index queries (`WithinRadius`, `WithinBoundingBox`, `NearestNeighbors`) are `.NET`-native DataVo extensions powered by `DataVo.Generators`.
:::

## Enabling Interceptors

Installing the `DataVo.Generators` NuGet package enables C# 12 interceptors automatically. If you reference the generator via a project reference, add the generated namespace to your `.csproj`:

```xml
<PropertyGroup>
  <InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces>
</PropertyGroup>
```

On compilers without interceptors (such as Unity), the same code compiles and returns identical results via fallback scans; use generated `Get<Model>ById` methods or prepared statements on hot paths.

## Supported LINQ Operators

| Operator | Compiled Behavior |
| --- | --- |
| `Where` | Translates `==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `\|\|`, boolean flags, and `null` checks. Uses PK/B-Tree indexes when available; streams in 1,024-row pages otherwise. |
| `FirstOrDefault`, `SingleOrDefault` | PK equality on `Lsm` reads a single row (0 B on miss). |
| `Count`, `Any`, `Sum`, `Min`, `Max` | Compiles directly into the storage scan (or `[Columnar]` index) for numeric/bool/date/Guid columns (**0 B allocated** once warm). |
| `OrderBy`, `OrderByDescending`, `ThenBy`, `ThenByDescending` | Emits `ORDER BY`; combined with `Take`, retains only top-K rows during the scan. `string` keys order ordinally (as SQL `ORDER BY` does), not by the current culture; pass a comparer (`OrderBy(p => p.Name, comparer)`) to order in memory instead. |
| `Skip`, `Take` | Emits `OFFSET @offset LIMIT @limit` following `Enumerable` clamping semantics. |
| `Select` | Projects only the referenced columns (`SELECT name, score ...`) without instantiating full models. |
| `ExecuteDelete`, `ExecuteUpdate` | Emits single atomic `DELETE` or `UPDATE` statements (`Set`, `SetNull`, and `+`, `-`, `*` column arithmetic). |

### Zero-Allocation Aggregates & Bulk Updates

```csharp
// 0 B allocated on warm InMemory and Lsm tables (accelerated further if Score is [Columnar])
int elites = players.Where(1000, static (p, min) => p.Score >= min).Count();
long totalScore = players.Where(1000, static (p, min) => p.Score >= min).Sum(static p => p.Score);

// Atomic server-side bulk update
int boosted = players
    .Where(500, static (p, min) => p.Score >= min)
    .ExecuteUpdate(100, static (set, bonus) => set.Set(p => p.Score, p => p.Score + bonus));
```

### Build Diagnostics

Queries that cannot be compiled still return accurate results via an in-memory fallback (except `ExecuteUpdate`, which has no fallback) and emit a build diagnostic:

| Code | Severity | Cause |
| --- | --- | --- |
| `DATAVOL001` | Warning | Lambda captures a local or instance member; pass it as state with a `static` lambda. |
| `DATAVOL002` | Warning | Predicate uses an unsupported construct (method call, arithmetic, or cross-column comparison). |
| `DATAVOL003` | Warning | `InterceptorsNamespaces` does not include `DataVo.Generated`. |
| `DATAVOL004` | Warning | `OrderBy`/`ThenBy` key or `Select` projection falls back to in-memory execution. |
| `DATAVOL005` | Error | `ExecuteUpdate` cannot be compiled (captured variable, PK assignment, or unsupported expression). |

## 3D Spatial Queries

Any `System.Numerics.Vector3` property on a `[DataVoTable]` model is stored as `VECTOR(3)` and can be queried spatially:

```csharp
using System.Numerics;

var origin = new Vector3(0f, 0f, 0f);

// Stream players inside a 50-unit sphere (surface inclusive)
foreach (Player p in players.WithinRadius(origin, radius: 50f)) { }

// Stream players inside an axis-aligned bounding box
List<Player> inZone = players.WithinBoundingBox(new(-10, -10, -10), new(10, 10, 10)).ToList();

// 5 closest players (uses [SpatialIndex] Euclidean HNSW; pass exact: true for exact scan)
IReadOnlyList<Player> nearest = players.NearestNeighbors(origin, k: 5);
```

- `WithinRadius` and `WithinBoundingBox` test stored coordinates in double precision before allocating models, so non-matching rows allocate nothing.
- `NearestNeighbors` uses the `[SpatialIndex]` Euclidean HNSW index (`sx_<table>_<column>`) when present, or falls back to an exact streaming scan when `exact: true`, unindexed, or inside a transaction.
- When a model has multiple `Vector3` properties, pass `column: "PropertyName"`. Nullable `Vector3?` rows with `null` never match.

## Reactive Subscriptions

Composed `DataVoTable<T>` queries (`Where`, `OrderBy`/`ThenBy`, `Skip`, `Take`, `Select`) integrate directly with DataVo's Z-set incremental view engine:

- **`.Subscribe(Action<QueryChange<T>> onChanged)`** delivers strongly-typed `Added`, `Removed`, `Updated`, and `UpdatedBefore` deltas when `db.DispatchPendingNotifications()` drains committed writes.
- **`.SubscribeInto(Memory<T> destination, Action<int> onUpdated)`** writes the live window directly into a caller-owned buffer with **0 B** heap allocations per drain for compiled `.Select(...)` struct projections:

```csharp
public struct HudSlot { public int Id; public int Score; }

var hud = new HudSlot[3];
using var sub = players
    .Where(100, static (p, min) => p.Score >= min)
    .OrderByDescending(static p => p.Score)
    .Take(3)
    .Select(static p => new HudSlot { Id = p.Id, Score = p.Score })
    .SubscribeInto(hud.AsMemory(), count => RenderHud(hud.AsSpan(0, count)));
```

## See Also

- [Roslyn Source Generators](/manual/client-interfaces/source-generators): `[DataVoTable]` annotations, generated helpers, and `[DataVoQuery]`
- [Reactive Views](/manual/fundamentals/reactive-views): Pull-drain Z-set IVM model and choosing a subscription API
- [Compiled Scan Kernels](/manual/internals/compiled-scan-kernels): How zero-allocation scan predicates and columnar indexes work
