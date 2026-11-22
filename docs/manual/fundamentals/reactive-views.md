---
title: Live Reactive Views
description: Register standing SQL or compiled LINQ queries and receive incremental Z-set IVM deltas with pull-drain dispatch and zero-allocation buffers.
---

# Live Reactive Views

Live Reactive Views let you register a standing SQL or compiled LINQ query and receive exact **incremental deltas** (`Added`, `Removed`, `Updated`) whenever underlying tables change, without polling or rescanning. Powered by **Z-set Incremental View Maintenance (IVM)** inspired by DBSP, every committed write emits signed tuple weights (`+1` insert, `-1` delete, `(-1, +1)` update) that update only affected keys or groups.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Game; USE Game;");
db.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR, region VARCHAR, score INT)");

using IDisposable sub = db.Subscribe(
    "SELECT region, COUNT(*) AS players, SUM(score) AS total FROM players GROUP BY region",
    change =>
    {
        foreach (var row in change.Added)
            Console.WriteLine($"+ {row["region"]}: total={row["total"]}");
        foreach (var row in change.Updated)
            Console.WriteLine($"~ {row["region"]}: total={row["total"]}");
    });

db.Execute("INSERT INTO players VALUES (1, 'Ada', 'EU', 120), (2, 'Lin', 'EU', 80)");
db.DispatchPendingNotifications(); // invokes callback on the calling thread
```

::: info DataVo Extension
Live Reactive Views (`Subscribe`, `SubscribeZeroAlloc`, `SubscribeInto`, and `DispatchPendingNotifications`) are a DataVo .NET-native IVM extension. Subscriptions return `IDisposable` directly; there is no separate `LiveView` wrapper class.
:::

## Choosing a Subscription API

| API | Input | Payload | Steady-State Allocation | Best For |
| --- | --- | --- | --- | --- |
| `db.Subscribe(sql, cb)` | Raw SQL | `QueryChange` (dictionaries) | Per-delta lists & dicts | Ad-hoc SQL (`JOIN`, `GROUP BY`, `UNION`, subqueries, `WITH RECURSIVE`) |
| `db.Subscribe(prepared, cb, params)` | `DataVoPreparedStatement` | `QueryChange` or `QueryChangeRef` | Skips SQL parsing | Multi-tenant / per-room subscriptions sharing one template |
| `db.SubscribeZeroAlloc(sql, cb)` | Raw SQL / prepared | `in QueryChangeRef` (`RowRef` spans) | Zero row/dict boxing | High-frequency SQL aggregates, Top-K, joins, recursive CTEs |
| `query.Subscribe(cb)` | Compiled `DataVoTable<T>` LINQ | `QueryChange<T>` | Typed models (AOT-safe) | Strongly-typed app services and UI state without SQL strings |
| `query.SubscribeInto(dest, onUpdated)` | Compiled LINQ + `Memory<T>` | Writes `dest.Span[0..count]` in place | **0 bytes** on dispatch | Real-time game HUDs, leaderboards, and frame-tick render loops |

## Pull-Drain Delivery Model

Notifications are **pulled**, never pushed from background threads or timers:

1. Committed writes append row deltas to an internal buffer (zero overhead when no subscriptions are active).
2. Nothing fires until your application calls `db.DispatchPendingNotifications()`.
3. `DispatchPendingNotifications()` evaluates net deltas for affected views and invokes callbacks **on the calling thread**, skipping empty deltas.

<div class="sketch-diagram">
  <img class="light-only" src="/manual/fig-04-zset-ivm-pull-light.svg" alt="Pull-drain Z-set IVM pipeline from committed writes to change buffer to DispatchPendingNotifications on the caller thread" />
  <img class="dark-only" src="/manual/fig-04-zset-ivm-pull-dark.svg" alt="Pull-drain Z-set IVM pipeline from committed writes to change buffer to DispatchPendingNotifications on the caller thread" />
</div>

Key operational guarantees:
- **Transaction-safe:** Rolled-back transactions emit zero notifications.
- **Reentrancy-deferred:** Writes issued inside a callback are buffered for the *next* `DispatchPendingNotifications()` call.
- **No baseline redelivery:** Subscribing seeds internal state from existing rows silently; only subsequent commits trigger callbacks.

## Zero-Allocation Buffered Windows (`SubscribeInto`)

For 60/120 FPS HUDs and leaderboards, project directly into a caller-owned `Memory<HudSlot>` with **0 bytes allocated** per dispatch:

```csharp
public struct HudSlot
{
    public int Id;
    public int Score;
}

var hudSlots = new HudSlot[3];
int liveCount = 0;

using IDisposable hudSub = db.Table<Player>()
    .Where(100, static (p, min) => p.Score >= min)
    .OrderByDescending(static p => p.Score)
    .Take(3)
    .Select(static p => new HudSlot { Id = p.Id, Score = p.Score })
    .SubscribeInto(hudSlots.AsMemory(), count => liveCount = count);

db.Insert(new Player { Id = 1, Score = 150 });
db.Insert(new Player { Id = 2, Score = 320 });
db.DispatchPendingNotifications(); // 0 B allocated; hudSlots[0..liveCount] updated in place
```

## Enter / Leave / Stay Semantics

| Mutation | Before Matches | After Matches | Emitted Delta |
| --- | --- | --- | --- |
| `INSERT` | — | Yes | `Added` |
| `DELETE` | Yes | — | `Removed` |
| `UPDATE` | Yes | No | `Removed` |
| `UPDATE` | No | Yes | `Added` |
| `UPDATE` | Yes | Yes | `Updated` (paired with `UpdatedBefore`) |

## Supported IVM Operators

Unsupported query shapes fail fast during `Subscribe` / `SubscribeZeroAlloc` with `NotSupportedException` and zero side effects.

| Query Shape | `Subscribe` | `SubscribeZeroAlloc` | Requirements / Notes |
| --- | --- | --- | --- |
| **Aggregates** (`GROUP BY`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) | Yes | Yes | Single table; O(1) for `COUNT`/`SUM`/`AVG`; no `HAVING` or CTEs |
| **Top-K** (`ORDER BY ... LIMIT k [OFFSET n]`) | Yes | Yes | Single table with a **primary key** |
| **Two-table equi-join** (`INNER`/`LEFT`/`RIGHT`/`FULL`) | Yes | Yes | Keyed DBSP arrangements; optional post-join `WHERE` |
| **Three-table VIP exposure** (`Orders × Accounts × Markets` + `GROUP BY`) | Yes | Yes | Specialized incremental 3-table exposure pipeline |
| **Recursive CTE** (`WITH RECURSIVE ... UNION ALL`) | Yes | Yes | Linear reachability over a table with a **primary key** |
| **Linear filter** (`SELECT ... [WHERE ...]`) | Yes | No | Single table; `=`, `!=`, `<`, `<=`, `>`, `>=`, `LIKE`, `IS [NOT] NULL`, `AND`/`OR` |
| **`SELECT DISTINCT`** | Yes | No | Single-table distinct multiplicity map |
| **`UNION` / `UNION ALL`** | Yes | No | Two single-table branches; no branch joins/aggregates or final `ORDER BY`/`LIMIT` |
| **Subqueries** (`IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`) | Yes | No | Uncorrelated or single-key equality correlation; outer table needs a **primary key** |

Window functions (`RANK() OVER`), `HAVING`, arbitrary 3+ table joins, self-joins, and correlated scalar-aggregate subqueries are not supported in reactive views.

::: details API Reference
```csharp
// DataVoContext SQL & Prepared Subscriptions
IDisposable Subscribe(string sql, Action<QueryChange> onChanged);
IDisposable Subscribe(DataVoPreparedStatement statement, Action<QueryChange> onChanged);
IDisposable Subscribe(DataVoPreparedStatement statement, Action<QueryChange> onChanged, params DataVoQueryParameter[] parameters);
IDisposable Subscribe(DataVoPreparedStatement statement, Action<QueryChange> onChanged, ReadOnlySpan<DataVoQueryParameter> parameters);
IDisposable Subscribe(DataVoPreparedStatement statement, Action<QueryChange> onChanged, IReadOnlyDictionary<string, object?> parameters);

IDisposable SubscribeZeroAlloc(string sql, QueryDeltaHandler onChanged);
IDisposable SubscribeZeroAlloc(DataVoPreparedStatement statement, QueryDeltaHandler onChanged);
IDisposable SubscribeZeroAlloc(DataVoPreparedStatement statement, QueryDeltaHandler onChanged, params DataVoQueryParameter[] parameters);
IDisposable SubscribeZeroAlloc(DataVoPreparedStatement statement, QueryDeltaHandler onChanged, ReadOnlySpan<DataVoQueryParameter> parameters);
IDisposable SubscribeZeroAlloc(DataVoPreparedStatement statement, QueryDeltaHandler onChanged, IReadOnlyDictionary<string, object?> parameters);

void DispatchPendingNotifications();
void SetMaxReactiveSubscriptions(int max);

// DataVoTable<T> & Composed LINQ Queries
IDisposable Subscribe(Action<QueryChange<T>> onChanged);
IDisposable SubscribeInto(Memory<TProjection> destination, Action<int> onUpdated);
```

`QueryChange` and `QueryChange<T>` expose `Added`, `Removed`, `Updated`, `UpdatedBefore`, and `IsEmpty`. `QueryChangeRef` (`readonly ref struct` passed to `QueryDeltaHandler`) exposes `Schema`, `RowSet` properties (`Added`, `Removed`, `Updated`, `UpdatedBefore`), `IsEmpty`, and `Materialize()`.
:::

## See Also

- [Roslyn Source Generators](/manual/client-interfaces/source-generators): Compile-time `IReactiveRowProjector<T>` generation for `SubscribeInto`
- [Streaming Queries](/manual/fundamentals/streaming-queries): Pull-based `yield return` and `StreamRef` table scans
- [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows): Memory layout of `RowRef` spans
