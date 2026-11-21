# Live Reactive Views (Z-set IVM)

Reactive queries let you register a standing `SELECT` statement and receive exact **incremental deltas** — which rows were **added**, **removed**, or **updated** — without polling or re-running the query against the entire table. Under the hood, DataVo implements **Z-set Incremental View Maintenance (IVM)** inspired by DBSP: every write emits signed tuple weights (`+1` for insert, `-1` for delete, paired `(-1, +1)` before/after images for update), and each registered view updates only the affected keys or groups.

## Public API

Four methods on `DataVoContext` expose reactive views:

```csharp
IDisposable Subscribe(string sql, Action<QueryChange> onChanged);
IDisposable SubscribeZeroAlloc(string sql, QueryDeltaHandler onChanged);
void DispatchPendingNotifications();
void SetMaxReactiveSubscriptions(int max);
```

- **`Subscribe(sql, onChanged)`** parses and validates `sql`, seeds internal state from existing table contents, and invokes `onChanged` with an owned `QueryChange` (`Added`, `Removed`, `Updated`, `UpdatedBefore`, `IsEmpty`). Dispose the returned handle to unsubscribe.
- **`SubscribeZeroAlloc(sql, onChanged)`** invokes `QueryDeltaHandler` (`delegate void QueryDeltaHandler(in QueryChangeRef change)`) on a **borrowed** `QueryChangeRef` backed by reusable spans — avoiding dictionary/list allocations on hot loops. Throws `NotSupportedException` cleanly (with no side effects) for operators not yet migrated to borrowed emit (see the [support matrix](#operator-routing-and-support-matrix) below).
- **`DispatchPendingNotifications()`** drains all buffered committed change sets through active subscriptions on the calling thread.
- **`SetMaxReactiveSubscriptions(max)`** caps concurrent active subscriptions (`Subscribe` throws `InvalidOperationException` once the cap is reached).

### Owned (`QueryChange`) vs Zero-Alloc (`QueryChangeRef`)

```csharp
public sealed class QueryChange
{
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Added { get; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Removed { get; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Updated { get; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> UpdatedBefore { get; }
    public bool IsEmpty { get; }
}
```

Each row in `QueryChange` is projected to the columns named in the `SELECT` list (or all columns for `SELECT *`).

For zero-allocation loops, `QueryChangeRef` is a stack-only `readonly ref struct` mirroring the same structure:

```csharp
public readonly ref struct QueryChangeRef
{
    public ReactiveRowSchema Schema { get; }
    public RowSet Added { get; }
    public RowSet Removed { get; }
    public RowSet Updated { get; }
    public RowSet UpdatedBefore { get; }
    public bool IsEmpty { get; }
    public QueryChange Materialize();
}
```

Each `RowSet` indexes into `RowRef` elements valid **only for the duration of the synchronous callback**. Call `change.Materialize()` (or copy individual primitives/strings out of `RowRef`) if you need to retain rows past the callback frame.

## Pull-Drain Delivery Model

Notifications are **pulled**, never pushed from background threads or timers:

1. On every successful `COMMIT` (or auto-commit `INSERT` / `UPDATE` / `DELETE`), committed row changes are appended to an internal change buffer.
2. Nothing is dispatched to callbacks until your application calls `DispatchPendingNotifications()`.
3. `DispatchPendingNotifications()` feeds buffered batches through affected subscriptions, collects net changes per subscription, and invokes callbacks (skipping empty deltas) **on the calling thread**.

This keeps callback execution deterministic — ideal for a frame tick in Unity/Godot, a UI dispatcher thread, or a simulation loop where callbacks must never contend with writer threads.

## Operator Routing and Support Matrix

`ReactiveRegistry` routes each subscribed `SELECT` to a specialized incremental operator by its parsed shape:

| Query shape | Internal operator | `Subscribe` | `SubscribeZeroAlloc` | Key requirements / notes |
| --- | --- | --- | --- | --- |
| **Aggregates** (`GROUP BY` or top-level `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) | `AggregateReactiveQuery` | Supported | Supported | Single `FROM` table; invertible ops (`COUNT`/`SUM`/`AVG`) update in O(1); `MIN`/`MAX` keep per-group multisets; no `HAVING` or CTEs. |
| **Top-K** (`ORDER BY ... LIMIT k [OFFSET n]`) | `TopKReactiveQuery` | Supported | Supported | Single `FROM` table with a **primary key**; maintains sorted matching set and diffs window shifts. |
| **Two-table equi-join** (`INNER`, `LEFT`, `RIGHT`, `FULL`) | `JoinReactiveQuery` | Supported | Supported | Both inputs held as keyed DBSP arrangements (`ΔR ⋈ S_old + R_new ⋈ ΔS`); emits hidden `__rid`/`__sid` output keys; optional post-join `WHERE`. |
| **Three-table VIP exposure shape** (`Orders × Accounts × Markets` `JOIN` + `GROUP BY`) | `VipExposureReactiveQuery` | Supported | Supported | Specialized fast incremental pipeline for the matched 3-table exposure pattern. |
| **Recursive CTE** (`WITH RECURSIVE ... UNION ALL`) | `RecursiveCteReactiveQuery` | Supported | Supported | Linear reachability over a table with a **primary key**; semi-naïve incremental on inserts, in-memory fixpoint diff on deletes/updates. |
| **Linear filter** (single-table `SELECT ... [WHERE ...]`) | `ReactiveSubscription` | Supported | Throws `NotSupportedException` | Supports `=`, `!=`, `<`, `<=`, `>`, `>=`, `LIKE`, `IS [NOT] NULL`, `AND`, `OR`; no joins or `HAVING`. |
| **`SELECT DISTINCT`** | `DistinctReactiveQuery` | Supported | Throws `NotSupportedException` | Single-table distinct tuple multiplicity map. |
| **`UNION` / `UNION ALL`** | `UnionReactiveQuery` | Supported | Throws `NotSupportedException` | Two single-table branches only; rejects branch joins, aggregates, wildcards, CTEs, and final `ORDER BY`/`LIMIT`. |
| **Subqueries** (`IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`) | `SubqueryReactiveQuery` | Supported | Throws `NotSupportedException` | Supports uncorrelated checks and single-key equality correlation (`S.K = R.K`); outer table requires a **primary key**. |

### Unsupported Query Shapes and Caveats

Unsupported constructs fail fast during `Subscribe` / `SubscribeZeroAlloc` with `NotSupportedException` and zero side effects (no hook enabled, no partial registration):

- Arbitrary three-or-more-table joins outside the matched VIP shape, non-equi join conditions, `OR` predicates spanning join inputs, and self-joins.
- Window functions (`RANK() OVER ...`) inside reactive queries.
- `HAVING`, final `ORDER BY`/`LIMIT` on `UNION`, `UNION` recursion in `WITH RECURSIVE`, mutual recursion, and correlated scalar-aggregate subqueries (`= (SELECT COUNT(*) ...)`).
- Tables without a primary key for operators that track row identity across out-of-place `UPDATE`s (`TopK`, `Subquery`, `RecursiveCte`).

## Examples

### Owned Deltas (`Subscribe`)

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
ctx.Execute("CREATE DATABASE Game; USE Game;");
ctx.Execute("CREATE TABLE Player (Id INT PRIMARY KEY, Name VARCHAR(50), Team VARCHAR(32), Score INT);");

// 1. Live GROUP BY leaderboard
using var teamBoard = ctx.Subscribe(
    "SELECT Team, COUNT(*) AS Players, SUM(Score) AS Total, MAX(Score) AS Best FROM Player GROUP BY Team",
    change =>
    {
        foreach (var row in change.Added)
            Console.WriteLine($"New team {row["Team"]}: total={row["Total"]}, best={row["Best"]}");
        foreach (var row in change.Updated)
            Console.WriteLine($"Updated {row["Team"]}: total={row["Total"]}, best={row["Best"]}");
        foreach (var row in change.Removed)
            Console.WriteLine($"Removed team {row["Team"]}");
    });

// 2. Live TOP-3 players
using var top3 = ctx.Subscribe(
    "SELECT Id, Name, Score FROM Player ORDER BY Score DESC LIMIT 3",
    change =>
    {
        foreach (var row in change.Added)
            Console.WriteLine($"Entered top 3: {row["Name"]} ({row["Score"]})");
        foreach (var row in change.Removed)
            Console.WriteLine($"Left top 3: id={row["Id"]}");
    });

// Commit writes, then drain deltas cleanly on the simulation/UI thread:
ctx.Execute("INSERT INTO Player VALUES (1, 'Ada', 'Red', 120), (2, 'Lin', 'Blue', 95);");
ctx.DispatchPendingNotifications();
```

### Zero-Allocation Deltas (`SubscribeZeroAlloc`)

Use `SubscribeZeroAlloc` when a reactive aggregate, top-K, or 2-table join runs every frame and GC pauses must stay near zero:

```csharp
using DataVo.Core.Runtime.Reactive;

using var sub = ctx.SubscribeZeroAlloc(
    "SELECT Team, COUNT(*) AS Players, SUM(Score) AS Total FROM Player GROUP BY Team",
    (in QueryChangeRef change) =>
    {
        int teamCol = change.Schema.GetOrdinal("Team");
        int totalCol = change.Schema.GetOrdinal("Total");

        RowSet updated = change.Updated;
        for (int i = 0; i < updated.Count; i++)
        {
            RowRef row = updated[i];
            ReadOnlySpan<char> team = row.GetStringSpan(teamCol);
            long total = row.GetInt64(totalCol);
            UpdateHudMetric(team, total);
        }
    });
```

## Enter / Leave / Stay Semantics

For each committed row mutation, rows are classified by whether their **before** and **after** images match the query:

| Change | Before matches | After matches | Output delta |
| --- | --- | --- | --- |
| Insert | — | yes | `Added` |
| Delete | yes | — | `Removed` |
| Update | yes | no | `Removed` |
| Update | no | yes | `Added` |
| Update | yes | yes | `Updated` (with `UpdatedBefore`) |
| Other | — | — | ignored |

## Operational Guarantees

- **Transaction-safe:** Rolled-back transactions produce zero notifications.
- **Reentrancy-deferred:** Writes made inside a callback are buffered for the **next** `DispatchPendingNotifications()` call rather than re-entering the active drain.
- **Opt-in capture:** Change capture overhead is zero while no subscriptions are registered.
- **No baseline redelivery:** `Subscribe` seeds internal operator state from existing rows silently; only subsequent committed mutations emit deltas.

## See Also

- [Streaming Queries (`StreamQuery` & `StreamRef`)](/manual/client-interfaces/streaming-queries) — pull-based `yield return` and zero-allocation scan streaming.
- [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows) — memory layout of `RowRef` spans.
- [SQL Compatibility Matrix](/manual/sql-language/sql-compatibility) — batch SQL vs reactive surface coverage.
