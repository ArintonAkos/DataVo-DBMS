# Live Reactive Views (Z-set IVM)

Reactive queries let you register a standing `SELECT` statement or typed LINQ query and receive exact **incremental deltas** — which rows were **added**, **removed**, or **updated** — without polling or re-running the query against the entire table. Under the hood, DataVo implements **Z-set Incremental View Maintenance (IVM)** inspired by DBSP: every write emits signed tuple weights (`+1` for insert, `-1` for delete, paired `(-1, +1)` before/after images for update), and each registered view updates only the affected keys or groups.

## Choosing a Subscription API

DataVo offers five reactive subscription entry points spanning ad-hoc SQL through zero-allocation, source-generated game-loop buffers:

| Subscription API | Input | Callback Payload | Steady-State Allocation | Type Safety & Native AOT | Best Use Case |
| --- | --- | --- | --- | --- | --- |
| `ctx.Subscribe(sql, onChanged)` | Raw SQL string | `QueryChange` (owned dictionaries) | Allocates per-delta lists & dictionaries | Dynamic SQL; AOT-safe | Exploratory dashboards, ad-hoc SQL (`JOIN`, `GROUP BY`, `UNION`, subqueries, `WITH RECURSIVE`) |
| `ctx.SubscribeZeroAlloc(sql, onChanged)` | Raw SQL string | `in QueryChangeRef` (borrowed spans) | Allocation-light (zero row/dict boxing) | Dynamic SQL; AOT-safe | High-frequency SQL aggregates, Top-K, joins, and recursive CTEs without materializing dictionaries |
| `ctx.Subscribe(prepared, ...)` / `ctx.SubscribeZeroAlloc(prepared, ...)` | Pre-parsed `DataVoPreparedStatement` + parameters | `QueryChange` or `in QueryChangeRef` | Bypasses SQL parsing; isolated AST per binding | Parameterized SQL; AOT-safe | Multi-tenant / per-team / per-room subscriptions sharing one parameterized query template |
| `db.Table<T>()...Subscribe(onChanged)` | Typed LINQ (`Where`, `OrderBy`, `Take`, `Select`) | `QueryChange<T>` (strongly-typed models/projections) | Typed arrays/models; skips intermediate dictionaries on Top-K | Compile-time checked; 100% Native AOT | Application logic, services, and UI models wanting strongly-typed incremental deltas without SQL strings |
| `db.Table<T>()...Select(...).SubscribeInto(dest, onUpdated)` | Typed LINQ + caller-owned `Memory<TProjection>` | Writes `dest.Span[0..count]` in place, calls `onUpdated(count)` | **0 bytes** on `DispatchPendingNotifications()` (primitive struct projections) | Source-generated `IReactiveRowProjector<T>`; 100% Native AOT | Real-time HUDs, leaderboards, and frame-tick render loops where GC pauses must be strictly zero |

## Public API

`DataVoContext` exposes SQL and prepared-statement reactive subscriptions:

```csharp
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
```

Strongly-typed, zero-SQL subscriptions are available on `db.Table<T>()` and composed LINQ queries (`Where`, `OrderBy`/`ThenBy`, `Skip`, `Take`, `Select`):

```csharp
IDisposable Subscribe(Action<QueryChange<T>> onChanged);
IDisposable SubscribeInto(Memory<TProjection> destination, Action<int> onUpdated);
```

### Owned (`QueryChange` / `QueryChange<T>`) vs Borrowed (`QueryChangeRef`)

```csharp
public sealed class QueryChange
{
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Added { get; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Removed { get; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Updated { get; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> UpdatedBefore { get; }
    public bool IsEmpty { get; }
}

public sealed class QueryChange<T>
{
    public IReadOnlyList<T> Added { get; }
    public IReadOnlyList<T> Removed { get; }
    public IReadOnlyList<T> Updated { get; }
    public IReadOnlyList<T> UpdatedBefore { get; }
    public bool IsEmpty { get; }
}
```

For borrowed SQL deltas, `QueryChangeRef` is a stack-only `readonly ref struct` mirroring the same structure:

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

Each `RowSet` indexes into `RowRef` elements valid **only for the duration of the synchronous callback**. Call `change.Materialize()` (or copy primitives/strings out of `RowRef`) if you need to retain rows past the callback frame.

## Pull-Drain Delivery Model

Notifications are **pulled**, never pushed from background threads or timers:

1. On every successful `COMMIT` (or auto-commit `INSERT` / `UPDATE` / `DELETE`), committed row changes are appended to an internal change buffer.
2. Nothing is dispatched to callbacks until your application calls `DispatchPendingNotifications()`.
3. `DispatchPendingNotifications()` feeds buffered batches through affected subscriptions, computes net changes per subscription, and invokes callbacks (skipping empty deltas) **on the calling thread**.

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

## Examples & Usage Guides

### 1. SQL Owned Deltas (`ctx.Subscribe(sql, onChanged)`)

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
ctx.Execute("CREATE DATABASE Game; USE Game;");
ctx.Execute("CREATE TABLE Player (Id INT PRIMARY KEY, Name VARCHAR(50), Team VARCHAR(32), Score INT);");

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

ctx.Execute("INSERT INTO Player VALUES (1, 'Ada', 'Red', 120), (2, 'Lin', 'Blue', 95);");
ctx.DispatchPendingNotifications();
```

**How it works & when to use it:** Parses `sql` once, compiles it into the matching incremental operator (linear filter, aggregate, top-K, join, distinct, union, subquery, or recursive CTE), seeds its state silently from existing rows, and delivers owned `QueryChange` dictionaries on each `DispatchPendingNotifications()`. Use it whenever you want full SQL expressiveness (such as multi-table joins, `GROUP BY`, or `WITH RECURSIVE`) and ergonomic dictionary rows.

### 2. SQL Borrowed Deltas (`ctx.SubscribeZeroAlloc(sql, onChanged)`)

```csharp
using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
ctx.Execute("CREATE DATABASE Game; USE Game;");
ctx.Execute("CREATE TABLE Player (Id INT PRIMARY KEY, Team VARCHAR(32), Score INT);");

using var sub = ctx.SubscribeZeroAlloc(
    "SELECT Team, COUNT(*) AS Players, SUM(Score) AS Total FROM Player GROUP BY Team",
    (in QueryChangeRef change) =>
    {
        change.Schema.TryGetOrdinal("Team", out int teamCol);
        change.Schema.TryGetOrdinal("Total", out int totalCol);

        RowSet added = change.Added;
        for (int i = 0; i < added.Count; i++)
        {
            RowRef row = added[i];
            Console.WriteLine($"{row[teamCol].AsString()}: {row[totalCol].AsInt64()}");
        }
    });

ctx.Execute("INSERT INTO Player VALUES (1, 'Red', 120), (2, 'Red', 80);");
ctx.DispatchPendingNotifications();
```

**How it works & when to use it:** Routes supported operators (`Aggregate`, `TopK`, `Join`, `VipExposure`, `RecursiveCte`) into a reusable `QueryChangeBuilder` cell arena and passes a stack-only `in QueryChangeRef` to your handler without allocating per-row dictionaries or boxing scalars. Use it for hot SQL views (aggregates, joins, top-K) where dictionary materialization overhead matters.

### 3. Prepared Statement Subscriptions (`ctx.Subscribe(statement, ...)` & `ctx.SubscribeZeroAlloc(statement, ...)`)

```csharp
using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
ctx.Execute("CREATE DATABASE Game; USE Game;");
ctx.Execute("CREATE TABLE Player (Id INT PRIMARY KEY, Name VARCHAR(50), Team VARCHAR(32), Score INT);");

// Parse once, reuse concurrently across independent parameter bindings.
DataVoPreparedStatement teamTop = ctx.Prepare(
    "SELECT Id, Name, Score FROM Player WHERE Team = @team ORDER BY Score DESC LIMIT @k");

using var redTop3 = ctx.SubscribeZeroAlloc(
    teamTop,
    (in QueryChangeRef change) =>
    {
        for (int i = 0; i < change.Added.Count; i++)
            Console.WriteLine($"Red top-3 added: {change.Added[i]["Name"].AsString()}");
    },
    ("team", "Red"), ("k", 3));

using var blueTop5 = ctx.Subscribe(
    teamTop,
    change =>
    {
        foreach (var row in change.Added)
            Console.WriteLine($"Blue top-5 added: {row["Name"]}");
    },
    new Dictionary<string, object?> { ["team"] = "Blue", ["k"] = 5 });

ctx.Execute("INSERT INTO Player VALUES (1, 'Ada', 'Red', 150), (2, 'Lin', 'Blue', 210);");
ctx.DispatchPendingNotifications();
```

**How it works & when to use it:** Validates the pre-parsed `DataVoPreparedStatement` (ensuring exactly one `SELECT` statement and honoring `SetMaxReactiveSubscriptions` before attaching any hook), clones and binds an isolated AST per subscription via `SqlParameterBinder`, and hands the bound AST directly to `ReactiveRegistry` without ever re-parsing SQL text. Use it when creating many parameterized subscriptions (per team, per room, per tenant) from one shared SQL template.

### 4. Typed LINQ Subscriptions (`db.Table<T>()...Subscribe(onChanged)`)

```csharp
using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }
    public string Name { get; set; } = "";
    [Indexed] public int Score { get; set; }
}

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Game; USE Game;");
db.EnsurePlayerTable();

using var sub = db.Table<Player>()
    .Where(100, static (p, minScore) => p.Score >= minScore)
    .OrderByDescending(static p => p.Score)
    .Take(3)
    .Subscribe(change =>
    {
        foreach (Player p in change.Added) Console.WriteLine($"+ {p.Name} ({p.Score})");
        foreach (Player p in change.Updated) Console.WriteLine($"~ {p.Name} ({p.Score})");
        foreach (Player p in change.Removed) Console.WriteLine($"- {p.Name}");
    });

db.Insert(new Player { Id = 1, Name = "Ada", Score = 120 });
db.DispatchPendingNotifications();
```

**How it works & when to use it:** `DataVo.Generators` intercepts `Where`, `OrderBy`/`OrderByDescending`, `ThenBy`, and `Select` at compile time, caches a prepared statement template on `DataVoTable<T>`, binds your `state` value into an isolated AST, and maps incoming rows via a generated `ReadReactiveRow` reader directly into `QueryChange<T>` with zero reflection. Use it for strongly-typed, Native AOT-safe reactive views over `[DataVoTable]` models (see [Roslyn Source Generators](/manual/client-interfaces/source-generators#reactive-subscriptions-and-zero-allocation-buffered-windows)).

### 5. Zero-Allocation Buffered Window (`SubscribeInto`) — Real-Time HUD Frame Tick

```csharp
using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }
    [Indexed] public int Score { get; set; }
}

// Primitive value-type struct: zero managed references, zero GC pressure.
public readonly record struct HudSlot(int Id, int Score);

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Game; USE Game;");
db.EnsurePlayerTable();

var hudSlots = new HudSlot[3];
int liveHudCount = 0;

using var hudSub = db.Table<Player>()
    .Where(100, static (p, minScore) => p.Score >= minScore)
    .OrderByDescending(static p => p.Score)
    .Take(3)
    .Select(static p => new HudSlot(p.Id, p.Score))
    .SubscribeInto(hudSlots.AsMemory(), count => liveHudCount = count);

// Simulation / frame loop:
db.Insert(new Player { Id = 1, Score = 150 });
db.Insert(new Player { Id = 2, Score = 320 });

// Runs on the render/main thread with 0 bytes allocated in steady state:
db.DispatchPendingNotifications();
ReadOnlySpan<HudSlot> visible = hudSlots.AsSpan(0, liveHudCount);
// visible[0] == HudSlot(2, 320), visible[1] == HudSlot(1, 150)
```

**How it works & when to use it:** `DataVo.Generators` emits a value-type `IReactiveRowProjector<HudSlot>` struct reading reactive `RowRef` cells by compile-time constant ordinals. The runtime maintains all qualifying rows in a pre-allocated flat `CellValue` slab indexed by unboxed primary-key `CellValue`s and sorted in a dense `int[]` slot array. When `DispatchPendingNotifications()` runs and the visible window `[Skip .. Skip + min(Take, destination.Length))` changed, DataVo writes the current live window directly into `destination.Span[0..count]` in place (clamped to `destination.Length`) and invokes `onUpdated(count)` with **0 bytes allocated** on the managed heap. Use `SubscribeInto` for real-time game HUDs, leaderboards, and frame-tick UI widgets where GC allocations are unacceptable.

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
- **No baseline redelivery:** `Subscribe` and `SubscribeInto` seed internal operator state from existing rows silently; only subsequent committed mutations trigger callbacks.

## See Also

- [Roslyn Source Generators](/manual/client-interfaces/source-generators) — compile-time `[DataVoTable]` models, LINQ interceptors, and `IReactiveRowProjector<T>` generation.
- [Streaming Queries (`StreamQuery` & `StreamRef`)](/manual/client-interfaces/streaming-queries) — pull-based `yield return` and zero-allocation scan streaming.
- [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows) — memory layout of `RowRef` spans.
- [SQL Compatibility Matrix](/manual/sql-language/sql-compatibility) — batch SQL vs reactive surface coverage.
