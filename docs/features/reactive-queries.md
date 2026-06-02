# Reactive Queries (Linear + Aggregates + Top-K / L1–L2)

Reactive queries let you register a standing `SELECT` and be told exactly what changed
in its result set — which rows were **added**, **removed**, or **updated** — without polling or
re-running the query against the whole table.

This page documents **Phase 1 (linear / L1)** — single-table `SELECT … WHERE` — and
**Phase 2 (aggregates + top-K / L2)** — single-table `GROUP BY` aggregates
(`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) and maintained `ORDER BY … LIMIT` top-K windows. Joins,
`DISTINCT`/`UNION`, subqueries, and recursion are later layers (L3–L4); `Subscribe` rejects them
today with a clear `NotSupportedException` naming the unsupported construct. The public API below is
**stable** and does not change as those layers land — adding aggregates and top-K did not change the
surface at all.

## API

```csharp
IDisposable Subscribe(string sql, Action<QueryChange> onChanged);
void DispatchPendingNotifications();
void SetMaxReactiveSubscriptions(int max);
```

- `Subscribe(sql, onChanged)` — parses and validates `sql`, seeds the subscription from the current
  table contents, and returns a handle. `Dispose()` the handle to stop delivery.
- `DispatchPendingNotifications()` — drains all buffered committed changes through every active
  subscription and invokes callbacks **on the calling thread**.
- `SetMaxReactiveSubscriptions(max)` — caps the number of concurrently active subscriptions.

`QueryChange` carries three projected result-row lists:

```csharp
public sealed class QueryChange
{
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Added { get; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Removed { get; }
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Updated { get; }
    public bool IsEmpty { get; }
}
```

Each row is projected to the columns named in your `SELECT` list (or the full row for `SELECT *`).

## Delivery model: pull-drain

Notifications are **pulled**, never pushed:

1. On every successful `COMMIT` (and auto-commit DML), the change set is appended to an internal
   buffer.
2. Nothing is delivered until you call `DispatchPendingNotifications()`.
3. That call feeds each buffered change set through every affected subscription, batches the result
   into one `QueryChange` per subscription, and invokes the callback (skipping empty results).

This puts you in control of *when* callback work runs — ideal for a game main loop or a
latency-sensitive consumer that must never block the writer.

## Supported SQL (L1)

A single-table `SELECT` with an optional `WHERE` over:

- comparisons: `=`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE`
- null checks: `IS NULL`, `IS NOT NULL`
- boolean composition: `AND`, `OR`

Rejected (until a later layer ships): `JOIN`, `HAVING`, `DISTINCT`, CTEs, subqueries, and any
multi-table query. These throw `NotSupportedException`. `GROUP BY` aggregates and `ORDER BY … LIMIT`
top-K are supported by the L2 operators described below.

## Supported SQL (L2)

`Subscribe` routes a query to the right incremental operator by its parsed shape:

- a `GROUP BY` clause, or any top-level aggregate in the select list, compiles to the **aggregate**
  operator;
- an `ORDER BY` plus `LIMIT` compiles to the **top-K** operator;
- everything else stays on the linear (L1) operator.

### Aggregates — `GROUP BY` with `COUNT` / `SUM` / `AVG` / `MIN` / `MAX`

A single-table `SELECT <group cols…>, <aggregates…> FROM t [WHERE …] GROUP BY <group cols…>`. Each
output row is `{ group columns…, aggregate aliases… }`. State is maintained **per group**:

- a **new** group emits `Added`;
- a group whose aggregates changed emits `Updated`;
- a group whose last row leaves (count reaches 0) emits `Removed`.

An optional pre-aggregation `WHERE` is applied first: a row only contributes to its group while it
matches, so a row entering the filter is an insert into its group and a row leaving is a delete.
Invertible aggregates (`COUNT`, `SUM`, `AVG`) are maintained by adding/subtracting each row's
contribution. Non-invertible `MIN`/`MAX` are backed by a **per-group, per-column value multiset**, so
when the current extreme is deleted the next one surfaces immediately without a rescan. `NULL`s are
excluded from `MIN`/`MAX` and from `COUNT(col)` (SQL semantics); `COUNT(*)` counts every row in the
group.

```csharp
// Live per-team score: one Updated row per team whenever its players' scores change.
using var leaderboard = ctx.Subscribe(
    "SELECT Team, COUNT(*) AS Players, SUM(Score) AS Total, MAX(Score) AS Best FROM Player GROUP BY Team",
    change =>
    {
        foreach (var g in change.Added.Concat(change.Updated))
            SetTeamPanel((string)g["Team"]!, (long)g["Players"]!, (long)g["Total"]!, (long)g["Best"]!);
        foreach (var g in change.Removed)
            ClearTeamPanel((string)g["Team"]!);
    });
```

### Top-K — maintained `ORDER BY … LIMIT`

A single-table `SELECT <cols…> FROM t [WHERE …] ORDER BY <keys…> LIMIT k [OFFSET n]`. The operator
keeps a sorted index of every matching row and derives the current window (the first `k` entries
after any offset). On each drain it recomputes the window and diffs it against the previous one: rows
that entered are `Added`, rows that left are `Removed`, and rows that stayed but whose projected
values changed are `Updated`.

Row identity in the index is the row's **primary key**, not the physical storage row id — DataVo's
`UPDATE` is out-of-place (it deletes the old row id and inserts a new one), so the primary key is what
keeps a row stable across an update. The query's table therefore must have a primary key.

```csharp
// Live top-3 leaderboard, highest score first.
using var top3 = ctx.Subscribe(
    "SELECT Id, Name, Score FROM Player ORDER BY Score DESC LIMIT 3",
    change =>
    {
        foreach (var r in change.Added)   AddToLeaderboard((int)r["Id"]!, (string)r["Name"]!, (int)r["Score"]!);
        foreach (var r in change.Removed) RemoveFromLeaderboard((int)r["Id"]!);
        foreach (var r in change.Updated) UpdateLeaderboardRow((int)r["Id"]!, (int)r["Score"]!);
    });
```

### Memory characteristics (L2)

- **Top-K** indexes *all* matching rows (not just the top `k`) so a row promoted into the window after
  a deletion is already known — memory is proportional to the number of rows passing the `WHERE`.
- **`MIN`/`MAX`** keeps a per-group multiset of the aggregated column's distinct values so the next
  extreme is available in O(log n) after a delete — memory is proportional to distinct values per
  group.

Both are acceptable for L2; a bounded-buffer refinement is a possible future optimization, not a
correctness requirement.

## Enter / leave / stay semantics

For each committed row mutation, the row is classified by whether its **before** and **after**
images satisfy the predicate:

| Change | Before matches | After matches | Result    |
|--------|----------------|---------------|-----------|
| Insert | —              | yes           | `Added`   |
| Delete | yes            | —             | `Removed` |
| Update | yes            | no            | `Removed` |
| Update | no             | yes           | `Added`   |
| Update | yes            | yes           | `Updated` |
| other  |                |               | ignored   |

The incrementally maintained result is verified to **exactly equal** a full re-execution of the
query — the L1, aggregate, and top-K oracle tests run long random operation sequences and assert
incremental == recompute on both `InMemory` and `Disk` storage.

## Safety rules

- **Transaction-safe.** Only committed change sets are ever processed; a `ROLLBACK` produces no
  notification.
- **Reentrancy is deferred.** Writes performed *inside* a callback are captured into a fresh buffer
  and surface on the **next** `DispatchPendingNotifications()` — they are never recursively
  dispatched in the current drain, so there are no infinite loops.
- **Bounded.** `SetMaxReactiveSubscriptions` caps active subscriptions; `Subscribe` throws
  `InvalidOperationException` when the cap is exceeded.
- **No background threads.** All capture and dispatch work runs on caller threads — no timers,
  sockets, or background workers.
- **Baseline is not redelivered.** `Subscribe` seeds internal state from existing rows but does not
  report them as `Added`; the first notification is the first *post-subscribe* change.
- **Opt-in capture.** Change capture is off until the first subscription registers, so there is no
  added per-write cost when reactive queries are not used.

## Example: a game loop

```csharp
using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
ctx.Execute("CREATE DATABASE Game");
ctx.Execute("USE Game");
ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Health INT)");

// React to players who drop into the danger zone.
using var lowHealth = ctx.Subscribe(
    "SELECT Id, Name, Health FROM Players WHERE Health < 20",
    change =>
    {
        foreach (var row in change.Added)
            ShowDangerWarning((int)row["Id"]!, (string)row["Name"]!);
        foreach (var row in change.Removed)
            ClearDangerWarning((int)row["Id"]!);
        foreach (var row in change.Updated)
            UpdateHealthBar((int)row["Id"]!, (int)row["Health"]!);
    });

// Main loop: apply gameplay writes, then drain once per frame.
while (running)
{
    ApplyGameplayWrites(ctx);          // INSERT / UPDATE / DELETE as the simulation runs
    ctx.DispatchPendingNotifications(); // deliver this frame's result changes on this thread
    RenderFrame();
}
```

Calling `DispatchPendingNotifications()` exactly once per frame keeps callback work deterministic and
on the loop thread.
