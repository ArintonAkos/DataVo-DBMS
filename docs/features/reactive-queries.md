# Reactive Queries (Linear / L1)

Reactive queries let you register a standing `SELECT … WHERE` and be told exactly what changed
in its result set — which rows were **added**, **removed**, or **updated** — without polling or
re-running the query against the whole table.

This page documents **Phase 1 (linear / L1)**: single-table `SELECT … WHERE`. Aggregates, top-K,
joins, `DISTINCT`/`UNION`, subqueries, and recursion are later layers (L2–L4); `Subscribe` rejects
them today with a clear `NotSupportedException` naming the unsupported construct. The public API
below is **stable** and will not change as those layers land.

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

Rejected (until a later layer ships): `JOIN`, aggregates / `GROUP BY` / `HAVING`, `ORDER BY`,
`LIMIT`/`OFFSET`, `DISTINCT`, CTEs, subqueries, and any multi-table query. These throw
`NotSupportedException`.

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
query (the L1 oracle test) on both `InMemory` and `Disk` storage.

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
