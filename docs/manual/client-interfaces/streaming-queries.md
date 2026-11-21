# Streaming Queries (`yield` & Zero-Allocation Scans)

By default, `db.Execute("SELECT ...")` materializes the full result set into `List<Dictionary<string, object?>>` before returning. On large tables—or when inspecting rows until a condition stops iteration—buffering every row wastes memory and work.

DataVo provides **three pull-based streaming APIs** that evaluate rows incrementally using C# iterator streaming (`yield return`) or stack-only `ref struct` spans:

| API | Return element | Page size / transport | Heap allocation per row | Best used when |
| --- | --- | --- | --- | --- |
| `db.StreamQuery(sql, ...)` | `DataVoRow` (`IEnumerable<DataVoRow>`) | 1,024-row pages (`yield return`) | Low (1 page live; owns cell array per yielded row) | Ad-hoc or parameterized SQL scans, `.Take(n)`, or early `break` without buffering millions of rows |
| `db.Table<T>()` (`Where`, spatial, `foreach`) | Typed POCO `T` (`IEnumerable<T>`) | 1,024-row pages (`yield return`) | One POCO per yielded row (unbuffered list) | Native AOT / Roslyn compiled queries streaming strongly typed models |
| `db.StreamRef("T")` / `db.Table<T>().StreamRef()` | `RowRef` (`readonly ref struct`) | 1,024-row buffer views (`RowRefStream`) | **0 B** after warmup | 60/120 FPS simulation loops, telemetry scanners, and GC-free hot paths |

---

## 1. Lazy SQL Streaming (`context.StreamQuery`)

```csharp
public IEnumerable<DataVoRow> StreamQuery(string query, params DataVoQueryParameter[] parameters)
public IEnumerable<DataVoRow> StreamQuery(string query, Guid sessionId)
```

For eligible single-table `SELECT` statements, `StreamQuery` binds the query and lazily fetches storage pages of **1,024 rows** (`StreamPageRows = 1024`), handing rows out one by one via `yield return row`.

```csharp
using DataVo.Core;

// Streams pages lazily; stops reading storage as soon as Take(50) or `break` ends the loop.
foreach (DataVoRow row in db.StreamQuery(
    "SELECT Id, Name, Score FROM Players WHERE Score >= @min",
    ("min", 900)))
{
    Console.WriteLine($"{row["Id"]}: {row["Name"]} ({row["Score"]})");

    if ((int)row["Id"]! == targetId)
    {
        break; // Triggers early exit (yield break) — remaining pages are never scanned
    }
}
```

### How Early Termination and `LIMIT` Work

- When the query contains `LIMIT k [OFFSET n]`, each fetched batch caps its page size to at most `Math.Min(1024, remaining + skip)` rows, and `EnumerateStream` exits immediately with `yield break` once `k` rows have been yielded.
- Each `DataVoRow` owns its decoded values (`object?[]`), so unlike `RowRef`, you can safely store individual yielded rows or inspect `row["Column"]` / `row[ordinal]` after the loop moves on.
- Each enumeration of the returned `IEnumerable<DataVoRow>` opens a fresh scan cursor.

### When `StreamQuery` Materializes Up Front

Not every SQL construct can emit rows before the full query completes. `StreamQuery` falls back cleanly to evaluating the query via the normal planner first—and then lazily iterates the result—when any of these apply:

- Multi-table `JOIN`s, `GROUP BY`, `HAVING`, `ORDER BY`, `SELECT DISTINCT`, `WITH` CTEs, window functions (`RANK()`), or computed SELECT expressions/aggregates.
- Active explicit transactions (`BEGIN TRANSACTION`), which require snapshot isolation over a single transaction view rather than releasing page read locks between 1,024-row chunks.
- Indexed equality predicates (`WHERE IndexedCol = @val`), where an index probe already fetches only matching rows directly.
- Predicates declined by the single-table typed scan fallback.

Also note lock granularity: during pure paged SQL streaming, `StreamQuery` acquires and releases the runtime read scope **per 1,024-row page** rather than locking the database across an arbitrarily slow consumer loop, so concurrent writers are never stalled by an open iterator.

---

## 2. Compiled `DataVoTable<T>` `IEnumerable<T>` Streaming

When using Roslyn source-generated models (`[DataVoTable]`), enumerating a table or `Where` query directly (without calling `.ToList()`) also streams 1,024-row pages via `yield return`:

```csharp
// Streams matching Player entities page-by-page without allocating a List<Player>:
foreach (Player player in db.Table<Player>().Where(50, static (p, min) => p.Level >= min))
{
    ProcessPlayer(player);
}

// Spatial helpers also stream pages lazily:
foreach (Entity e in db.Table<Entity>().WithinRadius(origin, radius: 25f))
{
    RenderNearby(e);
}
```

Call `.ToList()` only when you intentionally need every matching object buffered in memory at once. See [Roslyn Source Generators](/manual/client-interfaces/source-generators) for predicate shapes that run directly on stored bytes before constructing models.

---

## 3. Zero-Allocation `StreamRef` (`RowRefStream`)

When even one object allocation per returned row (`DataVoRow` or `T`) is too much—for example in a game simulation tick scanning hundreds of thousands of entities—use `StreamRef`:

```csharp
RowRefStream stream = db.Table<Player>().StreamRef();
int scoreCol = stream.GetOrdinal("Score");

long sum = 0;
foreach (RowRef row in stream)
{
    if (!row.IsNull(scoreCol))
    {
        sum += row.GetInt32(scoreCol);
    }
}
```

`RowRef` is a `readonly ref struct` pointing straight at reusable page buffer spans, so after per-thread warmup on a table schema, a full pass allocates **0 bytes** on the managed heap. Because its underlying buffer is reused on the next page, `RowRef` cannot be stored past its loop iteration or captured across `await`.

For full memory invariants and typed accessors, see [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows).

---

## Which One Should You Pick?

- Choose **`db.StreamQuery`** when writing ad-hoc or parameterized SQL and you want `yield`-based memory boundedness or early `break` termination.
- Choose **`db.Table<T>().Where(...)`** (`foreach`, no `.ToList()`) when you want compiled, Native AOT-safe domain objects streamed one page at a time.
- Choose **`db.StreamRef`** inside zero-GC hot loops that aggregate or inspect scalars/vectors inline.
- Choose **[Live Reactive Views (`Subscribe` / `SubscribeZeroAlloc`)](/manual/client-interfaces/reactive-views)** instead of repeated scanning whenever you need continuous incremental deltas (`Added` / `Removed` / `Updated`) as tables mutate.
