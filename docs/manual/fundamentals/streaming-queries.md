---
title: Streaming Queries
description: Compare buffered Execute, paged yield-return StreamQuery, compiled DataVoTable<T> streaming, and zero-allocation StreamRef scans.
---

# Streaming Queries

By default, `db.Execute("SELECT ...")` buffers the entire result set into `List<Dictionary<string, object?>>` before returning. When scanning large tables or stopping early on a match, use DataVo's pull-based streaming APIs to process rows incrementally in 1,024-row pages, or with zero heap allocations.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
db.Execute("CREATE DATABASE Game; USE Game;");
db.Execute("CREATE TABLE players (id INT PRIMARY KEY, name VARCHAR, score INT)");

// Lazily fetches 1,024-row pages; stops scanning storage immediately on break
foreach (DataVoRow row in db.StreamQuery(
    "SELECT id, name, score FROM players WHERE score >= @min",
    ("min", 900)))
{
    Console.WriteLine($"{row["name"]}: {row["score"]}");
    if ((int)row["id"]! == targetId)
        break; // triggers yield break; remaining pages are never read
}
```

## Comparing Execution Modes

<div class="sketch-diagram">
  <img class="light-only" src="/manual/fig-07-streaming-modes-light.svg" alt="Comparison of Execute buffered results, StreamQuery 1024-row yield pages, and zero-allocation StreamRef RowRef spans" />
  <img class="dark-only" src="/manual/fig-07-streaming-modes-dark.svg" alt="Comparison of Execute buffered results, StreamQuery 1024-row yield pages, and zero-allocation StreamRef RowRef spans" />
</div>

| API | Element Type | Memory & Allocation | Best For |
| --- | --- | --- | --- |
| `db.Execute(sql)` | `List<Dictionary<string, object?>>` | Buffers full result set | Small results, DDL/DML, scripts |
| `db.StreamQuery(sql, ...)` | `IEnumerable<DataVoRow>` | 1,024-row pages (`yield return`); owns cell array per row | Large SQL scans, `.Take(n)`, early `break` |
| `db.Table<T>().Where(...)` | `IEnumerable<T>` | 1,024-row pages (`yield return`); one POCO per row | Native AOT compiled domain queries without `.ToList()` |
| `db.StreamRef("T")` / `table.StreamRef()` | `RowRef` (`readonly ref struct`) | **0 B** per row after warmup | 60/120 FPS simulation loops and GC-free aggregators |

## Paged SQL Streaming (`db.StreamQuery`)

For eligible single-table `SELECT` queries, `StreamQuery` fetches 1,024-row pages lazily and yields each `DataVoRow`:

- **Early exit & `LIMIT`:** With `LIMIT k [OFFSET n]`, each batch caps its fetch size to `Math.Min(1024, remaining + skip)` and exits via `yield break` once `k` rows are emitted.
- **Owned rows:** Each `DataVoRow` owns its decoded values (`row["Column"]` or `row[ordinal]`), so you can safely store rows beyond the loop iteration.
- **Per-page locking:** During pure paged streaming, DataVo acquires and releases the read scope **per 1,024-row page**, so a slow consumer never stalls concurrent writers.

`StreamQuery` automatically falls back to materializing first (while still streaming the resulting rows lazily) when a query uses `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `DISTINCT`, CTEs, `RANK()`, indexed equality probes, or runs inside an explicit `BEGIN` transaction (which needs a single snapshot).

## Compiled `DataVoTable<T>` Streaming

Enumerating a source-generated `[DataVoTable]` query without calling `.ToList()` streams 1,024-row pages of strongly-typed entities:

```csharp
foreach (Player p in db.Table<Player>().Where(100, static (p, min) => p.Score >= min))
{
    ProcessPlayer(p);
}

foreach (Player nearby in db.Table<Player>().WithinRadius(origin, radius: 25f))
{
    RenderNearby(nearby);
}
```

Only call `.ToList()` when you intentionally need every matching object buffered at once.

## Zero-Allocation Scans (`StreamRef`)

::: info DataVo Extension
`StreamRef` (`RowRefStream` yielding stack-only `RowRef` spans) is a DataVo .NET-native extension designed for zero-GC hot loops.
:::

When allocating even one `DataVoRow` or POCO per row is too expensive, stream raw page views with `StreamRef`:

```csharp
RowRefStream stream = db.Table<Player>().StreamRef();
int scoreCol = stream.GetOrdinal("Score");

long total = 0;
foreach (RowRef row in stream)
{
    if (!row.IsNull(scoreCol))
        total += row.GetInt32(scoreCol);
}
```

`RowRef` is a `readonly ref struct` backed by a reusable page buffer, allocating **0 bytes** on the managed heap after warmup. Because the buffer is overwritten on the next page, `RowRef` cannot be stored past its loop iteration or captured across `await`.

::: details API Reference
```csharp
// DataVoContext
public IEnumerable<DataVoRow> StreamQuery(string query, params DataVoQueryParameter[] parameters);
public IEnumerable<DataVoRow> StreamQuery(string query, Guid sessionId);
public RowRefStream StreamRef(string tableName);

// DataVoTable<T>
public RowRefStream StreamRef();
```
:::

## See Also

- [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows): Memory layout and typed accessors of `RowRef`
- [Live Reactive Views](/manual/fundamentals/reactive-views): Receive incremental deltas instead of rescanning tables
- [Roslyn Source Generators](/manual/client-interfaces/source-generators): Compile-time predicates on `DataVoTable<T>`
