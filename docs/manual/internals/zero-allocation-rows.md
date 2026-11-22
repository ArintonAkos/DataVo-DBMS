---
title: Zero-Allocation Row Model
description: Stream table rows without managed heap allocations using RowRef, a stack-only readonly ref struct over stored byte spans.
---

# Zero-Allocation Row Model

DataVo can scan entire tables without allocating a single managed object per row. `RowRef` is a `readonly ref struct` that wraps a `ReadOnlySpan<byte>` over a row's stored bytes, and `RowRefStream` yields `RowRef` values to a `foreach` loop in 1,024-row pages. Once a thread has warmed up on a table's layout, a full scan allocates **0 bytes** on the managed heap.

```csharp
using DataVo.Core;

RowRefStream players = db.Table<Player>().StreamRef();

// Resolve column ordinals once before the loop (ordinal lookup allocates once)
int scoreCol = players.GetOrdinal("Score");

long totalScore = 0;
foreach (RowRef row in players)
{
    if (!row.IsNull(scoreCol))
    {
        totalScore += row.GetInt32(scoreCol);
    }
}
```

The same zero-allocation stream is available without a model type via `db.StreamRef("players")`.

## How `RowRef` Works

Conventional database drivers materialize a dictionary, `object[]`, or POCO for every row: 200,000 rows means 200,000 heap allocations that the GC must later collect. `RowRef` avoids materialization entirely by decoding only the cells you read directly from the storage buffer.

<div class="sketch-diagram">
  <img class="light-only" src="/manual/fig-06-zero-alloc-rowref-light.svg" alt="FlatRowStore packed chunk copied in 1,024-row pages into a stack-only RowRef ref struct for zero-allocation column reads" />
  <img class="dark-only" src="/manual/fig-06-zero-alloc-rowref-dark.svg" alt="FlatRowStore packed chunk copied in 1,024-row pages into a stack-only RowRef ref struct for zero-allocation column reads" />
</div>

Because `RowRef` is a `ref struct`, the C# compiler enforces that it lives strictly on the stack: it cannot be boxed, stored on a class field, captured by a lambda, or held across an `await`. That compile-time guarantee makes it memory-safe for `RowRefStream` to reuse the same page buffer for the next 1,024 rows.

## Rules & Safety Guarantees

- **Iteration-scoped lifetime:** A `RowRef` is valid only inside the `foreach` iteration that produced it. Copy any value you need to retain past the loop body.
- **Span views follow the same lifetime:** `GetStringSpan` and `GetStringUtf8` return views directly into the row buffer; call `new string(span)` or copy the bytes if you need the text later.
- **Strict typing with lossless widening:** Accessors must match the column's declared type (with lossless widening: `INT` via `GetInt64`, `FLOAT` via `GetDouble`). Mismatched types throw `InvalidCastException`.
- **Explicit NULL checks:** Reading a NULL cell throws `InvalidOperationException`. Call `row.IsNull(ordinal)` first on nullable columns.
- **Table column order:** Ordinals follow `CREATE TABLE` order or the property declaration order on a `[DataVoTable]` model.

## Concurrency & Page Isolation

- **Non-blocking 1,024-row pages:** The engine acquires a short read lock only while copying a 1,024-row page into the stream buffer, then releases it before your loop body executes, so slow consumers never stall writers.
- **Pass semantics:** Every row that exists for the entire pass is visited once; concurrent inserts or deletes may or may not appear. A concurrent schema change aborts the pass with `InvalidOperationException`.
- **Inside explicit transactions:** `StreamRef()` falls back to the query engine so it observes the transaction's own uncommitted writes. **That transactional path allocates.**

## What "Zero Allocation" Covers

Zero allocation applies specifically to `RowRefStream` scans and compiled scan-kernel aggregates; materializing a `Player` class instance still allocates the `Player` object itself.

| Operation | Managed Allocation |
| --- | ---: |
| Full `RowRefStream` pass (after thread warm-up) | **0 B** |
| Compiled LINQ `Count`/`Sum`/`Min`/`Max` over 200,000 rows | **0 B** |
| Compact `InMemory` PK lookup into `Player` (`FlatRowStore`) | **56 B** (the `Player` object itself) |
| `Lsm` PK lookup into `Player` | **144 B** |
| `RowRefStream` pass inside an explicit transaction | Allocates |

Nothing in `RowRef` or `RowRefStream` uses runtime reflection or `System.Reflection.Emit`; `DataVo.Core` compiles cleanly with `IsAotCompatible` and zero trim/AOT warnings.

## See Also
- [Streaming Queries](/manual/fundamentals/streaming-queries): Comparing `Execute`, `StreamQuery`, and `StreamRef`
- [Compiled Scan Kernels](/manual/internals/compiled-scan-kernels): Build-time LINQ kernels that run over stored bytes with 0 B allocation
- [Native AOT Deployment](/manual/client-interfaces/native-aot): Publishing trimmed Native AOT binaries
