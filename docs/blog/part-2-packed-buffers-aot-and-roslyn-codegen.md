---
title: "Packed byte buffers, Native AOT, and Roslyn codegen: how a 55 ns point lookup works with zero native binaries"
description: "Part 2 of 7: Deconstructing the managed-to-native tax, 64-byte aligned FlatRowStore records, lock-free ARM64 optimistic reads, AST-isolated prepared statements, Roslyn interceptors, and locking the Native AOT fence from 184 warnings to zero."
outline: [2, 3]
prev:
  text: "Part 1: Where it wins and where it loses"
  link: "/blog/part-1-where-it-wins-and-where-it-loses"
next: false
---

# Packed byte buffers, Native AOT, and Roslyn codegen: how a 55 ns point lookup works with zero native binaries

*Part 2 of 7.*

In [Part 1](/blog/part-1-where-it-wins-and-where-it-loses), a single benchmark line stood out above the rest: looking up one row by primary key out of 100,000 shuffled records took **55.0 ns** in memory and **216.2 ns** on LSM disk files on an Apple M4 Pro—**7.7–13× faster** than SQLite, allocating **56 B** in memory (literally the size of the returned `Player` object) and **144 B** on disk, versus **608 B** for `Microsoft.Data.Sqlite`.

That result raises an obvious question: **how can C# beat a battle-tested C database library by an order of magnitude on an in-memory lookup?**

It is not because C# executes instructions faster than C. It is because, in a managed runtime, calling *into* a C database library is never free—and neither is asking a general-purpose SQL engine to rediscover your schema on every row. When the storage engine lives inside the same managed compilation unit as your application, and the C# compiler knows your table schema and query predicate at build time, four layers of runtime ceremony collapse into a handful of inlined bounds checks, aligned loads, and a single object allocation.

This post opens the hood on that entire path—from the byte layout of `FlatRowStore`, through the lock-free ARM64 liveness protocol in `PackedKeyReader<T>`, to AST-isolated prepared statements (`DataVoPreparedStatement`), Roslyn `[InterceptsLocation]` code generation (`DataVo.Generators`), and the ratchet that took `DataVo.Core` and `DataVo.Data` from **184** Native AOT warnings down to **0**.

At a glance, here is what makes the numbers work—and what they cost in engineering complexity:

- **Zero native binaries and zero P/Invoke marshalling:** `DataVo.Core` has a single `net10.0` dependency (`System.Numerics.Tensors`) and zero `.dll`/`.so`/`.dylib` native libraries. Every step from caller to storage sits inside one RyuJIT / Native AOT compilation unit.
- **Widest-first packed buffers (`FlatRowStore`):** Fixed-width columns live in 64-byte-aligned Pinned Object Heap chunks (`FlatChunk`, 4,096 slots), shrinking retained row memory **13×** from **640.3 B/row** to **49.2 B/row** (shared strings).
- **Lock-free optimistic reads on weakly ordered CPUs:** In-memory integer primary-key lookups take no lock, using opposite `Volatile.Write` / `Volatile.Read` ordering to guarantee that a concurrent delete never surfaces a half-read row.
- **Prepared statements as the universal backbone:** `ctx.Prepare(...)` parses once and clones bound ASTs via `SqlParameterBinder` without mutating the shared template—feeding both 216 ns LSM point lookups (via `ExecuteBorrowed` and reused `RowResultSlot` boxes) and multi-tenant reactive views (`Subscribe` / `SubscribeZeroAlloc`).
- **Build-time Roslyn codegen, zero runtime reflection:** `DataVoTableGenerator` and `DataVoLinqInterceptorGenerator` replace `Expression.Compile()` and reflection mapping with static readers (`ReadPackedRow`, `ReadReactiveRow`), compiled scan structs (`IRowRefFilter`), and zero-allocation reactive projectors (`IReactiveRowProjector<T>`), compiling cleanly under `PublishAot=true` with `IL2026`–`IL3056` treated as build errors.

None of this is free. Every shortcut introduces a parity invariant against the general SQL evaluator, weak memory-ordering subtleties, or compiler-version constraints. Those costs are tallied honestly in the final section.

---

## The invisible tax of crossing the managed–native boundary

When a .NET application reads one row from an in-memory SQLite database using a prepared `SqliteCommand`, the lookup is not just a B-tree probe in RAM. Even after `Prepare()` has compiled the SQL into SQLite VDBE bytecode, every single row fetch crosses a managed–native boundary and navigates a dynamically typed C API <a class="cite-badge" href="#ref-2">2</a> <a class="cite-badge" href="#ref-3">3</a> (illustrated in Figure 1):

1. **Managed parameter binding and command state.** Assigning `_sqliteId.Value = id` boxes the integer into `object?`, and calling `ExecuteReader()` allocates a `SqliteDataReader` and transitions through `Microsoft.Data.Sqlite`'s state machine.
2. **Crossing the P/Invoke ABI wall.** Each call into `e_sqlite3` (`sqlite3_reset`, `sqlite3_bind_int64`, `sqlite3_step`, and one `sqlite3_column_*` call per column) crosses an unmanaged calling convention boundary. The .NET JIT cannot inline `sqlite3_step` into your caller, cannot eliminate redundant null/bounds checks across the boundary, and pays a fixed marshalling/transition overhead on every invocation.
3. **Dynamic column typing and string transcoding.** SQLite uses dynamic type affinity per cell <a class="cite-badge" href="#ref-2">2</a>. Reading `Name` invokes `sqlite3_column_text`, reads a null-terminated UTF-8 byte pointer in unmanaged memory, and transcodes it into a newly allocated UTF-16 `.NET` `string` on every lookup—even if the database lives in `:memory:`.
4. **Optional ORM / micro-ORM overhead.** If your application maps rows through Dapper (`connection.QuerySingleOrDefault<Player>(...)`), Dapper adds dynamic reader-to-POCO mapping on top of ADO.NET (DataVo supports Dapper through `DataVo.Data`'s `DataVoConnection` as documented in [Dapper](/manual/client-interfaces/dapper), though we do not include a Dapper row in the BenchmarkDotNet suite below). Under Native AOT, reflection-heavy mapping layers also become trimming liabilities.

<div class="sketch-diagram light-only">

![Figure 1: SQLite managed-to-native P/Invoke boundary versus DataVo single managed compilation unit](/blog/part-2/fig-01-interop-boundary-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 1: SQLite managed-to-native P/Invoke boundary versus DataVo single managed compilation unit](/blog/part-2/fig-01-interop-boundary-dark.svg)

</div>

*Figure 1: SQLite managed-to-native P/Invoke boundary versus DataVo's single managed compilation unit.*

Notice what happens to that stack when the database engine itself is written in pure managed C#. On `net10.0`, `DataVo.Core.csproj` has only one NuGet package reference—`System.Numerics.Tensors` `10.0.0` (managed SIMD intrinsics)—and `DataVo.Data.csproj` references only `DataVo.Core`. There is no native `.dll`, `.so`, or `.dylib` to package per RID, no `DllImport` wall blocking the inliner, and no UTF-8-to-UTF-16 string transcoding on in-memory reads because in-memory strings are already .NET `string` objects.

Removing the FFI wall is necessary, but it is **not** sufficient. An earlier version of DataVo was already 100% pure managed C#, yet its in-memory point lookup took **405–566 ns** and allocated **1,008 B** per call—slower than its own LSM disk engine (**241 ns**). The reason was data layout and runtime dispatch.

---

## Packing rows into cache-aligned byte buffers (`FlatRowStore`)

In a naive managed in-memory database, the natural representation of a row is an object: a `StoredRow` containing a `CellValue[]` or `Dictionary<string, object?>`. Each row carries an object header, an array header, bounds metadata, and scattered heap pointers. On the benchmark `Player` schema—six columns (`Id`, `Active`, `Level`, `Gold`, `Ratio`, `Name`)—that object-per-row layout retained **640.3 bytes per row** (with shared strings). Scanning or probing 100,000 rows meant chasing hundreds of thousands of heap pointers and handing the garbage collector an enormous reference graph to traverse during Gen-2 collections <a class="cite-badge" href="#ref-1">1</a>.

```csharp
[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }
    public bool Active { get; set; }
    [Columnar] public int Level { get; set; }
    [Columnar] public long Gold { get; set; }
    [Columnar] public double Ratio { get; set; }
    public string Name { get; set; } = "";
}
```

To eliminate per-row heap objects without resorting to unmanaged `Marshal.AllocHGlobal` memory (and its manual free / use-after-free risks), DataVo's in-memory storage engine stores rows in **`FlatRowStore`** (`DataVo.Core/StorageEngine/Memory/FlatRowStore.cs`).

### 64-byte aligned Pinned Object Heap chunks

`FlatRowStore` splits every row into two parallel structures indexed by a stable, append-only row number (`0, 1, 2, ...`):

1. **Fixed-width records (`FlatSlotList` → `FlatChunk`).** Every chunk holds `4,096` slots (`1 << 12`; small tables start with `FirstChunkRows = 16` slots so tiny tables do not waste memory). On .NET 10, `FlatChunk` allocates its backing array directly on the **Pinned Object Heap (POH)** <a class="cite-badge" href="#ref-3">3</a> and computes the leading offset `_start` so slot 0 sits on a 64-byte cache-line boundary:

```csharp
// DataVo.Core/Memory/Flat/FlatChunk.cs
_memory = GC.AllocateArray<byte>(length + Alignment - 1, pinned: true);
_start = AlignmentGap(_memory); // Alignment = 64
```

Because the array is owned by the GC and never moves or shrinks once published, a lock-free reader holding a `ReadOnlySpan<byte>` over a slot can never dereference freed memory—and aligned vector loads never cross unnecessary cache-line boundaries.

2. **Object references (`ReferenceDirectory`).** Variable-length managed references—strings and `float[]` vectors—cannot live inside a raw `byte[]` without hiding pointers from the garbage collector. Instead, `ReferenceDirectory` allocates parallel `object?[]` chunks aligned with the same 4,096-slot chunk index (`slot & 4095`). For `Player`, only `Name` is a reference column, so each row owns exactly one reference slot (`slot 0`).

### Widest-first alignment and the 32-byte `Player` record

Inside each slot, `RowLayout.Create` (`DataVo.Core/StorageEngine/Memory/RowLayout.cs`) orders fixed-width columns **widest first**—16-byte (`Decimal`, `Guid`), then 8-byte (`Int64`, `Double`), then 4-byte (`Int32`, `Date`), then 1-byte (`Boolean`)—followed by a 1-byte **status flag**, a **null bitmask** (`(columnCount + 7) >> 3` bytes), and trailing padding up to an 8-byte multiple (`RecordAlignment = 8`).

Widest-first ordering guarantees that every primitive sits on a multiple of its own width with zero interior padding holes. For `Player`, that packs all five fixed-width fields, the status byte, and the null bitmask into **32 bytes** (Figure 2):

- `Gold` (`Int64`, 8 B): bytes `0..7`
- `Ratio` (`Double`, 8 B): bytes `8..15`
- `Id` (`Int32`, 4 B): bytes `16..19`
- `Level` (`Int32`, 4 B): bytes `20..23`
- `Active` (`Boolean`, 1 B): byte `24`
- `Status` (1 B): byte `25` (`0 = Empty`, `1 = Packed`, `2 = KeptApart`)
- `NullBits` (1 B, 1 bit per column): byte `26`
- Padding (5 B): bytes `27..31` → `RecordBytes = 32`

<div class="sketch-diagram light-only">

![Figure 2: FlatRowStore 32-byte widest-first packed record layout for Player and parallel ReferenceDirectory](/blog/part-2/fig-02-flatrowstore-layout-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 2: FlatRowStore 32-byte widest-first packed record layout for Player and parallel ReferenceDirectory](/blog/part-2/fig-02-flatrowstore-layout-dark.svg)

</div>

*Figure 2: `FlatRowStore` 32-byte widest-first packed record layout for `Player` and its parallel `ReferenceDirectory`.*

Moving to `FlatRowStore` dropped retained row-store memory **13×**: from **640.3 B/row** down to **49.2 B/row** with shared strings (**145.3 B/row** with unique `"player-N"` strings; **231.6 B/row** total including the primary-key index).

What happens if a dynamic SQL statement inserts a row whose cell types do not match the table's packed layout (for instance, a `BIGINT` cell in an `INT` column, or a mismatched column count)? Rather than complicating the packed binary format with per-cell type tags, `FlatRowStore.Add` stores that row's `CellValue[]` in a lazily created side `ConcurrentDictionary<int, CellValue[]>` and marks its status byte `StatusKeptApart = 2`. Ordinary rows pay nothing for the escape hatch; `StatusKeptApart` rows simply route through the general cell decoder.

And for streaming scans over serialized rows (including LSM SSTables), DataVo exposes the same zero-copy philosophy through **`RowRef`** (`DataVo.Core/Streaming/RowRef.cs`)—a stack-only `readonly ref struct` wrapping `ReadOnlySpan<byte>` that decodes columns on demand via `BinaryPrimitives` without allocating per-row objects (see [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows)).

---

## Tracing a 55 ns point lookup—and the 216 ns LSM fallback

Having compact memory is only half the story. If a primary-key lookup still builds a SQL string, runs a parser, acquires a table read lock, and materializes a `Dictionary<string, object?>`, it will still burn hundreds of nanoseconds.

Here is the exact execution path of the BenchmarkDotNet lookup in [PointLookupBenchmarks.cs](https://github.com/ArintonAkos/DataVo-DBMS/blob/master/benchmarks/DataVo.Benchmarks/PointLookupBenchmarks.cs):

```csharp
// BenchmarkDotNet baseline call:
Player? found = _players.FirstOrDefault(id, static (p, key) => p.Id == key);
```

At build time, `DataVoLinqInterceptorGenerator` intercepts that call site with `[InterceptsLocation]` and routes it to a generated singleton `DataVoLinqPlan<Player, int>` whose `TryGetIntegerKey` override extracts `key` directly without touching SQL or boxing parameters. At runtime, execution splits cleanly into two paths (Figure 3):

<div class="sketch-diagram light-only">

![Figure 3: 55 ns lock-free packed lookup versus 216 ns LSM fallback and ARM64 volatile memory ordering](/blog/part-2/fig-03-lookup-pipeline-and-liveness-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 3: 55 ns lock-free packed lookup versus 216 ns LSM fallback and ARM64 volatile memory ordering](/blog/part-2/fig-03-lookup-pipeline-and-liveness-dark.svg)

</div>

*Figure 3: The 55 ns lock-free `PackedKeyReader<T>` shortcut versus the 216 ns LSM `ClusteredPoint` path, and the ARM64 volatile ordering invariant.*

### Path A: In-memory `PackedKeyReader<T>` (55.0 ns, 56 B)

When `DataVoLinqPlan.FirstOrDefault` calls `table.TryReadByIntegerKey(key, out item)`, `PackedKeyReader<T>` (`DataVo.Core/Tables/PackedKeyReader.cs`) first verifies a fast fail-closed guard (`MustUseStatement()`):

- Is the backing storage packed (`HasPackedRows`)?
- Are authorization and query diagnostics off?
- Is this session outside an explicit transaction, MVCC snapshot, and active row write locks?

If any guard fails, `PackedKeyReader<T>` immediately returns `false` and hands off to the prepared statement. Otherwise, **without taking any lock**:

1. It probes the cached `ConcurrentInt64Int64Map` (`_PK_players`) to map `key → rowId`.
2. It borrows a `FlatRow` (`ReadOnlySpan<byte>` + `object?[] references` + `referenceBase`) at `position = (int)(rowId - 1)` via `rows.TryReadPacked(position, out FlatRow row)`.
3. It checks `binding.KeyOf(row.Record) == key` to verify the cached index and layout binding are not stale.
4. It wraps the `FlatRow` in a stack-only `DataVoPackedRow` `readonly ref struct` and invokes the source-generated `ReadPackedRow` delegate, which reads each column by precomputed offset directly into `new Player { ... }`.
5. **After** constructing `Player`, it checks `rows.IsTypedLive(position) && source.Table.CurrentRows is not null`.

Why does step 5 happen *after* reading the row, and why does it work on weakly ordered ARM64 processors like the Apple M4 Pro? <a class="cite-badge" href="#ref-4">4</a>

Look closely at the bottom half of Figure 3. A packed row in `FlatRowStore` is immutable once published—its fixed-width bytes are never modified in place. The only concurrent mutation possible is **`FlatRowStore.Remove(index)`**, which marks the slot empty and nulls out its string/vector references so the GC can reclaim them:

```csharp
// Writer: DataVo.Core/StorageEngine/Memory/FlatRowStore.cs + RowLayout.cs
Layout.MarkEmpty(record);                   // 1. Volatile.Write(ref record[StatusOffset], 0)
FlatRowCodec.ClearReferences(Layout, ...);  // 2. Volatile.Write(ref references[base + slot], null)
```

Meanwhile, `PackedKeyReader<T>.TryReadFrom` reads in the **exact opposite order**:

```csharp
// Reader: DataVo.Core/Tables/PackedKeyReader.cs
try
{
    // 1. Fixed-width bytes (immutable) + reference slots via Volatile.Read
    item = _read(new DataVoPackedRow(row, binding.Fields!));
}
catch (Exception) when (!rows.IsTypedLive(position))
{
    // Row was removed mid-read, so a cleared non-null string threw NullColumn.
    return false;
}

// 2. Trailing acquire load on StatusOffset: Volatile.Read(ref record[StatusOffset]) != 0
return rows.IsTypedLive(position) && source.Table.CurrentRows is not null;
```

Because the writer publishes `Status = Empty` with release semantics *before* clearing references, and the reader loads every reference with `Volatile.Read` *before* checking `IsTypedLive` (which reads `StatusOffset` with `Volatile.Read`), a reader that observes a cleared reference is guaranteed to also observe `Status = Empty`—even on weakly ordered ARM64 cores. If a delete raced with the lookup, the half-read `Player` is discarded and the call falls back cleanly to the prepared statement, which resolves whether the row exists.

And where do the **56 bytes** of managed allocation come from on 64-bit .NET? They are the `Player` object itself: 16 B object header + MethodTable pointer + 8 B `Gold` + 8 B `Ratio` + 8 B `Name` reference + 4 B `Id` + 4 B `Level` + 1 B `Active` + 7 B alignment padding = **56 bytes**. The database engine allocates **0 bytes**.

### Path B: LSM file `ClusteredPoint` (216.2 ns, 144 B)

When the database runs in `StorageMode.Lsm`, `_packedStorage` is `false`, so `PackedKeyReader<T>` steps aside immediately. `DataVoLinqPlan` fetches the cached `DataVoPreparedStatement` from its per-table slot and calls `statement.ExecuteBorrowed(p0)`:

1. `DataVoPreparedStatement` dispatches to its cached schema-versioned `CompiledSelectPlan` (`SelectAccessKind.ClusteredPoint` in `DataVoPreparedStatement.Plans.cs`). Because `Player` has a single `INT` primary key, LSM stores rows **clustered** directly under that key—no secondary index hop.
2. `TryExecuteClusteredPointSelect` calls `StorageContext.TryVisitRowByKey` → `LsmStorageEngine.TryVisitStoredRowByKey`.
3. `LsmStorageEngine` first probes `table.LatestRows` (a lock-free concurrent overlay of unflushed/recent writes). If not present, it acquires an immutable `TableVersion`, probes memory-mapped SSTable V2 files (`SsTableV2Reader.Lookup` with blocked Bloom filters and binary search), and copies the serialized row bytes into a thread-local `_singleRowScratch` buffer.
4. `VisitClusteredPointRow` decodes only the projected columns into a reusable thread-local `RowResultSlot`. To avoid allocating fresh boxed `int`, `long`, and `double` objects for every row dictionary entry, `RowResultSlot` reuses pre-allocated boxes in place (using `BoxedIntegerCache` for `-1..32767` and `Unsafe.Unbox<T>(box) = value` for everything else):

```csharp
// DataVo.Core/PreparedStatements/DataVoPreparedStatement.Plans.cs
[MethodImpl(MethodImplOptions.AggressiveInlining)]
public object BoxInt64(int ordinal, long value)
{
    object box = _boxedLongs[ordinal];
    Unsafe.Unbox<long>(box) = value;
    return box;
}
```

5. Finally, the generated `ReadRow` delegate unboxes those fields into a fresh `Player` (`56 B`) and receives the decoded UTF-8 `Name` string (`~48–56 B` for `"player-XXXXX"`), leaving **144 B** total allocation per lookup and **216.2 ns** mean latency.

### Cross-platform BenchmarkDotNet results

Figure 6 (below) and the tables summarize the BenchmarkDotNet `PointLookupBenchmarks` and `FilteredAggregateBenchmarks` measurements across all three platforms (Apple M4 Pro local `02fa240f`, Linux x64 CI, and Windows x64 CI from run `44748508039`).

<details>
<summary>Test environments (Apple M4 Pro, Linux x64 CI, Windows x64 CI)</summary>

| Environment | Hardware | Runtime | Source |
| --- | --- | --- | --- |
| Apple M4 Pro | 12 cores, 48 GiB, APFS SSD | .NET 10.0.3, Arm64 RyuJIT, macOS 26.3 | BDN `0.15.8` (`--job short`, `02fa240f`, 20 Nov 2026) under `docs/public/benchmarks/2026-11-21-m4pro/` |
| Linux x64 CI | AMD EPYC 9V45, 4 logical / 2 physical cores | .NET 10.0.15, Ubuntu 26.04 | GitHub Actions run `44748508039` (`master` @ `02fa240f`, 20 Nov 2026) |
| Windows x64 CI | Xeon Platinum 8370C, 4 logical / 2 physical cores (Hyper-V) | .NET 10.0.15, Windows 11 24H2 | GitHub Actions run `44748508039` (`master` @ `02fa240f`, 20 Nov 2026) |

</details>

| Primary-key lookup (100k rows, shuffled) | DataVo (Mem) | DataVo (LSM File) | SQLite (Mem) | SQLite (WAL File) | DuckDB 1t (File) |
| --- | ---: | ---: | ---: | ---: | ---: |
| **Apple M4 Pro** (`02fa240f`) | **55.01 ns** *(±5.61 ns)* | **216.22 ns** *(±12.90 ns)* | 715.65 ns | 1,654.00 ns | 62,702 ns |
| **Linux x64 CI** (`44748508039`) | **130.8 ns** | **288.3 ns** | 1,398.2 ns | 3,642.7 ns | 204,140 ns |
| **Windows x64 CI** (`44748508039`) | **239.6 ns** | **666.9 ns** | 2,024.0 ns | 6,741.1 ns | 380,684 ns |
| **Managed alloc / call** (64-bit .NET 10) | **56 B** | **144 B** | 608 B | 608 B | 2,144 B |

*(Note: on the full-job 13 November M4 Pro run `ce818d4`, DataVo in-memory measured **52.1 ns** mean / **49.3 ns** median, ±2.3 ns error.)*

![Figure 6: Cross-platform point-lookup latency and managed heap allocation per call](/blog/part-2/01-lookup-latency-and-alloc.png)

*Figure 6: Primary-key point lookup latency across Apple M4 Pro, Linux x64 CI, and Windows x64 CI (Panel A), and managed heap allocation per operation for point lookups and filtered `COUNT(*)` (Panel B).*

One caveat deserves repeating plainly: **this benchmark compares DataVo's build-time compiled LINQ path against SQLite and DuckDB over ADO.NET.** When all engines run raw SQL text prepared statements (`sql-workload` over 1,000,000 file-backed rows on the M4 Pro; see [Benchmark Results](/manual/performance/benchmarks#sql-over-1-000-000-rows)), DataVo's point lookup is **1.4 µs** (`INT` key) and **1.2 µs** (`GUID` key) versus SQLite's **2.0 µs** and **2.2 µs**—still faster, but a **1.4–1.8×** advantage rather than **7.7–13×**. The gap between `216 ns` and `1.4 µs` is the cost of owned-result materialization (`Execute` vs `ExecuteBorrowed`) and dictionary-based callers outside the generated reader path.

---

## Prepared statements, AST isolation, and the bridge to reactive views

Not every query is a primary-key lookup, and not every team wants to express every query in LINQ. Underneath both raw SQL and generated LINQ sits **`DataVoPreparedStatement`** (`DataVo.Core/PreparedStatements/DataVoPreparedStatement.cs`).

When you call `ctx.Prepare(sql)`, DataVo runs `Lexer` and `Parser` **once**, stores the resulting `List<SqlStatement>` template, and immediately runs `InitializeFastPathCandidates()` to classify eligible single-table `SELECT`, `UPDATE`, and `DELETE` shapes. On first execution against a database, it compiles and caches a schema-versioned `CompiledSelectPlan` (tagged with `Catalog.GetTableSchemaVersion`) so `ALTER TABLE` invalidates the plan automatically.

### Why template ASTs are never mutated

A subtle concurrency bug in many hand-rolled SQL engines is mutating parameter placeholder nodes in place during binding—which breaks as soon as two threads (or two standing subscriptions) share one prepared statement.

DataVo prevents this structurally through **`SqlParameterBinder`** (`DataVo.Core/PreparedStatements/SqlParameterBinder.cs`). Whenever execution falls back to the general evaluator, or whenever a prepared statement seeds a reactive view, `SqlParameterBinder.BindStatement` performs a clean structural clone of the template AST and substitutes every `ParameterNode` (`@team`, `@k`, including inside `WHERE`, `SELECT`, CTEs, `HAVING`, and `LIMIT/OFFSET`) with immutable `BoundValueLiteralNode` or `NullLiteralNode` instances. The shared template inside `DataVoPreparedStatement` is strictly read-only after construction (Figure 4).

<div class="sketch-diagram light-only">

![Figure 4: Prepared statement AST isolation via SqlParameterBinder serving point queries and reactive views concurrently](/blog/part-2/fig-04-prepared-ast-isolation-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 4: Prepared statement AST isolation via SqlParameterBinder serving point queries and reactive views concurrently](/blog/part-2/fig-04-prepared-ast-isolation-dark.svg)

</div>

*Figure 4: Prepared statement AST isolation via `SqlParameterBinder` serving concurrent executions and independent reactive views from one parsed handle.*

### Owned vs. borrowed execution—and zero-reparse reactive subscriptions

For point queries and tight loops, `DataVoPreparedStatement` exposes two execution modes:

- **`Execute(...)`** copies out an owned `QueryResult` whose dictionaries and lists stay valid forever.
- **`ExecuteBorrowed(...)`** returns the thread-local `RowResultSlot.HitResult` without copying—with 1-to-4 parameter stack-struct overloads (`TwoParams`, `ThreeParams`, `FourParams`) so passing parameters does not even allocate a `params[]` array. Because `RowResultSlot` overwrites its reused boxes on the next call on that thread, the caller must finish reading the row immediately (which generated code always does).

That same AST-isolation pipeline feeds DataVo's Z-set incremental view engine <a class="cite-badge" href="#ref-7">7</a> directly via `BindSingleReactiveStatement`:

```csharp
// Parse once, bind independently for multiple live views:
DataVoPreparedStatement teamTop = ctx.Prepare(
    "SELECT Id, Name, Score FROM Player WHERE Team = @team ORDER BY Score DESC LIMIT @k");

using var redTop3 = ctx.SubscribeZeroAlloc(
    teamTop,
    (in QueryChangeRef change) => ApplyHudDelta(change.Added, change.Updated, change.Removed),
    ("team", "Red"), ("k", 3));

using var blueTop5 = ctx.Subscribe(
    teamTop,
    change => LogTeamChanges(change),
    ("team", "Blue"), ("k", 5));
```

Before attaching any change-capture hook, `BindSingleReactiveStatement` validates that the prepared template contains exactly one `SELECT` statement, rejects unsupported constructs such as `DISTINCT` aggregates, clones an isolated AST bound to `("team", "Red"), ("k", 3)`, and hands that bound AST straight to `ReactiveRegistry`. Multi-tenant, per-room, or per-team views share one prepared SQL template with zero re-parsing and zero shared mutable state (see [Live Reactive Views (Z-set IVM)](/manual/client-interfaces/reactive-views)).

---

## Replacing runtime reflection and `Expression.Compile()` with Roslyn codegen

Traditional .NET LINQ providers (`IQueryable<T>`) work by turning a lambda into a heap-allocated `Expression<Func<T, bool>>` tree, walking that tree at runtime, and compiling a mapper delegate via `Expression.Compile()` or `Reflection.Emit`. Under Native AOT, `Reflection.Emit` is unavailable, `Expression.Compile()` falls back to a slow interpreter, and reflection over arbitrary entity properties fights the IL trimmer <a class="cite-badge" href="#ref-6">6</a>.

`DataVo.Generators` takes the opposite approach: **ordinary `Func<...>` lambdas, zero `Expression` trees, and two Roslyn `IIncrementalGenerator`s** <a class="cite-badge" href="#ref-5">5</a> that move schema discovery, SQL generation, predicate compilation, and row materialization entirely to `csc` build time (Figure 5).

<div class="sketch-diagram light-only">

![Figure 5: Build-time Roslyn source generators versus trimmed Native AOT runtime execution](/blog/part-2/fig-05-roslyn-build-vs-runtime-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 5: Build-time Roslyn source generators versus trimmed Native AOT runtime execution](/blog/part-2/fig-05-roslyn-build-vs-runtime-dark.svg)

</div>

*Figure 5: What moves to build time in `DataVo.Generators` versus what ships inside the trimmed Native AOT binary.*

### 1. `DataVoTableGenerator`: four readers, zero reflection

When you mark a model with `[DataVoTable("players")]`, `DataVoTableGenerator` inspects the type once via `ForAttributeWithMetadataName` and emits `PlayerDataVoExtensions`. Rather than reflecting over properties at startup, it generates four concrete readers—one for every storage and reactive surface in the engine—and registers a `DataVoTableInfo<Player>` automatically via `[ModuleInitializer]`:

```csharp
// Simplified excerpt of what DataVoTableGenerator emits for Player:
private static readonly DataVoPackedReader<Player> ReadPackedRow = static row =>
    new Player()
    {
        Id     = row.ReadInt32(0, "Id"),
        Active = row.ReadBoolean(1, "Active"),
        Level  = row.ReadInt32(2, "Level"),
        Gold   = row.ReadInt64(3, "Gold"),
        Ratio  = row.ReadDouble(4, "Ratio"),
        Name   = row.ReadString(5, "Name"),
    };

private static Player ReadReactiveRow(in DataVo.Core.Runtime.Reactive.RowRef row)
{
    return new Player()
    {
        Id     = row.ReadInt32(0, "Id"),
        Active = row.ReadBoolean(1, "Active"),
        Level  = row.ReadInt32(2, "Level"),
        Gold   = row.ReadInt64(3, "Gold"),
        Ratio  = row.ReadDouble(4, "Ratio"),
        Name   = row.ReadString(5, "Name"),
    };
}
```

Notice there are no string-keyed dictionary lookups in either reader: column ordinals (`0` through `5`) are baked into the IL as compile-time constants. Alongside `ReadPackedRow` and `ReadReactiveRow`, the generator emits `ReadRow` (for prepared-statement results), `ReadStreamedRow` (for `DataVoRow`), `EnsurePlayerTable()`, `GetPlayerById(...)`, `Insert(...)`, and `Update(...)`, caching prepared statements per `(DataVoContext, SessionId)` in a `ConditionalWeakTable`.

### 2. `DataVoLinqInterceptorGenerator`: `[InterceptsLocation]` at every call site

For LINQ-shaped calls on `DataVoTable<T>` (`FirstOrDefault`, `SingleOrDefault`, `Where`, `OrderBy`/`ThenBy`, `Select`, `Sum`, `Min`, `Max`, `ExecuteUpdate`), `DataVoLinqInterceptorGenerator` inspects the lambda's syntax and semantic model and emits a C# 12+ **interceptor** <a class="cite-badge" href="#ref-5">5</a> pointing at the exact source file and call-site location.

For `_players.FirstOrDefault(id, static (p, key) => p.Id == key)`, Roslyn rewrites the call to invoke this generated interceptor directly—no closure, no expression tree, no runtime parsing:

```csharp
// Simplified excerpt of what DataVoLinqInterceptorGenerator emits:
private sealed class Plan0 : DataVoLinqPlan<Player, int>
{
    public static readonly Plan0 Instance = new Plan0();
    private Plan0() : base("Id = @p0", unique: true) { }

    // Enables PackedKeyReader<Player> to skip the prepared statement completely:
    protected override bool TryGetIntegerKey(int state, out long key)
    {
        key = (long)state;
        return true;
    }

    // Fallback path (LSM file, explicit transaction, miss):
    protected override QueryResult Run(DataVoPreparedStatement statement, int state, bool borrowed)
    {
        DataVoQueryParameter p0 = new DataVoQueryParameter("p0", state);
        return borrowed ? statement.ExecuteBorrowed(p0) : statement.Execute(p0);
    }
}

[InterceptsLocation(version: 1, data: "...")]
public static Player? FirstOrDefault0(
    this DataVoTable<Player> table, int state, Func<Player, int, bool> predicate)
{
    return Plan0.Instance.FirstOrDefault(table, state, predicate);
}
```

The same interceptor machinery pays off in two additional places beyond point lookups:

- **Zero-allocation compiled scan kernels (`IRowRefFilter` / `IColumnarFilter`).** For predicates over fixed-width columns (`Where(50, static (p, min) => p.Level >= min).Count()`), `DataVoLinqEmitter` emits a value-type `struct Kernel : IRowRefFilter, IColumnarFilter`. Because the storage scan is generic over `TFilter : struct`, RyuJIT / Native AOT specializes the loop directly over the struct with zero delegate calls and zero row allocations. On 200,000 rows (`[Columnar]` on `Level`, `Gold`, `Ratio`), `Count`, `Sum`, `Min`, and `Max` run in **27.1–89.6 µs** on the M4 Pro (both in memory and on LSM) with **0 B** allocated on the managed heap—versus **504 B** for SQLite and **1,136–1,169 B** for DuckDB (Figure 6, Panel B; see [Compiled Scan Kernels](/manual/internals/compiled-scan-kernels)).
- **Zero-allocation reactive HUD buffers (`SubscribeInto`).** When a composed query ends in `.Select(...)`, `DataVoLinqShapeEmitter` emits a value-type `RowRefProjector : IReactiveRowProjector<TProjection>` struct reading reactive `RowRef` cells by constant ordinal:

```csharp
public readonly record struct HudSlot(int Id, int Score);

var hud = new HudSlot[3];
using var sub = db.Table<Player>()
    .Where(100, static (p, min) => p.Score >= min)
    .OrderByDescending(static p => p.Score)
    .Take(3)
    .Select(static p => new HudSlot(p.Id, p.Score))
    .SubscribeInto(hud.AsMemory(), count => RenderHud(hud.AsSpan(0, count)));
```

Underneath, `BufferedWindowReactiveQuery<HudSlot, RowRefProjector0>` keeps matching rows in a pre-allocated flat `CellValue` slab indexed by unboxed primary-key `CellValue`s and sorted in a dense `int[]` slot array. On `db.DispatchPendingNotifications()`, if the visible window `[Skip .. Skip + min(Take, destination.Length))` changed, it invokes the struct projector in place into `hud.AsSpan(0, count)` with **0 bytes allocated** on the managed heap in steady state.

### Transparent fallbacks when a lambda cannot compile

A source generator that silently falls back to a slow full-table scan is a performance footgun; one that refuses to compile valid C# is frustrating. DataVo steers between the two: any uncompilable filter, sort key, or projection still executes and returns the exact right rows by streaming pages, while emitting an actionable build diagnostic:

- **`DATAVOL001`:** Lambda captures a local variable or instance member (pass it as `state` with a `static` lambda).
- **`DATAVOL002`:** Predicate uses a construct with no compiled form (e.g., arbitrary method call, column-to-column comparison, or arithmetic).
- **`DATAVOL003`:** Project has not enabled `<InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces>`.
- **`DATAVOL004`:** `OrderBy` key or `Select` projection runs in memory over whole models.
- **`DATAVOL005` (error):** `ExecuteUpdate` cannot be compiled (it has no in-memory fallback, so failing at build time prevents a surprise `NotSupportedException` at runtime).

Because `DataVo.Generators` emits strictly C# 9-compatible members outside the `[InterceptsLocation]` attribute, older toolchains without C# 12 interceptors (such as Unity's compiler) still compile `[DataVoTable]` models cleanly—falling back to streaming scans for LINQ lambdas while keeping `Get<Model>ById` and prepared statements fast (see [Roslyn Source Generators](/manual/client-interfaces/source-generators)).

---

## Locking the Native AOT fence (184 warnings to zero)

Declaring a library "Native AOT compatible" is easy; keeping it AOT-clean as features land every week is much harder. One casual `JsonSerializer.Serialize(obj)` without a `JsonSerializerContext`, one `Activator.CreateInstance`, or one `dynamic` dispatch can silently break trimming <a class="cite-badge" href="#ref-6">6</a>.

When we first enabled `<IsAotCompatible>true</IsAotCompatible>` on `DataVo.Core`, the Roslyn trim and AOT analyzers reported **184 diagnostics**. Rather than suppressing them with `[UnconditionalSuppressMessage]`, we ratcheted the baseline to zero in five discrete passes (recorded in `scripts/check-aot-baseline.sh`):

| Milestone | Remaining IL Warnings | What was eliminated |
| --- | ---: | --- |
| **Baseline fence** | 184 | Initial enablement of .NET trim & AOT analyzers across `DataVo.Core` |
| **T1 — Catalog persistence** | 144 | Removed `XmlSerializer` from schema catalog serialization |
| **T2 — Core JSON serialization** | 100 | Replaced `Newtonsoft.Json` / reflection JSON with `System.Text.Json` source generation |
| **T3 — Execution & planner JSON** | 76 | Migrated remaining Volcano / `Select` serialization paths to source-generated STJ |
| **T4 + T5 — Dynamic dispatch & factories** | **0** | Eliminated all `dynamic`/DLR call sites and reflection `Activator` factories |

Once the count reached zero, we locked both `DataVo.Core.csproj` and `DataVo.Data.csproj` permanently by promoting every trim and AOT analyzer code (`IL2026` through `IL3056`) to a compiler error:

```xml
<!-- DataVo.Core/DataVo.Core.csproj -->
<IsAotCompatible Condition="'$(TargetFramework)' == 'net10.0'">true</IsAotCompatible>
<WarningsAsErrors Condition="'$(TargetFramework)' == 'net10.0'">$(WarningsAsErrors);IL2026;IL2046;IL2055;IL2057;IL2070;IL2072;IL2075;IL2080;IL2087;IL2090;IL2091;IL2092;IL2093;IL3050;IL3051;IL3052;IL3053;IL3054;IL3055;IL3056</WarningsAsErrors>
```

Two CI gates enforce this fence on every change:

1. **`scripts/check-aot-baseline.sh`** rebuilds `DataVo.Data` and `DataVo.Core` in `Release` mode, failing immediately if any new reflection or dynamic-code warning appears.
2. **`DataVo.AotSmoke`** (`DataVo.AotSmoke/DataVo.AotSmoke.csproj`) publishes a real standalone native executable with `<PublishAot>true</PublishAot>` and `<InvariantGlobalization>true</InvariantGlobalization>`, then executes it to verify dynamic SQL, `InsertTyped`, `SubscribeZeroAlloc`, `DataVoConnection` / `DataVoDataReader` (ADO.NET), `[DataVoTable]` module-initializer registration, intercepted `Where` / `OrderBy` / `Skip` / `Take` / `Select` / `ExecuteUpdate` / `ExecuteDelete`, `RowRefStream`, `Vector3` HNSW nearest-neighbor search, prepared-statement reactive subscriptions, and zero-allocation `SubscribeInto` inside a trimmed native binary.

One boundary stays explicit: **`DataVo.EntityFrameworkCore` is outside this Native AOT guarantee.** EF Core brings its own upstream AOT and trimming constraints, so AOT-targeted deployments use `DataVo.Core`, `DataVo.Data`, and `DataVo.Generators` directly (see [Native AOT](/manual/client-interfaces/native-aot)).

---

## What this cost in complexity—and where it stops paying off

In Part 1, I promised that Part 2 would explain not only how a 55 ns lookup is built, but **what it cost in complexity**. Stripping 660 nanoseconds out of a lookup is not free magic; it shifts work from runtime into architecture, invariants, and maintenance. Four real costs stand out:

1. **Dual-path parity is an ongoing tax.** `PackedKeyReader<T>`, `CompiledSelectPlan`, `IRowRefFilter`, and the general SQL evaluator are four different ways to answer queries over the same table. Every shortcut must be strictly fail-closed: if a transaction is open, a schema version bumped, a row is `StatusKeptApart`, or a `FLOAT` sum on LSM might reorder floating-point rounding, the fast path must decline cleanly and let the general path produce the exact SQL result. Maintaining bit-for-bit parity across those paths is why Part 6's differential oracle became indispensable.
2. **Low-level concurrency and lifetime hazards.** Lock-free reads over `FlatRowStore` rely on exact `Volatile.Write` / `Volatile.Read` pairing across ARM64 and x64 memory models. Likewise, `ExecuteBorrowed` and `RowResultSlot`'s `Unsafe.Unbox<T>` box reuse are safe *only* because generated readers consume every cell before returning—exposing borrowed mutable boxes carelessly to application code would cause silent action-at-a-distance bugs if a caller stored a reference to a row dictionary across calls.
3. **Compile-time surface limits.** Roslyn interceptors require C# 12+ and `static` lambdas with explicit state arguments (to avoid heap closures). They compile single-table predicates, sorts, paging, projections, aggregates, and bulk updates—they do **not** compile joins, `GROUP BY`, `DISTINCT`, or arbitrary vector expressions, which remain SQL territory. Even on `DataVoTable<T>`, moving a `string` column *before* fixed-width columns in your model disables the fixed-offset byte-scan optimization in `RowRef` kernels because variable-width text shifts subsequent cell offsets.
4. **Amdahl's Law still rules disk durability.** Shaving a point lookup from `1,654 ns` to `216 ns` matters enormously in a 60 FPS game loop, a local telemetry pipeline, or a 100k-ops/sec read cache. It matters **almost nothing** on a durable write waiting for an SSD to flush. As we saw in Part 1, once `LsmStrictFsync = true` hits an Apple APFS SSD or Windows `FlushFileBuffers`, both DataVo and SQLite sit at the exact same **~4.0 ms** wall—roughly **72,000×** longer than our 55 ns lookup.

Choosing the right API layer follows directly from those boundaries (see [Choosing an Interface](/manual/getting-started/choosing-an-interface)): use `[DataVoTable]` + compiled LINQ for hot single-table lookups, aggregates, and HUD windows; use `ctx.Prepare(...)` for parameterized SQL, joins, and multi-tenant reactive views; and use `DataVo.Data` / Dapper when ADO.NET ecosystem compatibility matters more than shaving the last few hundred nanoseconds.

> [!TIP]
> Follow the repository at [`ArintonAkos/DataVo-DBMS`](https://github.com/ArintonAkos/DataVo-DBMS) to inspect `FlatRowStore`, `PackedKeyReader<T>`, `DataVo.Generators`, and `DataVo.AotSmoke` directly—or star the repo to catch Parts 3–7.

---

## What comes next

- **Part 3.** What an fsync costs, across operating systems: the 4 ms tie, the 0.1 ms Linux case, and the Windows scaling problem.
- **Part 4.** Z-sets, from incremental views to a 36 MB sync: wire protocol, offline conflict convergence, and reproducible Docker benchmarks against PostgreSQL 17 and ElectricSQL 1.8.1.
- **Part 5.** HNSW, the JIT, and the exact fallback: recall on clustered data, and why a graph search sometimes loses to a scan.
- **Part 6.** Differential oracle testing: how the matrix found five wrong answers before tuning anything.
- **Part 7.** Building an offline-first .NET 10 app end-to-end: combining Native AOT, filtered `VECTOR(n)` search, live Z-set views, and bidirectional Postgres sync in a real desktop/mobile architecture.

---

## References

<ol class="ref-list">
  <li id="ref-1">Martin Kleppmann. <em>Designing Data-Intensive Applications.</em> Chapter 3: "Storage and Retrieval." O'Reilly Media, 2017.</li>
  <li id="ref-2">SQLite Documentation. "An Introduction To The SQLite C/C++ Interface" (<code>sqlite3_prepare_v2</code>, <code>sqlite3_bind_*</code>, <code>sqlite3_step</code>, <code>sqlite3_column_*</code>) and "Datatypes In SQLite." <a href="https://www.sqlite.org/cintro.html">sqlite.org/cintro.html</a>.</li>
  <li id="ref-3">Microsoft .NET Documentation. "Native interoperability best practices" and "Pinned Object Heap (<code>GC.AllocateArray</code>)." <a href="https://learn.microsoft.com/en-us/dotnet/standard/native-interop/best-practices">learn.microsoft.com</a>.</li>
  <li id="ref-4">ECMA-335 Common Language Infrastructure &amp; .NET Memory Model (<code>System.Threading.Volatile.Read</code> / <code>Volatile.Write</code> acquire-release semantics on weakly ordered ARM64 processors).</li>
  <li id="ref-5">Roslyn / C# Language Design. "Interceptors" (<code>[InterceptsLocation]</code>) and "Incremental Generators" (<code>IIncrementalGenerator</code>) specifications. <a href="https://github.com/dotnet/roslyn/blob/main/docs/features/interceptors.md">github.com/dotnet/roslyn</a>.</li>
  <li id="ref-6">Microsoft .NET Documentation. "Native AOT deployment overview" and "Trimming and AOT warnings (<code>IL2026</code>, <code>IL3050</code>)." <a href="https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/">learn.microsoft.com</a>.</li>
  <li id="ref-7">Mihai Budiu, Frank McSherry, Leonid Ryzhyk, Val Tannen. "DBSP: Automatic Incremental View Maintenance for Rich Query Languages." <em>Proceedings of the VLDB Endowment</em> 16(7), pp. 1601–1614, 2023.</li>
</ol>

### Data sources

- **Source code audited:** `DataVo.Core/StorageEngine/Memory/{FlatRowStore,RowLayout,FlatRowCodec}.cs`, `DataVo.Core/Memory/Flat/{FlatChunk,FlatSlotList,ReferenceDirectory}.cs`, `DataVo.Core/Tables/{DataVoTable,DataVoLinqPlan,PackedKeyReader,DataVoPackedRow,DataVoReactiveLinq}.cs`, `DataVo.Core/PreparedStatements/{DataVoPreparedStatement,DataVoPreparedStatement.Plans,SqlParameterBinder}.cs`, `DataVo.Core/StorageEngine/Lsm/LsmStorageEngine.cs`, `DataVo.Core/Streaming/RowRef.cs`, `DataVo.Core/Runtime/Reactive/{IReactiveRowProjector,BufferedWindowReactiveQuery}.cs`, `DataVo.Generators/{DataVoTableGenerator,DataVoLinqInterceptorGenerator}.cs`, `DataVo.Generators/Tables/DataVoTableEmitter.cs`, `DataVo.Generators/Linq/{DataVoLinqEmitter,DataVoLinqShapeEmitter}.cs`, `DataVo.AotSmoke/{DataVo.AotSmoke.csproj,Program.cs,GeneratedTableSmoke.cs}`, and `scripts/check-aot-baseline.sh`.
- **Benchmarks:** Apple M4 Pro BenchmarkDotNet `0.15.8` (`02fa240f`, 20 Nov 2026, `--job short`, `docs/public/benchmarks/2026-11-21-m4pro/DataVo.Benchmarks.{PointLookup,FilteredAggregate}Benchmarks-report-github.md`), earlier M4 Pro full-job point-lookup run (`ce818d4`, 13 Nov 2026, `docs/manual/performance/benchmarks.md`), and GitHub Actions Linux x64 & Windows x64 CI run `44748508039` (`master` @ `02fa240f`, 20 Nov 2026).
- **Diagram & chart scripts:** `docs/public/blog/part-2/make_diagrams.mjs` (`roughjs` 4.6.6) and `docs/public/blog/part-2/make_charts.py` (`matplotlib` 3.10.8).
