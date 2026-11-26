---
title: "Packed byte buffers, Native AOT, and Roslyn codegen: how a 55 ns point lookup works with zero native binaries"
description: "Part 2 of 7: How packed byte buffers, lock-free reads, AST-isolated prepared statements, and compile-time Roslyn codegen deliver 55 ns point lookups in pure .NET 10 with zero native binaries."
outline: [2, 3]
prev:
  text: "Part 1: Where it wins and where it loses"
  link: "/blog/part-1-where-it-wins-and-where-it-loses"
next: false
---

# Packed byte buffers, Native AOT, and Roslyn codegen: how a 55 ns point lookup works with zero native binaries

*Part 2 of 7.*

In [Part 1](/blog/part-1-where-it-wins-and-where-it-loses), one benchmark line stood out above the rest: looking up a single row by primary key out of 100,000 shuffled records took **55.0 ns** in memory and **216.2 ns** on LSM disk files on an Apple M4 Pro (**7.7–13× faster** than SQLite), allocating **56 B** in memory (the size of the returned `Player` object itself) and **144 B** on disk, versus **608 B** for `Microsoft.Data.Sqlite`.

Why does a C# implementation finish an in-memory lookup roughly an order of magnitude faster than SQLite, a C library?

The difference is boundary and abstraction cost, not instruction throughput. Calling into a C library from .NET crosses a P/Invoke boundary and steps through a dynamically typed C API on every row. When the storage engine lives in the same managed compilation unit as the caller, and the C# compiler knows the table layout and query predicate at build time, those layers are replaced by inlined bounds checks, aligned loads, and a single object allocation.

At a glance, five engineering choices drive both the speed and the packaging story:

- **Zero native binaries and zero interop marshalling:** Keeping the entire engine in pure .NET 10 removes per-OS native libraries and lets the JIT and AOT compilers inline straight from your query call into storage buffers.
- **Cache-aligned packed byte buffers:** Fixed-width columns sit contiguously inside 64-byte aligned Pinned Object Heap buffers rather than per-row objects, shrinking retained memory **13×** from **640.3 B/row** to **49.2 B/row** (with shared strings).
- **Lock-free optimistic reads:** In-memory primary-key lookups take no lock, pairing opposite volatile write/read ordering so a concurrent delete can never surface a half-read row.
- **Immutable prepared-statement templates:** Parsing SQL once and cloning parameter-bound syntax trees lets concurrent threads, LSM point lookups, and live reactive views share one prepared query safely.
- **Compile-time Roslyn code generation under Native AOT:** Incremental source generators replace runtime reflection and expression-tree compilation with static readers and build-time interceptors, passing `PublishAot=true` with **0** trim/AOT warnings.

Each of these optimizations trades runtime cost for additional parity invariants against the general SQL evaluator, memory-ordering constraints, or compiler requirements; the final section examines those trade-offs.

::: tip Source & Benchmarks
Explore or bookmark the source code, Roslyn generators, Native AOT smoke tests, and reproducible benchmarks at [`ArintonAkos/DataVo-DBMS`](https://github.com/ArintonAkos/DataVo-DBMS).
:::

## The invisible tax of crossing the managed–native boundary

When a .NET application reads one row from an in-memory SQLite database via a prepared `SqliteCommand`, the lookup is far more than a B-tree probe in RAM. Even after `Prepare()` compiles the SQL into VDBE bytecode, every row fetch crosses an ABI boundary and navigates a dynamically typed C API <a class="cite-badge" href="#ref-2">2</a> <a class="cite-badge" href="#ref-3">3</a> (Figure 1).

1. **Managed command and parameter overhead.** Setting `_sqliteId.Value = id` boxes the integer into `object?`, and `ExecuteReader()` allocates a `SqliteDataReader` and steps through `Microsoft.Data.Sqlite`'s state machine.
2. **Crossing the P/Invoke boundary.** Each call into `e_sqlite3` (`sqlite3_reset`, `sqlite3_bind_int64`, `sqlite3_step`, and one `sqlite3_column_*` call per column) crosses an unmanaged calling convention. The .NET JIT cannot inline across `DllImport` or eliminate redundant checks, so it pays a fixed transition tax on every invocation.
3. **Dynamic column typing and string transcoding.** SQLite uses dynamic type affinity per cell <a class="cite-badge" href="#ref-2">2</a>. Reading `Name` calls `sqlite3_column_text`, inspects a null-terminated UTF-8 pointer in unmanaged memory, and transcodes it into a newly allocated UTF-16 `.NET` `string` on every lookup, even in `:memory:`.
4. **Optional ORM mapping overhead.** Mapping rows through Dapper adds dynamic reader-to-POCO reflection on top of ADO.NET (DataVo supports Dapper via `DataVoConnection` as documented in [Dapper](/manual/client-interfaces/dapper), though the BenchmarkDotNet suite below tests raw ADO.NET readers rather than Dapper). Under Native AOT, reflection-heavy mapping also conflicts with IL trimming.

<div class="sketch-diagram light-only">

![Figure 1: SQLite managed-to-native P/Invoke boundary versus DataVo single managed compilation unit](/blog/part-2/fig-01-interop-boundary-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 1: SQLite managed-to-native P/Invoke boundary versus DataVo single managed compilation unit](/blog/part-2/fig-01-interop-boundary-dark.svg)

</div>

*Figure 1: SQLite managed-to-native P/Invoke boundary versus DataVo's single managed compilation unit.*

Keeping the engine in managed C# eliminates that entire boundary. On `net10.0`, `DataVo.Core.csproj` has only one NuGet package reference (`System.Numerics.Tensors` `10.0.0`, which compiles to managed SIMD intrinsics), and `DataVo.Data.csproj` references only `DataVo.Core`, leaving zero `.dll`, `.so`, or `.dylib` files per target platform and zero UTF-8-to-UTF-16 transcoding for in-memory strings.

Yet removing the FFI wall alone is not enough: an earlier pure-managed build of DataVo still took **405–566 ns** and allocated **1,008 B** per in-memory lookup (slower than its own LSM disk engine at **241 ns**) because every row still lived as a scattered heap object.

## Packing rows into cache-aligned byte buffers (`FlatRowStore`)

In a straightforward managed in-memory store, each row is an object holding a `CellValue[]` or dictionary. On our six-column `Player` benchmark model, that object-per-row layout retained **640.3 bytes per row** (with shared strings), forcing the CPU to chase scattered pointers and handing the garbage collector hundreds of thousands of heap references to scan during Gen-2 collections <a class="cite-badge" href="#ref-1">1</a>.

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

To replace per-row objects without risking manual `Marshal.AllocHGlobal` memory management, DataVo's in-memory storage packs rows into **`FlatRowStore`** (`DataVo.Core/StorageEngine/Memory/FlatRowStore.cs`), splitting each table into two parallel 4,096-slot chunk structures indexed by an immutable, append-only row number.

### 64-byte aligned Pinned Object Heap chunks

Fixed-width columns live inside `FlatSlotList` → `FlatChunk` (`DataVo.Core/Memory/Flat/FlatChunk.cs`). Each chunk allocates 4,096 slots (`1 << 12`; tiny tables start at `FirstChunkRows = 16`) directly on the .NET 10 **Pinned Object Heap (POH)** <a class="cite-badge" href="#ref-3">3</a> and offsets slot 0 to a 64-byte cache-line boundary:

```csharp
// DataVo.Core/Memory/Flat/FlatChunk.cs
_memory = GC.AllocateArray<byte>(length + Alignment - 1, pinned: true);
_start = AlignmentGap(_memory); // Alignment = 64
```

Because the GC owns the chunk and never moves or frees it while referenced, a lock-free reader holding a `ReadOnlySpan<byte>` can never touch dangling memory. Variable-length managed references (strings and `float[]` vectors) cannot sit inside raw byte arrays without hiding pointers from the GC, so `ReferenceDirectory` stores them in parallel `object?[]` chunks indexed by the same slot number (`slot & 4095`).

### Widest-first alignment and the 32-byte `Player` record

Inside each slot, `RowLayout.Create` (`DataVo.Core/StorageEngine/Memory/RowLayout.cs`) packs fixed-width columns **widest first**—16-byte (`Decimal`, `Guid`), 8-byte (`Int64`, `Double`), 4-byte (`Int32`, `Date`), then 1-byte (`Boolean`)—followed by a 1-byte status flag, a null bitmask (`(cols + 7) >> 3` bytes), and 8-byte alignment padding. Every primitive sits on a multiple of its own width with zero interior padding holes, compressing `Player` into **32 bytes** plus one reference slot for `Name` (Figure 2):

- `Gold` (`Int64`, 8 B): bytes `0..7` · `Ratio` (`Double`, 8 B): bytes `8..15`
- `Id` (`Int32`, 4 B): bytes `16..19` · `Level` (`Int32`, 4 B): bytes `20..23`
- `Active` (`Boolean`, 1 B): byte `24` · `Status`: byte `25` (`0 = Empty`, `1 = Packed`, `2 = KeptApart`)
- `NullBits`: byte `26` · Padding: bytes `27..31` → `RecordBytes = 32`

<div class="sketch-diagram light-only">

![Figure 2: FlatRowStore 32-byte widest-first packed record layout for Player and parallel ReferenceDirectory](/blog/part-2/fig-02-flatrowstore-layout-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 2: FlatRowStore 32-byte widest-first packed record layout for Player and parallel ReferenceDirectory](/blog/part-2/fig-02-flatrowstore-layout-dark.svg)

</div>

*Figure 2: `FlatRowStore` 32-byte widest-first packed record layout for `Player` and its parallel `ReferenceDirectory`.*

Switching to `FlatRowStore` cut retained row-store memory **13×**: from **640.3 B/row** down to **49.2 B/row** with shared strings (**145.3 B/row** with unique `"player-N"` strings; **231.6 B/row** including the primary-key index). If dynamic SQL inserts a mismatched cell type (such as a `BIGINT` into an `INT` column), `FlatRowStore.Add` spills only that row into a side `ConcurrentDictionary<int, CellValue[]>` marked `StatusKeptApart = 2` so ordinary rows pay zero overhead.

For streaming scans over serialized rows (including LSM SSTables), DataVo applies the same principle via **`RowRef`** (`DataVo.Core/Streaming/RowRef.cs`), a stack-only `readonly ref struct` wrapping `ReadOnlySpan<byte>` that decodes cells on demand without heap allocations (see [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows)).

## Tracing a 55 ns point lookup and the 216 ns LSM fallback

Here is the BenchmarkDotNet lookup in [PointLookupBenchmarks.cs](https://github.com/ArintonAkos/DataVo-DBMS/blob/master/benchmarks/DataVo.Benchmarks/PointLookupBenchmarks.cs):

```csharp
// Intercepted at build time via [InterceptsLocation]:
Player? found = _players.FirstOrDefault(id, static (p, key) => p.Id == key);
```

At build time, `DataVo.Generators` intercepts that call site and routes it to a generated `DataVoLinqPlan<Player, int>` that extracts the integer primary key directly without building SQL or boxing parameters. At runtime, execution splits cleanly between the in-memory shortcut and the LSM disk path (Figure 3).

<div class="sketch-diagram light-only">

![Figure 3: 55 ns lock-free packed lookup versus 216 ns LSM clustered point path](/blog/part-2/fig-03-lookup-pipeline-and-liveness-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 3: 55 ns lock-free packed lookup versus 216 ns LSM clustered point path](/blog/part-2/fig-03-lookup-pipeline-and-liveness-dark.svg)

</div>

*Figure 3: The 55 ns lock-free `PackedKeyReader<T>` shortcut versus the 216 ns LSM `ClusteredPoint` path.*

### Path A: In-memory `PackedKeyReader<T>` (55.0 ns, 56 B)

When `DataVoLinqPlan.FirstOrDefault` calls `table.TryReadByIntegerKey(key, out item)`, `PackedKeyReader<T>` (`DataVo.Core/Tables/PackedKeyReader.cs`) first checks a fail-closed guard (`MustUseStatement()`): it steps aside if storage is not packed, authorization or diagnostics are enabled, or the session is inside an explicit transaction, MVCC snapshot, or active row write lock.

Otherwise, it reads **without taking any lock**:

1. Probes the cached concurrent primary-key map (`ConcurrentInt64Int64Map`) for `key → rowId`.
2. Borrows a `FlatRow` span at `position = (int)(rowId - 1)` via `rows.TryReadPacked(...)` and verifies `binding.KeyOf(row.Record) == key`.
3. Wraps the span in a stack-only `DataVoPackedRow` `ref struct` and calls the generated `ReadPackedRow` delegate to populate `new Player { ... }` directly from fixed byte offsets.
4. Verifies `rows.IsTypedLive(position)` **after** constructing `Player` so a concurrent delete can never return a torn or half-cleared row.

<details>
<summary>Details: lock-free ARM64 liveness proof &amp; 56-byte allocation math</summary>

A packed row in `FlatRowStore` is immutable once published: its fixed-width bytes are never rewritten in place. The only concurrent mutation is **`FlatRowStore.Remove(index)`**, which writes the empty status byte *before* clearing managed references:

```csharp
// Writer: DataVo.Core/StorageEngine/Memory/FlatRowStore.cs + RowLayout.cs
Layout.MarkEmpty(record);                   // 1. Volatile.Write(ref record[StatusOffset], 0)
FlatRowCodec.ClearReferences(Layout, ...);  // 2. Volatile.Write(ref references[base + slot], null)
```

`PackedKeyReader<T>.TryReadFrom` reads in the **exact opposite volatile order** <a class="cite-badge" href="#ref-4">4</a>:

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

Because the writer issues a release store on `Status = Empty` before clearing `references`, and the reader issues acquire loads on every reference before checking `IsTypedLive`, any reader that sees a cleared reference is guaranteed to also see `Status = Empty`, even on weakly ordered ARM64 cores like the Apple M4 Pro. If a delete raced with the lookup, the partial object is discarded and the call falls back cleanly to the prepared statement.

The **56 bytes** of managed heap allocation on 64-bit .NET are the exact size of the `Player` instance itself: 16 B object/MethodTable header + 8 B `Gold` + 8 B `Ratio` + 8 B `Name` reference + 4 B `Id` + 4 B `Level` + 1 B `Active` + 7 B alignment padding = **56 B** (**0 B** engine overhead).

</details>

### Path B: LSM file `ClusteredPoint` (216.2 ns, 144 B)

Under `StorageMode.Lsm`, `PackedKeyReader<T>` declines (`!_packedStorage`), so `DataVoLinqPlan` executes the slot's cached `DataVoPreparedStatement` via `ExecuteBorrowed(p0)`. Because `Player` has a single `INT` primary key, LSM stores rows **clustered** directly under that key, so `SelectAccessKind.ClusteredPoint` (`DataVoPreparedStatement.Plans.cs`) skips any secondary index hop and calls `LsmStorageEngine.TryVisitStoredRowByKey`.

`LsmStorageEngine` probes the lock-free `LatestRows` write overlay first, then acquires an immutable `TableVersion` to search memory-mapped SSTable V2 files (`SsTableV2Reader.Lookup`) into a thread-local `_singleRowScratch` buffer. `VisitClusteredPointRow` decodes only the projected columns into a reusable thread-local `RowResultSlot` (avoiding fresh primitive boxes), and `ReadRow` constructs `Player` (`56 B`) plus the decoded UTF-8 `Name` string (`~48–56 B`), totaling **144 B** and **216.2 ns**.

<details>
<summary>Details: reusing boxed primitives in RowResultSlot</summary>

When returning rows through `ExecuteBorrowed`, `RowResultSlot` (`DataVo.Core/PreparedStatements/DataVoPreparedStatement.Plans.cs`) avoids allocating fresh boxed `int`, `long`, `float`, `double`, and `Guid` objects on every lookup by reusing pre-allocated per-slot boxes in place (using `BoxedIntegerCache` for `-1..32767` and `Unsafe.Unbox<T>` for everything else):

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

This is safe only because `ExecuteBorrowed` callers, such as generated `ReadRow` delegates, unbox every cell immediately before the next query executes on the same thread.

</details>

### Cross-platform BenchmarkDotNet results

Figure 4 and the table below summarize `PointLookupBenchmarks` across Apple M4 Pro (`02fa240f`), Linux x64 CI, and Windows x64 CI (run `44748508039`).

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

*(On the full-job 13 November M4 Pro run `ce818d4`, DataVo in-memory measured **52.1 ns** mean / **49.3 ns** median, ±2.3 ns error.)*

![Figure 4: Cross-platform point-lookup latency and managed heap allocation per call](/blog/part-2/01-lookup-latency-and-alloc.png)

*Figure 4: Primary-key point lookup latency across Apple M4 Pro, Linux x64 CI, and Windows x64 CI (Panel A), and managed heap allocation per operation for point lookups and filtered `COUNT(*)` (Panel B).*

**Scope note:** this benchmark compares DataVo's build-time compiled LINQ path against SQLite and DuckDB over ADO.NET. When all engines run raw SQL text prepared statements (`sql-workload` over 1,000,000 file-backed rows on the M4 Pro; see [Benchmark Results](/manual/performance/benchmarks#sql-over-1-000-000-rows)), DataVo takes **1.4 µs** (`INT` key) and **1.2 µs** (`GUID` key) versus SQLite's **2.0 µs** and **2.2 µs** (**1.4–1.8×** faster), because raw SQL callers pay for owned `QueryResult` dictionaries rather than `ExecuteBorrowed`.

## Prepared statements, AST isolation, and the bridge to reactive views

Underneath both raw SQL and generated LINQ sits **`DataVoPreparedStatement`** (`DataVo.Core/PreparedStatements/DataVoPreparedStatement.cs`). Calling `ctx.Prepare(sql)` runs `Lexer` and `Parser` **once**, stores the template `List<SqlStatement>`, and runs `InitializeFastPathCandidates()` to classify eligible single-table `SELECT`, `UPDATE`, and `DELETE` shapes. On first execution it caches a schema-versioned `CompiledSelectPlan` that automatically invalidates on `ALTER TABLE`.

### Why template ASTs are never mutated

Mutating parameter placeholder nodes in place during binding breaks concurrency as soon as two threads or two standing views share one prepared statement. DataVo prevents that structurally via **`SqlParameterBinder`** (`DataVo.Core/PreparedStatements/SqlParameterBinder.cs`): whenever execution falls back to the general evaluator or seeds a reactive view, `BindStatement` clones the template AST and replaces every `ParameterNode` (across `WHERE`, `SELECT`, CTEs, `HAVING`, and `LIMIT/OFFSET`) with immutable `BoundValueLiteralNode` or `NullLiteralNode` values, keeping the shared template strictly read-only (Figure 5).

<div class="sketch-diagram light-only">

![Figure 5: Prepared statement AST isolation via SqlParameterBinder serving point queries and reactive views concurrently](/blog/part-2/fig-04-prepared-ast-isolation-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 5: Prepared statement AST isolation via SqlParameterBinder serving point queries and reactive views concurrently](/blog/part-2/fig-04-prepared-ast-isolation-dark.svg)

</div>

*Figure 5: Prepared statement AST isolation via `SqlParameterBinder` serving concurrent executions and independent reactive views from one parsed handle.*

### Owned vs. borrowed execution and zero-reparse reactive subscriptions

`DataVoPreparedStatement` provides `Execute(...)` (returning an owned `QueryResult`) alongside `ExecuteBorrowed(...)`, which returns the thread-local `RowResultSlot.HitResult` and uses 1-to-4 parameter stack-struct overloads (`TwoParams`, `ThreeParams`, `FourParams`) to avoid `params[]` array allocations. That same AST-isolation pipeline feeds DataVo's Z-set incremental view engine <a class="cite-badge" href="#ref-7">7</a> through `BindSingleReactiveStatement`:

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

Before attaching any capture hook, `BindSingleReactiveStatement` validates that the template holds a single `SELECT`, rejects unsupported constructs like `DISTINCT` aggregates, clones an isolated AST bound to each subscription's parameters, and passes it straight to `ReactiveRegistry` with zero SQL re-parsing (see [Live Reactive Views (Z-set IVM)](/manual/fundamentals/reactive-views)).

## Replacing runtime reflection and `Expression.Compile()` with Roslyn codegen

Traditional .NET LINQ providers turn lambdas into heap-allocated `Expression<Func<T, bool>>` trees, walk them at runtime, and compile mappers via `Expression.Compile()` or `Reflection.Emit`. Under Native AOT, `Reflection.Emit` is unavailable, `Expression.Compile()` drops to a slow interpreter, and reflection over arbitrary model properties fights the IL trimmer <a class="cite-badge" href="#ref-6">6</a>.

`DataVo.Generators` flips that pipeline: it uses ordinary `Func<...>` lambdas, zero `Expression` trees, and two Roslyn `IIncrementalGenerator`s <a class="cite-badge" href="#ref-5">5</a> to move schema discovery, SQL generation, predicate compilation, and row materialization entirely to `csc` build time (Figure 6).

<div class="sketch-diagram light-only">

![Figure 6: Build-time Roslyn source generators versus trimmed Native AOT runtime execution](/blog/part-2/fig-05-roslyn-build-vs-runtime-light.svg)

</div>
<div class="sketch-diagram dark-only">

![Figure 6: Build-time Roslyn source generators versus trimmed Native AOT runtime execution](/blog/part-2/fig-05-roslyn-build-vs-runtime-dark.svg)

</div>

*Figure 6: What moves to build time in `DataVo.Generators` versus what ships inside the trimmed Native AOT binary.*

### `DataVoTableGenerator`: four readers, zero reflection

Marking a model `[DataVoTable("players")]` triggers `DataVoTableGenerator` via `ForAttributeWithMetadataName` to emit `PlayerDataVoExtensions`, registering `DataVoTableInfo<Player>` automatically through `[ModuleInitializer]`. Instead of reflecting over properties at startup, it generates four specialized readers—`ReadRow`, `ReadStreamedRow`, `ReadPackedRow`, and `ReadReactiveRow`—with column ordinals baked into IL as compile-time constants:

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

### `DataVoLinqInterceptorGenerator`: `[InterceptsLocation]` at every call site

For LINQ-shaped calls on `DataVoTable<T>` (`FirstOrDefault`, `SingleOrDefault`, `Where`, `OrderBy`/`ThenBy`, `Select`, `Sum`, `Min`, `Max`, `ExecuteUpdate`), `DataVoLinqInterceptorGenerator` emits C# 12+ `[InterceptsLocation]` interceptors <a class="cite-badge" href="#ref-5">5</a> targeting each exact call site. For `_players.FirstOrDefault(id, static (p, key) => p.Id == key)`, Roslyn rewrites the call to invoke the generated plan directly:

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

Beyond point lookups, the same generator powers two other zero-allocation paths:

- **Compiled scan kernels (`IRowRefFilter` / `IColumnarFilter`).** For predicates over fixed-width columns (`Where(50, static (p, min) => p.Level >= min).Count()`), `DataVoLinqEmitter` emits a value-type `struct Kernel`. Generic specialization across the storage loop eliminates delegate calls and row allocations, running `[Columnar]` `Count`, `Sum`, `Min`, and `Max` over 200,000 rows in **27.1–89.6 µs** with **0 B** heap allocation versus **504 B** for SQLite and **1,136–1,169 B** for DuckDB (Figure 4, Panel B; see [Compiled Scan Kernels](/manual/internals/compiled-scan-kernels)).
- **Zero-allocation reactive HUD buffers (`SubscribeInto`).** When a composed query ends in `.Select(...)`, `DataVoLinqShapeEmitter` emits a value-type `IReactiveRowProjector<TProjection>` struct reading `RowRef` cells by constant ordinal into `BufferedWindowReactiveQuery`, updating a caller-owned `Memory<HudSlot>` slice on `DispatchPendingNotifications()` with **0 bytes** allocated in steady state:

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

### Transparent fallbacks when a lambda cannot compile

Any filter, sort key, or projection that the generator cannot translate still executes and returns exact results by streaming pages, while emitting a build diagnostic explaining why. Outside `[InterceptsLocation]`, emitted code stays within C# 9 so older compilers (such as Unity's) compile `[DataVoTable]` models cleanly and keep `Get<Model>ById` and prepared statements fast (see [Roslyn Source Generators](/manual/client-interfaces/source-generators)).

<details>
<summary>Compiler fallback diagnostics (DATAVOL001–DATAVOL005)</summary>

| Diagnostic | Severity | Meaning |
| --- | --- | --- |
| `DATAVOL001` | Warning | Lambda captures a local variable or instance member; pass it as `state` with a `static` lambda. |
| `DATAVOL002` | Warning | Predicate uses an untranslatable construct (arbitrary method call, arithmetic, column-to-column comparison, or mismatched type). |
| `DATAVOL003` | Warning | Project has not enabled `<InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces>`. |
| `DATAVOL004` | Warning | `OrderBy` key or `Select` selector runs in memory over whole models. |
| `DATAVOL005` | **Error** | `ExecuteUpdate` cannot be compiled (it has no in-memory fallback, so failing at build time prevents a runtime `NotSupportedException`). |

</details>

## Locking the Native AOT fence (184 warnings to zero)

Maintaining Native AOT compatibility across ongoing changes requires automated enforcement. A single unannotated `JsonSerializer.Serialize`, `Activator.CreateInstance`, or `dynamic` call site can break trimming <a class="cite-badge" href="#ref-6">6</a>.

When we first enabled `<IsAotCompatible>true</IsAotCompatible>` on `DataVo.Core`, the Roslyn trim and AOT analyzers reported **184 diagnostics**. Rather than hiding them behind `[UnconditionalSuppressMessage]`, we ratcheted the baseline down to zero across five passes (recorded in `scripts/check-aot-baseline.sh`):

| Milestone | Remaining IL Warnings | What was eliminated |
| --- | ---: | --- |
| **Baseline fence** | 184 | Initial enablement of .NET trim & AOT analyzers across `DataVo.Core` |
| **T1: Catalog persistence** | 144 | Removed `XmlSerializer` from schema catalog serialization |
| **T2: Core JSON serialization** | 100 | Replaced `Newtonsoft.Json` / reflection JSON with `System.Text.Json` source generation |
| **T3: Execution & planner JSON** | 76 | Migrated remaining Volcano / `Select` serialization paths to source-generated STJ |
| **T4 + T5: Dynamic dispatch & factories** | **0** | Eliminated all `dynamic`/DLR call sites and reflection `Activator` factories |

Once the count hit zero, we locked both `DataVo.Core.csproj` and `DataVo.Data.csproj` by promoting every trim and AOT analyzer code (`IL2026` through `IL3056`) to a build error:

```xml
<!-- DataVo.Core/DataVo.Core.csproj -->
<IsAotCompatible Condition="'$(TargetFramework)' == 'net10.0'">true</IsAotCompatible>
<WarningsAsErrors Condition="'$(TargetFramework)' == 'net10.0'">$(WarningsAsErrors);IL2026;IL2046;IL2055;IL2057;IL2070;IL2072;IL2075;IL2080;IL2087;IL2090;IL2091;IL2092;IL2093;IL3050;IL3051;IL3052;IL3053;IL3054;IL3055;IL3056</WarningsAsErrors>
```

Two CI gates enforce this fence on every commit: `scripts/check-aot-baseline.sh` rebuilds both locked libraries in `Release`, and **`DataVo.AotSmoke`** publishes a standalone native executable (`<PublishAot>true</PublishAot>`, `<InvariantGlobalization>true</InvariantGlobalization>`) that runs dynamic SQL, `InsertTyped`, `SubscribeZeroAlloc`, ADO.NET (`DataVoConnection`/`DataVoDataReader`), `[DataVoTable]` registration, intercepted LINQ (`Where`, `OrderBy`, `Skip`, `Take`, `Select`, `ExecuteUpdate`, `ExecuteDelete`), `RowRefStream`, HNSW `Vector3` search, prepared-statement reactive views, and `SubscribeInto` inside a trimmed native binary. Only `DataVo.EntityFrameworkCore` sits outside this guarantee due to EF Core's own upstream AOT constraints (see [Native AOT](/manual/client-interfaces/native-aot)).

## What this cost in complexity, and where it stops paying off

In Part 1, I promised that Part 2 would explain not only how a 55 ns lookup is built, but **what it cost in complexity**. Shaving 660 nanoseconds out of a lookup shifts work into invariants, concurrency subtleties, and surface constraints:

1. **Dual-path parity is an ongoing tax.** `PackedKeyReader<T>`, `CompiledSelectPlan`, `IRowRefFilter`, and the general SQL evaluator are four distinct execution paths over the same tables. Every shortcut must fail closed: if a transaction is open, a schema version changed, a row is `StatusKeptApart`, or a `FLOAT` sum on LSM might reorder rounding, the fast path must decline cleanly so the general evaluator produces exact SQL semantics. Keeping those paths bit-for-bit identical is why Part 6's differential oracle became essential.
2. **Lock-free ordering and borrowed lifetimes.** Optimistic reads over `FlatRowStore` rely on exact `Volatile.Write` / `Volatile.Read` pairing on weakly ordered CPUs. Likewise, `RowResultSlot`'s `Unsafe.Unbox<T>` box reuse in `ExecuteBorrowed` is safe *only* because generated readers unbox immediately before returning. Exposing borrowed mutable boxes carelessly to callers would invite silent aliasing bugs.
3. **Compile-time surface limits.** Roslyn interceptors require C# 12+ and `static` lambdas with explicit state arguments. They compile single-table predicates, sorts, paging, projections, aggregates, and bulk updates—not joins, `GROUP BY`, `DISTINCT`, or arbitrary vector expressions, which remain SQL territory. Even on `DataVoTable<T>`, placing a `string` column *before* fixed-width columns disables fixed-offset byte-scan kernels because variable-width text shifts subsequent cell offsets.
4. **Amdahl's Law still rules disk durability.** Cutting a lookup from `1,654 ns` to `216 ns` matters directly in a 60 FPS game loop, telemetry pipeline, or high-QPS read cache. It barely registers on a durable write waiting for an SSD flush: with `LsmStrictFsync = true` on Apple's APFS SSD or Windows `FlushFileBuffers`, both DataVo and SQLite hit the same **~4.0 ms** floor (roughly **72,000×** longer than a 55 ns lookup).

In practice, let your workload pick the interface (see [Choosing an Interface](/manual/getting-started/choosing-an-interface)): use `[DataVoTable]` + compiled LINQ for hot single-table lookups, aggregates, and HUD windows; use `ctx.Prepare(...)` for parameterized SQL, joins, and multi-tenant reactive views; and use `DataVo.Data` / Dapper when ADO.NET ecosystem compatibility matters more than shaving the last few hundred nanoseconds.

::: tip Source Code
`FlatRowStore`, `PackedKeyReader<T>`, `DataVo.Generators`, and `DataVo.AotSmoke` are available in [`ArintonAkos/DataVo-DBMS`](https://github.com/ArintonAkos/DataVo-DBMS).
:::

## What comes next

- **Part 3.** What an fsync costs, across operating systems: the 4 ms tie, the 0.1 ms Linux case, and the Windows scaling problem.
- **Part 4.** Z-sets, from incremental views to a 36 MB sync: wire protocol, offline conflict convergence, and reproducible Docker benchmarks against PostgreSQL 17 and ElectricSQL 1.8.1.
- **Part 5.** HNSW, the JIT, and the exact fallback: recall on clustered data, and why a graph search sometimes loses to a scan.
- **Part 6.** Differential oracle testing: how the matrix found five wrong answers before tuning anything.
- **Part 7.** Building an offline-first .NET 10 app end-to-end: combining Native AOT, filtered `VECTOR(n)` search, live Z-set views, and bidirectional Postgres sync in a real desktop/mobile architecture.

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
