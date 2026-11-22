# Compiled Scan Kernels

> Source route: /manual/internals/compiled-scan-kernels
> Source file: manual/internals/compiled-scan-kernels.md

When you write `db.Table<Player>().Where(static p => p.Score >= 100).Count()`, DataVo does not build an expression tree or invoke a delegate per row. Instead, the `DataVo.Generators` Roslyn source generator inspects the lambda at build time and emits a `[InterceptsLocation]` method with a value-type condition struct that tests stored row bytes directly inside the storage loop.

```csharp
// Intercepted at compile time → 0 B allocated per query, Native AOT safe
int topCount = db.Table<Player>()
    .Where(100, static (p, min) => p.Score >= min)
    .Count();

int totalScore = db.Table<Player>()
    .Where(100, static (p, min) => p.Score >= min)
    .Sum(static p => p.Score);
```

## Build-Time Compilation Pipeline

1. **Analyze:** The incremental Roslyn generator locates `DataVoTable<T>` query chains and validates static lambdas containing comparisons, boolean checks, null tests, `&&`, `||`, and parentheses.
2. **Emit interceptor:** For each qualifying call site, the generator emits an interceptor in the `DataVo.Generated` namespace marked with `[InterceptsLocation]`, routing that exact call to generated C# with zero runtime reflection.
3. **Specialize the condition struct:** When the predicate references only unindexed fixed-width columns (`int`, `long`, `float`, `double`, `bool`, `Guid`, `DataVoDate`, `DateOnly`), the generator emits a `struct` implementing the engine's predicate interface.
4. **Inline into the storage loop:** Because the storage scan is generic over the condition struct across `InMemory`, `Lsm`, and `Disk`, the JIT / Native AOT compiler inlines the predicate directly into the row loop—no virtual dispatch or delegate call per row.

## Fixed-Offset Byte Evaluation

When every column up to the last column referenced by the predicate has a fixed byte width, values sit at constant byte offsets in every row whose leading cells are non-null:

- **Single offset pass:** Byte offsets are computed once per scan; the loop tests leading null flags and reads cells straight from raw row spans without constructing a row view.
- **Automatic safe fallback:** Any row with a NULL in a leading column falls back to the general cell reader so results remain 100% identical.
- **Model layout tip:** Place fixed-width columns (`int`, `long`, `float`, `double`, `bool`, `Guid`) **before** variable-width `string` or `VECTOR` columns in your `[DataVoTable]` class to maximize fixed-offset kernel hits.

## Supported Compiled Aggregates

| Terminal Call | Compiled Kernel Support | Notes |
| --- | --- | --- |
| `Count()`, `Any()` | Yes (unindexed fixed-width predicates) | Zero model allocations. |
| `Sum(selector)` | Yes (single non-nullable `int`, `long`, `float`, `double` column) | Accumulates in registers; integer overflow throws `OverflowException`. |
| `Min(selector)`, `Max(selector)` | Yes (single `int`, `long`, `float`, `double`, `DataVoDate`, `DateOnly` column) | Skips `NULL`; `NaN` orders below all numbers. |

When target columns are also marked `[Columnar]`, the kernel upgrades automatically to SIMD columnar chunk evaluation—achieving **27.5–90.1 µs** over 200,000 rows with **0 B allocated** (see [Benchmark Results](/manual/performance/benchmarks)).

Inside an explicit transaction, queries stream through the MVCC snapshot path instead of reading raw storage in place, so transactional scans allocate.

## Non-Compilable Shapes & Diagnostics

Any lambda shape the generator cannot compile—such as capturing a local variable instead of passing a state argument, calling an arbitrary C# method, performing arithmetic inside the predicate, or comparing two columns—still executes correctly by streaming and filtering with the original lambda. The compiler emits diagnostic warnings (`DATAVOL001`–`DATAVOL005`) explaining why the call was not intercepted. Predicates on `[PrimaryKey]` or `[Indexed]` columns continue using their index fast paths.

## See Also
- [Compiled LINQ](/manual/client-interfaces/compiled-linq) — Full list of supported LINQ operators and `DATAVOL001`–`DATAVOL005` diagnostics
- [Roslyn Source Generators](/manual/client-interfaces/source-generators) — Setting up `[DataVoTable]`, `[DataVoQuery]`, and `<InterceptorsNamespaces>`
- [Zero-Allocation Row Model](/manual/internals/zero-allocation-rows) — Manual zero-GC table scans with `RowRef`
