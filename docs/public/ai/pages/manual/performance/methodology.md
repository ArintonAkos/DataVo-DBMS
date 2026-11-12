# Benchmark Methodology

> Source route: /manual/performance/methodology
> Source file: manual/performance/methodology.md

This page describes how the measurements on the [results page](./benchmarks.md) were taken, what each benchmark controls, and where the method limits what the numbers can show. For commands, see [Reproducing Benchmarks](./reproducing-benchmarks.md).

## Two kinds of benchmark

The results are split into two parts because they answer different questions and carry different statistical weight.

**Part I, `benchmarks/DataVo.Benchmarks`**, uses [BenchmarkDotNet](https://benchmarkdotnet.org). It runs one operation many times, discards warm-up iterations, reports the mean with a 99.9% confidence interval and standard deviation, and measures managed allocation per operation. All engines hold the same table, and every benchmark verifies that they return the same answer before timing. It answers: how fast is one well-defined operation on each engine?

**Part II, `demos/Research.Benchmark`**, runs a scenario once and reports total time, latency percentiles and total managed allocation. A scenario is a small program: ingest orders and read a dashboard after each one, run a mixed read/update load on 12 threads, load a million rows and reopen them. It answers: how do the engines behave on something shaped like an application? It has no repeated trials by default, so it carries no confidence interval. A difference of under about 20% between two engines in Part II should not be treated as established. Where a scenario was repeated, the page says so.

## What is held constant

- **Data.** Each benchmark generates its rows from a fixed seed, so every engine stores the same values.
- **Machine and build.** One machine, a Release build, one .NET runtime. The environment table on the results page records the chip, OS, runtime, and commit.
- **Result checking.** Part I compares every engine's answer to DataVo's before timing. A benchmark that returns a wrong answer fails and produces no numbers.
- **Indexes.** The polling engines in the streamed-view scenarios get explicit indexes on the join and filter columns (see the scenario's README in `demos/Research.Benchmark`).

## What is not held constant, and why

**Durability.** This is the largest source of misleading comparisons, so every result row names its durability.

| Label | What a write waits for | Survives power loss |
| --- | --- | --- |
| DataVo (LSM Production) | The log reaches stable storage (`LsmStrictFsync = true`). | Yes |
| SQLite (WAL, full) | The same, with `PRAGMA fullfsync=1`. | Yes |
| DataVo (LSM Relaxed) | The operating system accepts the write into its buffers. | No: recent writes can be lost |
| SQLite (WAL, normal) | The same. | No: recent writes can be lost |
| DataVo (LSM, default) in Part I | No flush per commit. | No |

Only rows with the same durability can be compared. A durable commit on the benchmark machine costs about 4 ms regardless of engine, because it is the time the drive needs to flush its cache. A result that shows a durable engine finishing 20,000 commits in half a second has not waited for the drive. The July revision of the results page contained exactly that comparison, and it was withdrawn. See [What changed since July](./benchmarks.md#what-changed-since-july).

**API.** Each engine is driven through the API a .NET program would use. DataVo uses compiled queries and typed rows. SQLite and DuckDB use ADO.NET with a statement prepared once. That cost is part of their numbers, as DataVo's API cost is part of DataVo's. DuckDB is also given a second configuration pinned to one thread, because DataVo and SQLite use one thread per query.

**Design fit.** Each engine is strongest on a different workload. DuckDB is a column store built for scans and aggregates; running point lookups against it measures a design mismatch and should be read that way. DataVo's streamed-view advantage depends on its incrementally maintained views, which apply only to query shapes it supports. Each benchmark's description states these conditions.

## Allocation figures

The "GC allocated" column is the managed memory the process allocated while the scenario ran, read from `GC.GetAllocatedBytesForCurrentThread` in the single-threaded scenarios and from `GC.GetTotalAllocatedBytes(precise: true)` in concurrent ops, thread scaling, YCSB, and space and recovery. It is a useful proxy for garbage-collector pressure. It has three limits.

- It counts the benchmark's own allocations. In the vector scenario most of the 61 MB is the input vectors copied during insert, not search overhead.
- It does not include native memory. DuckDB's appender and SQLite's page cache allocate outside the managed heap, so their managed figures understate their memory use.
- The two methods are not interchangeable. The multi-threaded scenarios also count allocations made by the engine's background threads, which the single-threaded ones do not.

## Percentiles

P50 and P99 are computed over per-operation timings taken with `Stopwatch.GetTimestamp` inside the measured loop. For scenarios that run only 100 operations, such as vector search, P99 is effectively the second-slowest sample. With under about 1,000 samples, treat P99 as an indication of the tail, not an estimate of it.

## Known limits of the method

- **Warm cache.** Reopen and recovery timings run in the same process right after the files were written. The operating system's page cache is warm. They show that opening a database does not read its data. They do not measure a cold boot, and they do not measure crash recovery.
- **Short runs.** Some Part II scenarios finish in around 100 ms, where JIT warm-up, thread start-up and garbage collections are a visible share. Scenarios that were run once cannot show run-to-run variance.
- **Background load.** The machine was not isolated. IDE language servers were running during the Part II runs.
- **No cross-machine claims.** The numbers describe one Apple M4 Pro under macOS. Other CPUs, storage devices, operating systems and runtimes will change both the absolute values and, for I/O-bound results, the ratios.
- **Platforms not measured.** No Linux x64, Windows, or Unity IL2CPP performance was measured for this revision. The Linux figures that remain in the repository date from 4 July 2026 and describe the v0.1 alpha engine.

## Reading an anomaly

When a result looks wrong, the results page states what was checked and what was not. A result that was not reproduced is reported as such, with the earlier figure, and is not silently dropped. An explanation marked as inferred is a hypothesis from timing and not a profiled fact. Where the page says "I did not profile", the claim stops there.
