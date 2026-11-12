# Benchmarks

The benchmark results, the methodology and the reproduction commands live in three pages of the manual. This file used to duplicate the results page and had drifted out of date with it, so it is now only a pointer.

- [Benchmark Results](./manual/performance/benchmarks.md): measurements from 12 November 2026 for DataVo 0.2.0-rc.3 against SQLite, DuckDB and LiteDB. Part I is the BenchmarkDotNet suite. Part II is the systems workloads: incrementally maintained views, durability, YCSB, vector search and a million-row SQL workload. Part III covers platforms and Unity IL2CPP.
- [Benchmark Methodology](./manual/performance/methodology.md): what each benchmark controls, what it does not, and how to read durability labels, allocation figures and percentiles.
- [Reproducing Benchmarks](./manual/performance/reproducing-benchmarks.md): every command, the `sqlite-vec` setup, and how the charts are regenerated.

The raw output behind every table is in `docs/public/benchmarks/2026-11-12/`.
