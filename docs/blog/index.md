---
title: Engineering Blog
description: Notes on building DataVo-DBMS and DataVo-Sync, measurements, and postmortems.
outline: false
prev: false
next: false
---

# Engineering Blog

Notes on building **DataVo-DBMS** (an MIT-licensed embedded relational and vector database in C# for .NET 10) and **DataVo-Sync** (its commercial offline-first PostgreSQL sync add-on), with reproducible measurements and postmortems.

## Series: Building DataVo

1. **[Part 1: I wrote an embedded database and Postgres sync engine in C#. Here is where it wins and where it loses.](/blog/part-1-where-it-wins-and-where-it-loses)** *(21 November 2026)*
   Architecture overview, cross-platform benchmarks (macOS M4 Pro, Linux x64, Windows x64) against SQLite, DuckDB, and ElectricSQL `1.8.1`, five wrong-answer bugs caught by a differential SQL oracle, and open trade-offs.

### Planned

- **Part 2:** Packed byte buffers, Native AOT, and Roslyn codegen: how a 55 ns point lookup works with zero native binaries, prepared statements, and typed execution paths.
- **Part 3:** What an fsync costs, across operating systems: the 4 ms tie, the 0.1 ms Linux case, and the Windows scaling problem.
- **Part 4:** Z-sets, from incremental views to a 36 MB sync: wire protocol, offline conflict convergence, and reproducible Docker benchmarks against PostgreSQL 17 and ElectricSQL 1.8.1.
- **Part 5:** HNSW, the JIT, and the exact fallback: recall on clustered data, and why a graph search sometimes loses to a scan.
- **Part 6:** Differential oracle testing: how the matrix found five wrong answers before tuning anything.
- **Part 7:** Building an offline-first .NET 10 app end-to-end: combining Native AOT, filtered `VECTOR(n)` search, live Z-set views, and bidirectional Postgres sync in a real desktop/mobile architecture.
