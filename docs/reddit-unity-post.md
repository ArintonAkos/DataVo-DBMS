# Built a C# embedded DB with vector search and reactive queries, looking for Unity feedback

Hey r/unity,

I’ve been working on DataVo, an embedded relational database engine written in C#. It started as a university project in 2023, and I’ve been revisiting it as a real engine because I wanted something that fits .NET/Unity-style workloads better than a native provider boundary.

To be transparent: yes, I used AI tools, mostly Claude, for boilerplate, docs, test scaffolding, and iteration speed. The architecture, constraints, review, benchmarks, and maintenance are still mine.

The idea is not “replace SQLite.” SQLite is mature and battle-tested. DataVo is alpha software. But I wanted to explore what happens if the engine is built around C# hot paths, allocation-aware storage, vector search, and reactive query maintenance.

The two parts that might be interesting for Unity/game tooling are:

## Native vector search

DataVo supports `VECTOR(n)`, HNSW and flat vector index paths, plus SIMD distance kernels. The use case I had in mind is local embedding search for NPC memory, AI tools, semantic asset lookup, local prototypes, etc. One reason this interests me for Unity is avoiding native extension cross-compilation for every target platform.

## Incremental view maintenance / reactive queries

Instead of polling and recomputing the same query every frame, selected query results can be maintained incrementally as writes happen. For things like inventories, scoreboards, live economy state, debugging overlays, or tool windows, that model feels closer to what games often need.

For one benchmark, I used 10,000 baseline rows followed by 50,000 insert+read iterations.

On the complex VIP join + group aggregate workload, DataVo finished the full loop in **485.5 ms** on my macOS arm64 run. SQLite took **401.8 seconds** and DuckDB took **43.0 seconds** in the same benchmark harness.

Important caveat: this is not a like-for-like primitive comparison. DataVo is maintaining selected results incrementally, while SQLite/DuckDB are running the baseline query path. I still think it is a useful end-to-end comparison if the application requirement is “keep this derived result live after writes.”

I’m not trying to advertise it as production-ready. The project is alpha and not suitable for production save data yet.

My actual question for Unity developers is: would a C#-native embedded DB with vector search and reactive query results be useful in real Unity tooling/game workflows, or would you rather keep this kind of thing in SQLite plus custom C# systems?

I can share the repo and benchmark page if links are allowed.
