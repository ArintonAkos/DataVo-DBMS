# DataVo Feature Research Bibliography

> Literature backing the proposed DataVo feature set across game dev, testing parity,
> HFT/low-latency, and cross-cutting concerns. Compiled 2026-06-19 via the
> `paper-search-mcp` (Google Scholar) and full-text reads of the two highest-impact papers.
>
> Each feature lists the **★ must-read** plus supporting work, and a one-line note on how it
> bears on DataVo's design.

---

## Game Dev

### 1. Time-travel queries (`AS OF TICK`)

Query a past database state at a specific game tick for replays, debugging, and anti-cheat.

- ★ Lomet et al. — *Transaction Time Support Inside a Database Engine* (ICDE 2006). ImmortalDB; native `AS OF` time-travel built into the engine — closest precedent. https://ieeexplore.ieee.org/abstract/document/1617403/
- Lomet, Hong, Nehme, Zhang — *Transaction Time Indexing with Version Compression* (VLDB 2008). Keeping historical versions cheap; addresses the per-tick storage-cost risk. https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/vldb08_ImmortalDB23.pdf
- Kaufmann et al. — *Benchmarking Bitemporal Database Systems* (EDBT 2014). Time-travel via snapshot recomputation; tick-snapshot tradeoffs. https://opus.bibliothek.uni-augsburg.de/opus4/files/64709/edbticdt2014industrial_submission_8.pdf
- Plattner, Wapf, Alonso — *Searching in Time* (SIGMOD 2006). https://dl.acm.org/doi/abs/10.1145/1142473.1142578

### 2. Frame-budget-aware execution (suspendable Volcano)

Yield mid-scan when execution exceeds a microsecond budget; resume next call.

- ★ Tang et al. — *Intermittent Query Processing* (PVLDB 2019). DISS: suspend/resume queries under a resource budget — the DB-research form of the frame-budget idea. https://vldb.org/pvldb/vol12/p1427-tang.pdf
- Amert et al. — *Timewall: Time Partitioning for Real-Time Multicore+Accelerator Platforms* (RTSS 2021). Budget enforcement from the real-time-systems side. https://ieeexplore.ieee.org/abstract/document/9622375/

### 3. Snapshot diff for netcode

Fast diff between two DB states producing a compact binary delta.

- ★ Orsten — *Dynamically Learning Efficient Server/Client Network Protocols for Networked Simulations* (MSc, 2011). Periodic server snapshots + delta encoding of game state. https://ualberta.scholaris.ca/bitstreams/ecbbad58-b559-4ea6-9ccd-5f7f78d55516/download
- Dantas & Baquero — *CRDT-based Game State Synchronization in P2P VR* (2025). Delta-state CRDTs; relevant if DataVo ever goes multi-writer. https://dl.acm.org/doi/abs/10.1145/3721473.3722144
- Moll et al. — *Inter-Server Game State Synchronization using Named Data Networking* (2019). Delta updates encoding only changes. https://dl.acm.org/doi/abs/10.1145/3357150.3357399

---

## Testing Parity

### 4. Copy-on-write database branches

Instant, isolated, low-cost forks (`db.Branch()`) for tests and alternate AI paths.

- ★ Zhu, Whittaker et al. — *Vive la Différence: Practical Diff Testing of Stateful Applications* (2025). DB branching on Postgres **plus** diff testing — ties branching and snapshot-diff together. https://mwhittaker.github.io/publications/diff_testing.pdf
- ★ *BranchBench: Aligning Database Branching with Agentic Demands* (2026). Fork latency, COW branching, Neon-style storage; benchmarks the instant-fork property. https://arxiv.org/abs/2604.17180
- Zhang et al. — *CloudyBench* (2025). Primary branch + COW child branches. https://ieeexplore.ieee.org/abstract/document/11112867/
- Xu et al. — *Toward Systems Foundations for Agentic Exploration* (2025). COW heaps, branching Postgres clones. https://arxiv.org/abs/2510.05556

### 5. Chaos mode (adversarial storage engine)

Storage mode that injects partial writes, disk-full, latency spikes into resilience tests.

- ★ Tran — *Chaos Engineering for Databases* (CWI MSc, 2020). Purpose-built fault injection for DBs incl. filesystem degradation — most on-point. https://homepages.cwi.nl/~boncz/msc/2020-LongTran.pdf
- Rosenthal & Jones — *Chaos Engineering: System Resiliency in Practice* (O'Reilly, 2020). Foundational principles. https://books.google.com/books?id=iVjbDwAAQBAJ

### 6. Schema-aware compiled query verification

Build-time source generator validating SQL strings against the entity schema.

- ★ Karakoidas et al. — *A Type-Safe Embedding of SQL into Java (J%)* (2015). Validates SQL syntax/semantics **against the DB schema at compile time** — exactly the source-generator goal. https://www.sciencedirect.com/science/article/pii/S1477842415000020
- Herlihy, Ailamaki, Odersky — *Static Typing Meets Adaptive Optimization* (SIGMOD 2025). Type-safe embedded queries; compile-time checking of raw SQL strings. https://dl.acm.org/doi/abs/10.1145/3735106.3736533
- Kučera — *Type-safe SQL Queries in Scala (Tyqu)* (2023). https://www.research-collection.ethz.ch/entities/publication/0ffae739-1dc4-46aa-a72e-5012fd40315e

---

## HFT / Low-Latency

### 7. Zero-allocation struct projection

Project query results directly into a pre-allocated `Span<T>`, bypassing heap/GC.

- ★ Behm et al. — *Photon: A Fast Query Engine for Lakehouse Systems* (SIGMOD 2022). Built native specifically because JVM **GC pressure** crippled latency — the precise argument for a no-GC path. https://dl.acm.org/doi/abs/10.1145/3514221.3526054
- Pedreira et al. — *Velox: Meta's Unified Execution Engine* (PVLDB 2022). Vectorized execution on small/single-record batches at low latency. https://horizon.documentation.ird.fr/exl-doc/pleins_textes/2024-03/010086183.pdf#page=111
- Li et al. — *Mainlining Databases: ... universal columnar data file formats* (2020). Zero-deserialization data access. https://arxiv.org/abs/2004.14471

### 8. Lock-free append-only tables

Internal atomic ring buffers for high-velocity event streams; no row locks/tombstones.

- ★ Thompson et al. — *Disruptor: High Performance Alternative to Bounded Queues* (LMAX, 2011). The canonical lock-free ring buffer, born in HFT. https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf
- Kumar — *Copy Ahead Segment Ring: An Ephemeral Memtable Design for Distributed LSM Tree* (2023). Lock-free ring buffer **as a DB memtable** — bridge from Disruptor to a table design. https://search.proquest.com/openview/1e367f3b604219e79c91bbc029e02ac3/1
- Wang et al. — *BBQ: A Block-based Bounded Queue for Exchanging Data and Profiling* (USENIX ATC 2022). Modern, higher throughput than circular/DPDK buffers. https://www.usenix.org/conference/atc22/presentation/wang-jiawei

### 9. Memory-mapped disk storage

Upgrade row `.dat` files to mmap, turning file seeks into pointer dereferences.

- ★ Crotty, Leis, Pavlo — *Are You Sure You Want to Use mmap in Your DBMS?* (CIDR 2022). The **cautionary** paper; argues mmap has serious correctness/perf pitfalls. Read before committing. https://www.pdl.cmu.edu/PDL-FTP/Database/p13-crotty.pdf
- ★ Leis et al. — *Virtual-Memory Assisted Buffer Management (vmcache)* (SIGMOD 2023). The modern answer that gets mmap-like ergonomics without the pitfalls — likely the design DataVo actually wants. https://dl.acm.org/doi/abs/10.1145/3588687
- Youssef et al. — *Optimizing Performance and Storage of Memory-Mapped Persistent Data Structures (Privateer)* (2022). https://ieeexplore.ieee.org/abstract/document/9926392/

---

## Cross-Cutting

### 10. Reactive queries (push deltas through the operator tree)

`db.Subscribe` pushing transaction-safe deltas through a WHERE operator tree, no polling.

- ★ Budiu, McSherry, Ryzhyk, Tannen — *DBSP: Automatic Incremental View Maintenance for Rich Query Languages* (VLDB 2023). Foundational theory; handles joins/aggregates/recursion cleanly. https://arxiv.org/abs/2203.16684
- Gjengset et al. — *Noria* (OSDI 2018). **Partial** incremental view maintenance for web backends; how to bound state/subscription cost.
- McSherry et al. — *Shared Arrangements: practical inter-query sharing for streaming dataflows* (VLDB 2020). Sharing state across many standing queries — addresses subscription explosion. https://arxiv.org/abs/1812.02639
- Sotolongo et al. — *Streaming Democratized ... Snowflake Dynamic Tables* (SIGMOD 2025). Delayed-view semantics across the latency spectrum. https://dl.acm.org/doi/abs/10.1145/3722212.3724455
- Xu & Erdweg — *Stateful Differential Operators for Incremental Computing* (2026). https://dl.acm.org/doi/abs/10.1145/3776728

---

## The unifying insight: one delta primitive, four features

Several "separate" features are the same primitive — a **delta/diff over database state** — viewed from different angles:

- **Snapshot diff** (netcode) = delta between two whole states
- **Reactive queries** = delta pushed continuously through operators
- **Time-travel** = the accumulated log of deltas, replayable
- **COW branching + diff testing** = forking state, then diffing results

DBSP's Z-set algebra is a unified theory of all four. If DataVo builds **one well-designed
delta/change primitive** in the engine, it becomes the foundation for reactive queries,
snapshot diff, time-travel, and branch-diff at once — rather than four bespoke subsystems.

---

## Deep reads (full-text)

### DBSP (Budiu et al., VLDB 2023) — read 2026-06-19

**Model.** A *stream* is a function `ℕ→A` (discrete time → values); whole DB snapshots are
streams of states. A *Z-set* is a function from elements to `ℤ` with finite support — sets/bags
generalized to allow **negative weights** (deletions). Addition is pointwise.

**Two operators generate everything:**
- Differentiation `𝒟(s) = s − z⁻¹(s)` (change between consecutive steps).
- Integration `ℐ(s)[t] = Σ_{i≤t} s[i]` (accumulate changes). `ℐ` and `𝒟` are mutual inverses.
- `z⁻¹` is delay; enables feedback/recursion.

**Incrementalization.** For any query `Q`, its incremental form is `Q^Δ = 𝒟 ∘ Q ∘ ℐ`.
By construction the incremental output **exactly equals** differentiating a full recompute — so
correctness is free. The **chain rule** `(Q₁∘Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ` means you incrementalize a
query *compositionally, operator by operator*.

**What this means for DataVo's volcano tree:** each operator gets an incremental variant.
- **Linear ops** (select, project, filter, union): `Q^Δ = Q` — **no state**, just run on the delta.
  This is the cheap, safe subset that maps directly onto the existing WHERE/projection path.
- **Bilinear ops** (joins): `(a×b)^Δ = a×b + z⁻¹(ℐ(a))×b + a×z⁻¹(ℐ(b))` — the classic incremental
  join, but it **requires materializing both integrated inputs** (memory cost ∝ relation size).
- **Non-linear** (`distinct`, aggregates, recursion): need extra machinery (stratification,
  nested streams); higher complexity.

**Design takeaway.** The earlier worry — that pushing deltas through joins/aggregates is
dangerous — is exactly right, and DBSP says *why*: those are the non-linear operators that need
materialized state. So **DataVo's V1 reactive queries should ship the linear subset only**
(single-table, `WHERE` with comparisons/AND/OR — no joins, no aggregates). That subset is
provably correct, stateless, and matches the "run the changed row through the predicate" intuition.
Joins/aggregates become a clearly-scoped V2 that opts into materialized operator state.

### "Are You Sure You Want to Use mmap?" (Crotty, Leis, Pavlo, CIDR 2022) — read 2026-06-19

**Thesis.** mmap looks simple but introduces performance and **correctness** problems that
outweigh its benefits for a DBMS in most scenarios.

**Concrete problems:**
- **Transactional safety / error handling:** accessing an unmapped/invalid region raises
  `SIGSEGV` unpredictably — hard to recover inside transaction logic.
- **I/O stalls:** page faults trigger *synchronous* disk I/O; the DBMS loses control of I/O
  timing/prioritization (fatal for latency SLAs — i.e. the HFT use case).
- **Page eviction:** the OS replacement policy is blind to DB semantics and can evict hot pages.
- **TLB shootdowns:** remapping pages forces cross-core TLB invalidations (expensive IPIs) on
  multicore — degrades exactly the multithreaded workloads DataVo targets.
- **Lost control:** no custom caching/prefetch/scheduling.

**Acceptable only for:** single-threaded apps, read-only workloads, small datasets.

**Design takeaway — this changes feature #9.** DataVo's row store is multithreaded, write-heavy,
and latency-sensitive — precisely where the paper says *don't* use mmap. The right move is **not**
"mmap the `.dat` files." It's the **vmcache** approach (Leis et al., SIGMOD 2023): a
DB-controlled buffer manager that uses virtual-memory tricks for mmap-like ergonomics **without**
ceding I/O control to the OS. Recommendation: reframe feature #9 from "memory-mapped row storage"
to "virtual-memory-assisted buffer pool (vmcache-style)," and keep mmap only for read-only/
single-threaded paths (e.g. a shipped, immutable game data file) where the paper says it's fine.

---

## Suggested reading order for implementers

1. **DBSP** — before any reactive-queries / delta work (it scopes all four delta features).
2. **mmap paper + vmcache** — before any storage-engine change (it may redirect feature #9).
3. **Intermittent Query Processing** — before frame-budget execution.
4. **Disruptor** — before lock-free append-only tables.
5. **Vive la Différence** — before branching/diff-testing (covers #3 and #4 together).
