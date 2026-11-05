# Engineering Notes

What building DataVo's durable, disk-resident storage taught us: the techniques, why they work, and the
bugs that slowed the engine down, each with the lesson it teaches. Numbers are from a 1M-row table on an
Apple Silicon laptop unless stated otherwise.

## 1. How to find a performance problem

1. **Measure first, with the real workload.** Every improvement below started from a measurement, and
   the obvious suspects (disk, B-trees, locks) were almost never the cause.
2. **Check how cost scales.** Time the operation at two or three sizes. Cost per item that stays flat
   means a constant overhead; cost that grows with the input means an algorithm problem (often hidden
   O(n^2)).
3. **Compare two variants that should behave the same way.** "A 1,000-row INSERT is slower per row than
   a 1-row INSERT" is impossible for a healthy system, and pointed straight at the bug.
4. **Profile, then read the code under the hottest frame.** A sampling profiler
   (`dotnet-trace --profile dotnet-sampled-thread-time`) shows where time goes; an allocation profile
   (`--profile gc-verbose`) shows what creates garbage.
5. **Know your profiler's blind spots.** Thread-time sampling charges garbage-collection pauses to
   whatever line allocated when the GC started; a "slow dictionary write" turned out to be GC work driven
   by all allocations together.
6. **Re-measure after every change, and keep the losers honest.** One "fix" (presizing dictionaries)
   moved the needle by 3%; we kept it because it was free, but called it what it was.

## 2. Bugs that made the engine slow (and what each teaches)

| Symptom | Cause | Lesson |
| --- | --- | --- |
| A one-row SELECT on 10,000 rows took 275 ms | SELECT read-locked every row of the table, and the planner loaded the whole table to estimate its size | Locks and estimates must be proportional to the result, not the table |
| "Indexed" SQL queries scanned the whole table | Lookups used the text `101`, index writers stored `[101]`; a fallback scan hid the miss | A silent fallback turns a correctness bug into a performance bug; test the fast path, not just the answer |
| Index keys differed per thread | Floats were formatted with the thread's culture: `1.25` became `[1,25]` under Hungarian settings | Anything persisted or compared must be culture-invariant |
| Bulk SQL INSERT: 80 us per row | The lexer computed each token's line/column by scanning from the start of the text: O(tokens x length) | Quadratic cost hides in innocent helpers; ask "how often is this called?" |
| 21% of a point read spent logging | The library wrote every log line to the console | An embedded library must be silent by default |
| Typed inserts 18x slower after indexes were persisted | Only SQL opened a write batch, so every index entry became its own commit | When you change what a write costs, recheck every write path, not just the one you tested |
| Compiled UPDATE lost its fast path | The fast path checked for a heap index structure that no longer existed in LSM mode | Feature gates encode assumptions; when the assumption changes, the gate silently turns off |
| Every open rebuilt the vector index: 1.3 s for 20k vectors | The HNSW graph lived only in memory; LSM mode never saved it | Derived state that is expensive to rebuild needs its own snapshot, tied to the data by a watermark |

## 3. LSM trees in one page

An LSM (log-structured merge) tree never updates data in place:

1. A write is appended to a **commit log** (and forced to disk) for durability.
2. It is inserted into a sorted in-memory table, the **MemTable**.
3. When the MemTable is full it is written out as an immutable sorted file, an **SSTable**.
4. Background **compaction** merges SSTables, keeping only the newest version of each key and dropping
   deletion markers (tombstones) that nothing older can hide behind.

Reads check the newest data first: the MemTable, then the SSTables from newest to oldest. Every version
carries a **sequence number**, so "newest" is never ambiguous, even when files overlap.

Why this design: writes are sequential (fast on any disk), files are immutable (easy to cache, map,
checksum, and recover), and a crash can only lose what was not yet in the commit log.

## 4. Memory-mapped files

`mmap` makes a file's pages directly addressable memory. The OS page cache *is* the cache:

- A **read()/pread()** costs a user-to-kernel transition (about 0.1-0.3 us) and a copy into your buffer,
  and needs your own cache on top of the OS one.
- A **mapped read** of a cached page is a plain memory access; an uncached page costs one page fault.

Measured: a random point lookup took 218 ns mapped vs 1.27 us with pread.

It is safe here because SSTables are immutable and never truncated while mapped (the classic mmap
dangers are files changing or shrinking underneath you). The costs: Windows cannot delete a mapped file
(so we unmap before deleting), 32-bit processes run out of address space, and browsers have no mmap
(so there is a pread fallback).

## 5. Designing for the hardware

- **Page-aligned blocks.** Data blocks start at multiples of the OS page size (4 KiB on x64, 16 KiB on
  Apple Silicon). Reading a block then costs at most one page fault and never drags in a neighbour.
- **Fixed-size entries for binary search.** Each block holds a dense array of 24-byte entries (row id,
  sequence, value offset/length), so a search compares native integers inside a few cache lines.
- **Cache-line-blocked Bloom filter.** A Bloom filter answers "definitely not here" for a key. A classic
  one spreads a key's bits over the whole array (several cache misses); the blocked variant puts all of a
  key's bits in one 64-byte block: one cache miss per lookup, for a slightly higher false-positive rate.
- **Lazy checksums.** Every block has a CRC32C (hardware-accelerated); it is verified the first time a
  block is read, so corruption is caught without paying for it on every access.

## 6. Durability and atomicity

- **One commit frame per write unit.** A statement's rows and index entries go into a single checksummed
  log record. Recovery applies a record completely or not at all, so a crash can never keep a row without
  its index entries.
- **Visibility after durability.** Other sessions see a change only after its record is on disk, so they
  never observe something a crash could undo.
- **Ordering across flushes.** A flush publishes the new file before evicting the in-memory copy, and
  readers check memory before the file list. A reader can therefore never miss a row that is "between"
  memory and disk.

## 7. Derived state: snapshots and watermarks

The rows are the truth; an index is *derived* from them. A cheap index (a B-tree) can live in the LSM
tree itself. An HNSW graph is costly to build (a nearest-neighbour search per inserted vector), so we
save it as a separate snapshot file, and need a rule for when that snapshot can be trusted.

- **Watermark.** The snapshot records the commit LSN it reflects, W. Every row version carries the
  LSN that wrote it. On open, a row whose version is <= W is exactly what the snapshot saw (indexed if
  it had a vector, absent if not); only rows with a version > W are re-read and re-indexed. Rows the
  index has but the table lacks were deleted after W and are removed. This is the same idea as a
  database checkpoint plus log replay, applied to an index.
- **Never run ahead of the data.** The snapshot is written after the storage engine's final flush. If
  it were written first and the machine lost power, it could contain rows the table never kept.
- **Numbers must never be reused.** The rule "version <= W means unchanged" only holds if every later
  write gets an LSN above W. After a restart, LSNs resumed from the highest LSN any table had flushed,
  which can be below W (when the last commit was a DROP TABLE marker, for example). The fix: open raises
  the LSN floor to every snapshot's W. Any number that a file trusts must also bound future numbers.
- **A damaged snapshot only costs time.** It has a checksum; any mismatch means "rebuild from the rows".
  Losing a cache must never lose data.

A bug worth remembering: the snapshot writer computed the final checksum *before* flushing its last
buffered bytes, so the checksum never matched. A round-trip test (write, read back, compare) caught it
at once. Every serializer deserves one.

## 8. Randomized testing: finding the bugs nobody thought to test

1,700 hand-written tests passed while the engine could lose rows after a crash and SQL deleted rows it
should have kept. Two kinds of randomized test found these bugs within minutes:

- **Model-based testing.** Run random operations against the real engine and against something
  trivially correct (a dictionary), and compare after every step. Crashes and reopens are just more
  random operations. It found that a MemTable could rotate in the middle of a commit frame: recovery
  skips a frame by its LSN, so a file holding half a frame made the other half disappear after a crash.
- **Differential testing.** Run the same random SQL on DataVo and on SQLite and compare. SQLite is
  heavily tested, so a difference usually means a DataVo bug. It found: `B <> 8` true for NULL (so a
  DELETE removed NULL rows), NULL joining NULL, `COUNT(*)` crashing on an empty table, `SUM` of nothing
  returning 0, ignored column aliases, columns in the wrong order, and an index lost after a reopen.

What makes them work:

- **Seeds make random runs reproducible.** A failure reports its seed and the last operations; the same
  seed replays the same run. Use fresh seeds in nightly runs, fixed ones in regular CI.
- **More runs find rarer bugs.** The split-frame bug needed 40 seeds x 1,500 steps; the 6 seeds of the
  regular run never hit it. That is why a nightly job runs many new seeds.
- **Separate dialect from bugs.** Avoid generating what SQL dialects legitimately disagree on (integer
  division, NULL ordering, case-insensitive LIKE), so every remaining difference deserves attention.
- **Compare everything observable.** Comparing only values missed wrong column names and order.

## 9. Fair benchmarks

A benchmark answers "which is faster for the same work?" only if both sides do the same work. Ours
committed DataVo's 50,000 updates as one batch (one fsync) but SQLite's one by one (50,000 fsyncs), and
reported DataVo 580x faster. Done the same way, they tie when every update is power-safe (both wait
~4 ms for the disk), and SQLite is 1.4-2.6x faster otherwise. Checks before trusting a comparison:

- Same durability (fsync per commit, and `fullfsync` on macOS), same batching, same data.
- Know what a latency measures: a call that only queues work has near-zero latency and proves nothing.
- Be most suspicious of results that look too good.

## 10. Testing lessons

- **A regression test must fail without the fix.** We check this by running the new test against the
  old code; the write-atomicity test saw 5, 3 and 7 commits before the fix and 1 after.
- **Pin the property, not the timing.** Timing assertions flake on busy CI machines; allocation counts,
  commit counts and row ids are deterministic.
- **Allocation tests need steady state.** .NET first runs methods unoptimized and replaces them in the
  background; measure until the optimized code is in place.
