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

## 7. Testing lessons

- **A regression test must fail without the fix.** We check this by running the new test against the
  old code; the write-atomicity test saw 5, 3 and 7 commits before the fix and 1 after.
- **Pin the property, not the timing.** Timing assertions flake on busy CI machines; allocation counts,
  commit counts and row ids are deterministic.
- **Allocation tests need steady state.** .NET first runs methods unoptimized and replaces them in the
  background; measure until the optimized code is in place.
