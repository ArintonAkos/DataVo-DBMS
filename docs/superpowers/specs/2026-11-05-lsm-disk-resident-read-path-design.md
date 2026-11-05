# LSM Disk-Resident Read Path

> **Date:** 2026-11-05
> **Status:** Direction approved (disk-resident, memory-mapped). Implementation in phases.
> **Scope:** `LsmStorageEngine` row storage and read path, SSTable format, open/recovery, compaction I/O,
> and (Phase 2) persisted secondary indexes.

## 1. Problem

The LSM engine is RAM-resident. Every live row is a separate `byte[]` on the managed heap
(`LsmLatestRowStore`), and SSTables are written for durability but only read at startup. Measured on
1,000,000 rows (61 MB on disk, Apple Silicon, warm OS cache):

| Metric | Value |
| --- | --- |
| Open (constructor) | 0.8 s |
| Allocated during open | 1,045 MB (17x the file size) |
| Working set after open | 665 MB |
| Gen2 GCs during open | 9 |
| Open time split | 65% index rebuild (decode every row), 26% SSTable load |

Open is O(data), memory is about 10x the data size, and the dataset must fit in RAM. SSTable v1
stores the whole file as one data block, so it cannot serve point reads anyway (a lookup is a
linear scan of the file); the loader copies every entry twice (`ToArray` per key and value) and
validates key order in a full extra pass.

## 2. Goals and non-goals

Goals:

1. **Open in O(metadata):** map files, read footers and block indexes; never read every row.
2. **Memory follows the working set:** cold rows live in the OS page cache, not in the GC heap.
   Datasets may exceed RAM.
3. **Point reads stay fast:** p50 well under 1 us in the storage layer for a warm block, one page
   fault at most for a cold block.
4. **Zero-copy reads** from mapped memory where the caller deserializes immediately.
5. **Same durability and crash-atomicity** as today (commit log, visibility after durability).
6. **Portable:** a non-mmap reader for targets without memory mapping (browser/WASM, restricted
   Unity targets), selected automatically or by configuration.

Non-goals (this design): changing the commit log, MVCC, or the SQL layer; range-partitioned
multi-file levels beyond what compaction needs; compression (blocks keep room for it via a flag).

## 3. Background: why memory mapping

A `read()`/`pread()` per block costs a user-to-kernel transition (~0.1-0.3 us) plus a copy from the
page cache into a user buffer, and needs a user-space block cache that duplicates the page cache.
A read-only mapping of an immutable SSTable makes the OS page cache directly addressable: a warm
block is a plain memory access, a cold block costs one page fault (minor fault ~0.5-1 us when the
page is cached, major fault = device read). SSTables are immutable and never truncated while open,
which removes the classic mmap hazards (SIGBUS on truncation, torn writes through the mapping).

Costs we design around:

- **Windows:** a mapped file cannot be deleted. Mappings are owned by the ref-counted
  `LsmSstableHandle`; compaction deletes a file only after its last reference unmaps it.
- **Address space:** files are mapped individually and lazily; 32-bit processes use the
  non-mmap reader.
- **Unpredictable latency on major faults:** acceptable for an embedded engine; readahead hints
  (`madvise`) are a later tuning step.
- **No mmap in browsers:** the `RandomAccess`/`FileStream` reader with a small block cache.

## 4. SSTable v2 format

All integers little-endian. Row user keys are fixed 8 bytes (row id), so the format specializes on
fixed-size keys instead of LevelDB-style prefix compression.

```
file  := data block* | block index | bloom filter | properties | footer
```

### 4.1 Data block

Blocks are **page-aligned**: each block starts at a multiple of the block alignment, so a block is
one page fault (or a contiguous run of pages when a row is larger than a page) and never straddles
a page boundary unnecessarily. Alignment = `max(4096, Environment.SystemPageSize)`, recorded in the
footer so a file written on a 4 KiB system reads correctly on a 16 KiB system. The target block size
is the alignment (4 KiB on x64, 16 KiB on Apple Silicon).

```
block := header | entry[count] | value bytes | padding to alignment
header := u32 count | u32 flags | u32 crc32c(entry array + value bytes)
entry := u64 row id | u64 tag (sequence << 8 | type) | u32 value offset | u32 value length   (24 bytes)
```

- The entry array is dense and fixed-size, so an in-block lookup is a binary search over 24-byte
  records: a 16 KiB block of ~150-byte rows holds ~100 entries, the search touches ~7 records and
  stays within a few cache lines. Row ids compare as native `long`, no `memcmp`.
- Entries are sorted by (row id ascending, sequence descending), so the first entry of a row id is
  its newest version. A version newer than the reader's snapshot is skipped forward.
- A row larger than the target block size gets a block of its own (rounded up to the alignment).
- The CRC covers the block payload and is verified lazily on first access per block (one bit per
  block in a per-file bitset), so verification costs O(1) amortized and corruption is still caught
  before a value is returned.

### 4.2 Block index

```
index := u32 block count | u32 reserved | (i64 first row id | i64 last row id | i64 offset | i32 length | i32 entry count)[block count]
```

32 bytes per block; for 1M rows in 16 KiB blocks (~7,000 blocks) that is ~220 KB, which fits in
L2. The index is read in place from the mapping (`MemoryMarshal.Cast`), with no parsing at open.

### 4.3 Bloom filter (blocked)

A cache-line-blocked Bloom filter: the key hash selects one 64-byte block, and all k probe bits lie
inside it, so a negative lookup costs one cache miss instead of k. 10 bits/key, k = 7 (~1% false
positive rate for blocked filters at this size). Hash: 64-bit mix of the row id (no byte hashing,
the key is an integer).

### 4.4 Properties and footer

Properties: smallest/largest row id, smallest/largest sequence, entry count, live put count,
tombstone count, next-row-id watermark (replaces the special metadata key), format flags.

```
footer := index handle | bloom handle | properties handle | u32 block alignment | u16 version (2) | u32 magic
```

v1 files (single block) are still readable; on open they are rewritten once to v2 (an atomic file
replacement through the existing durable-file helper plus a manifest edit), so the read path only
ever serves v2.

## 5. Read path

### 5.1 Layers, newest first

1. **Staged writes** of the current flow (unchanged).
2. **Unflushed overlay:** the existing lock-free `LsmLatestRowStore`, restricted to rows whose newest
   version is still in a MemTable. Each slot also records its sequence; after a flush is published,
   slots whose sequence is covered by the flush are evicted.
3. **SSTables:** level 0 newest-first (files may overlap), then each deeper level (non-overlapping,
   binary search by row-id range). Per file: range check, blocked Bloom probe, block-index binary
   search, in-block binary search.

The first layer that holds the row id decides: a value, or a tombstone (deleted).

### 5.2 Versions and concurrent flush/compaction

The set of live files is an immutable **version** object (array of handles per level), published by
reference. A reader takes a reference (ref-counted handles, as today) for the duration of one
lookup or scan. Ordering that keeps reads correct across a flush:

1. the flush writes the file and the manifest edit,
2. publishes the new version,
3. then evicts the flushed overlay slots.

A reader checks the overlay first, then takes the current version. If it missed in the overlay
because the slot was evicted, the eviction happened after step 2, so the version it takes includes
the new file.

### 5.3 Zero-copy reads

- `ReadRow` keeps returning a `byte[]` copy (public contract).
- New internal visitor API `TryReadRow(rowId, ReadOnlySpanAction)` and scan visitors pass spans that
  point into the mapping while the version reference is held; typed deserialization
  (`RowSerializer.DecodeProjectedCells`, `ByteSpanReader`) runs directly on mapped memory.

### 5.4 Scans

A k-way merge over the overlay and per-file block iterators (min-heap keyed by row id, newest
sequence wins, tombstones suppress). Blocks are visited sequentially, so scans are a linear walk
through mapped pages (OS readahead applies).

### 5.5 Row counts and next row id

The live row count and the next row id are recovered from the newest file properties plus the
commit-log tail; the in-memory counter (`LiveCount`) stays exact after open.

## 6. Writes, flush, compaction

- Commit path unchanged (commit log, MemTables, deferred publication).
- Flush writes v2 through a streaming writer: blocks are filled from the MemTable iterator, padded
  to the alignment, and written sequentially; no whole-file image in memory.
- Compaction merges mapped input files through block iterators and writes outputs split at a target
  file size (64 MB), instead of loading every entry of both levels and writing one file.

## 7. Open and recovery

1. Read the manifest, map each live file, validate footer, block index bounds, and properties.
2. Rewrite v1 files to v2 (one time).
3. Replay the commit-log tail into MemTables and the overlay (unchanged logic).

No row is decoded. Index rebuild (65% of today's open) is removed in Phase 2.

## 8. Reader backends

`ISsTableBlockSource`:

- `MappedBlockSource`: `MemoryMappedFile` + view accessor, base pointer acquired once per mapping;
  spans over regions (files are < 2 GB by the 64 MB file-size target, so `int` lengths suffice).
- `RandomAccessBlockSource`: positional reads (`RandomAccess.Read` on .NET 6+, locked `FileStream`
  on netstandard2.1) into pooled buffers, with a small CLOCK block cache. Used where mapping is
  unavailable (browser) or disabled by configuration, and for 32-bit processes.

Selection: `DataVoConfig.LsmReadMode` = `Auto` (default: mmap where supported and 64-bit),
`MemoryMapped`, `PositionalRead`.

## 9. Phases

1. **Disk-resident rows:** v2 format (writer, reader, blocked Bloom), mapped and positional block
   sources, version objects, overlay eviction, point reads and scans from SSTables, streaming flush,
   v1 upgrade on open. Index rebuild still runs at open.
2. **Persisted indexes:** index entries stored as their own sorted runs (key bytes + row id) written
   with the same commit frames, so open no longer rebuilds indexes and index memory follows the
   working set too.
3. **Tuning:** streaming compaction with split outputs, readahead hints (`madvise`
   `MADV_RANDOM`/`MADV_WILLNEED`, Windows `PrefetchVirtualMemory`), zero-copy typed reads,
   measurements on x64 (4 KiB pages) and Apple Silicon (16 KiB pages), cold-cache benchmarks.

## 10. Verification

- Format: round trip, page alignment, CRC detects corruption, v1 to v2 upgrade, rows larger than a
  block, tombstones, snapshot sequence filtering.
- Engine: reads across flush, compaction, reopen; concurrent readers during flush/compaction/eviction;
  crash harness unchanged and green; Windows deletion of mapped files (CI).
- Both block sources run the same test set.
- Benchmarks: open time and allocation, working set, point-read latency (typed and SQL), scan
  throughput, at 1M and 10M rows.
