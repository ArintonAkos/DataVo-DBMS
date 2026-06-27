# LSM Storage Engine — Design Spec

**Date:** 2026-06-29
**Status:** Approved (foundation + all sections), ready for implementation planning
**Branch context:** continues the Maximalist disk initiative after the zero-alloc compiled UPDATE (`b3b42d3`)
**Predecessors:** Phase 3 group-commit WAL, Phase 4 binary recovery + checkpointer (`c06062b`), zero-alloc compiled UPDATE (`b3b42d3`)

---

## 1. Motivation

The zero-alloc compiled UPDATE (Step 1 of the Maximalist route) cut the disk UPDATE hot path from ~24.2 KB/op to ~4.9 KB/op (−79.7%) by eliminating `Dictionary<string,object>` materialization. The residual ~4.9 KB/op and the flat single-writer wall time are **structural artifacts of the B-tree-on-disk model**:

- **In-place tombstone write** on every update/delete (`DiskStorageEngine.DeleteRow` negates the length prefix — a random write).
- **Synchronous B-tree index churn** (`JsonBTreeIndex.DeleteValues` rebuilds the whole tree per delete).
- **Out-of-place update** = tombstone old + append new + delete/insert index entries.
- A global per-file lock (`GlobalFileLocks`) serializes all reads and writes to a table file.

These cannot be optimized away inside the B-tree model; they are the model. This spec replaces the disk storage model with a **full, textbook RocksDB-style Log-Structured Merge (LSM) engine**: writes append to an in-memory MemTable protected by the existing binary WAL, MemTables flush to immutable sorted SSTable segments, and a background compactor merges segments and reclaims space. The result is an append-only hot path with no in-place mutation and no synchronous index deletes.

### Goals
- **G1.** Update and Delete become pure appends (WAL frame + MemTable write); no `.dat` mutation, no synchronous B-tree delete.
- **G2.** Steady-state writes approach zero GC allocation via an arena-backed MemTable (plus the already-pooled WAL).
- **G3.** Reads efficiently resolve the newest version across MemTable(s) and SSTables, using Bloom filters to bypass disk I/O on point lookups.
- **G4.** Crash-consistent recovery with bounded WAL via checkpointing — preserve the Phase-4 ACID guarantees exactly.
- **G5.** Ship side-by-side with the existing `DiskStorageEngine` for seamless A/B benchmarking and as a safety fallback.

### Non-goals (this spec)
- Replacing the existing `DiskStorageEngine` (it stays; LSM is selected by config).
- Distributed/replicated storage, column families beyond per-table + per-secondary-index trees, or block compression (LZ4/Zstd) — these are future extensions; v1 keys, blocks, and Bloom are uncompressed.
- Changing the in-memory `StorageMode.Memory` backend.

---

## 2. Keystone: the identity shift

Today the stable row identity **is the physical byte offset** (`RowId = offset into .dat`). An LSM moves a row's physical location on every flush and compaction, so identity must become logical.

| Concern | B-tree model (today) | LSM model |
|---|---|---|
| **Logical identity** | byte offset (`RowId`) | **`InternalKey = encode(PK) ‖ seqno ‖ valueType`** |
| **Physical location** | the offset itself | `(sstableId, blockOffset)` or MemTable node — resolved by lookup, never exposed |
| **Version ordering** | one live row per PK (tombstones enforce) | **highest seqno wins**; older versions coexist until compaction |
| **Secondary indexes** | `value → offset` | **`value → PK`** (then point-read through the LSM by PK) |
| **MVCC `RowVersion`** | separate version map | **becomes the LSM sequence number** — one unified version clock |

**Sequence numbers (`seqno`)** unify three mechanisms DataVo currently keeps separate: newest-version-wins ordering, MVCC snapshot isolation (a snapshot = "read at `seqno ≤ S`"), and compaction's garbage rule ("drop versions below the oldest live snapshot's seqno"). **The seqno source is the Phase-4 WAL `Lsn`** — one monotonic clock for WAL position and version ordering.

**`InternalKey` encoding (order-preserving).** `InternalKey = userKey ‖ tag`, where:
- `userKey` = PK encoded big-endian / sign-flipped for signed integers (extend `IndexKeyEncoder` to be byte-comparable for the LSM comparator).
- `tag` = a single packed 8-byte trailer `(seqno << 8) | valueType` (RocksDB convention; `valueType` ∈ {`Put`, `Delete`}), written big-endian.

The comparator sorts by `userKey` **ascending**, then by `tag` **descending** — so for a given user key the highest seqno (newest version) sorts first, and the type is decided by the same single comparison. On disk and in the MemTable the authority for the type is this packed tag; the MemTable node also caches it for convenience. A tombstone is a `Delete`-tagged entry with an empty value.

---

## 3. Topology & delivery

Each **table is an independent LSM tree** (it already owns its own `.dat`):

```
   writes ─► WAL (Phase 4, shared) ─► Active MemTable (mutable, sorted, arena-backed)
                                          │ full → freeze (atomic swap)
                                          ▼
                                  Immutable MemTable(s) ─► flush thread
                                          │
                                          ▼
   L0: [sst][sst][sst]   (overlapping — one file per flush; tiered)
   L1: [───sst───][───sst───]   (non-overlapping, sorted by key range; leveled)
   L2: [──────sst──────][──────sst──────] ...
        + per-table MANIFEST (atomic source of truth: live SSTables, level assignment, seqno watermark)
```

**Delivery (approved):** a new `LsmStorageEngine : IStorageEngine`, selected by config, **coexisting** with `DiskStorageEngine`. `IStorageEngine` is the existing seam, so the catalog/transaction/query layers above change minimally; the LSM is selected by a new `StorageMode.Lsm` (or equivalent config switch) and A/B'd in the benchmark harness exactly like the `--legacy-update` toggle. This is the *complete* textbook engine — coexistence concerns the integration seam, not reduced scope.

Each secondary index is also an LSM tree (see §10), keyed by `(indexedValue ‖ PK)`.

---

## 4. The MemTable (ask #1)

**Mental model:** an LSM UPDATE is **not** an in-place byte-patch — it is an **append of a new full row image at a higher seqno**; the old version is shadowed. The Step-1 work is *repurposed, not reused verbatim*: `RowSerializer`'s fixed-width writers still build the new row image cheaply, and the Step-1 `WalUpdateFramePayload` becomes the Update redo record — but in-place patching is gone. Zero-alloc on the hot path comes from the **arena**, not from patching.

**Structure:** a sorted **skiplist** keyed by `InternalKey`, value = `{ valueType: Put|Delete, rowImage }`. Sorted (not the hash `ConcurrentDictionary` fast lane) so it flushes to a sorted SSTable with no sort step and supports merge iteration.

**Concurrency — single-writer / multi-reader.** Phase-4 group commit already funnels every write through one commit pipeline, so the **commit thread is the sole mutator**. Readers traverse via volatile forward-pointers (lock-free reads; no lock-free writes required). This sidesteps a full lock-free skiplist while staying correct. (Confirmed: a true lock-free concurrent skiplist is deferred as over-engineering given the single-writer commit funnel.)

**Allocation:** skiplist nodes + key bytes + value bytes bump-allocate into **pooled arena slabs**; on flush the entire arena returns to a pool. Steady-state MemTable churn → ~0 GC. Size is tracked as bytes-resident-in-arena; crossing a threshold (default 64 MB, configurable) triggers freeze.

**Write absorption:** Insert/Update/Delete all become MemTable writes — Insert/Update → `Put(InternalKey, rowImage)`, Delete → `Put(InternalKey, tombstone)` (a Delete entry with empty value).

---

## 5. WAL integration (ask #2)

The Phase-4 binary WAL **is** the MemTable's redo log; the MemTable is volatile, the WAL makes it durable:

```
write ─► serialize op ─► WAL frame {Insert|Update|Delete, seqno=Lsn}
       ─► group-commit fsync (durable ack) ─► apply to active MemTable @ seqno
```

- **Seqno = WAL `Lsn`** (monotonic): one clock for WAL position, newest-wins, and MVCC.
- **Full frame catalog now emitted.** Step 1 added Update; the LSM now also emits **Insert and Delete** binary frames. Inserts no longer write straight to `.dat`; deletes no longer tombstone in place. The Insert/Delete payload codecs mirror `WalUpdateFramePayload` (binary, span-based, full new row for Insert; PK only for Delete).
- **Lifecycle = Phase-4 checkpoint-LSN, repurposed.** Flushing a MemTable advances that table's **flushed-LSN** to the max seqno it contained. The checkpointer prunes WAL frames below the **global prune watermark = min(flushed-LSN) across all tables** (a frame cannot be dropped while any unflushed table still needs it). "Flush MemTable" replaces Phase-4's "apply WAL to .dat"; "persist watermark + prune WAL" is unchanged.

---

## 6. Flush & SSTable format (ask #3)

**Freeze (non-blocking):** at the size threshold, atomically swap the active MemTable for a fresh one (single reference exchange). The old becomes **immutable** and is queued for flush; new writes hit the new MemTable and never block. The flush thread walks the immutable MemTable in sorted order (already sorted → sequential write, no sort).

**SSTable file layout** (write-once, fsync'd, then committed via the manifest):

```
┌────────────── Data Blocks (sorted InternalKey → value, ~16 KB target each) ─────────┐
│  entry: [keyLen][InternalKey = userKey ‖ tag(seqno«8|type)][valLen][rowImage]  (rep) │
├────────────── Filter Block (custom alloc-free Bloom over user PKs) ──────────────────┤
├────────────── Index Block (sparse: last InternalKey of each data block → off+len) ───┤
└────────────── Footer (fixed 48 B: indexBlockHandle, filterBlockHandle, magic, ver) ──┘
```

- **Footer** is fixed-size at the file end → read it first to locate the index and filter blocks.
- **Index block** is binary-searchable (sorted last-keys) to find the one data block that may contain a key.
- **Custom allocation-free Bloom filter (ask #3, specific):**
  - One `byte[]` bit array, `m = n × bitsPerKey` (default `bitsPerKey = 10` → ~1% false-positive rate).
  - `k` probe positions synthesized from a **single** 64-bit hash via double-hashing `g_i = h1 + i·h2` (Kirsch–Mitzenmacher) — **one** hash computed per key, **zero** per-probe allocation.
  - Hash = FNV-1a / xxHash64 over the **user PK** `ReadOnlySpan<byte>` (not the InternalKey — we test PK membership).
  - Built inline during flush (we already visit every key); at read time it is a resident `byte[]` probed with no allocation.
- **Atomic install:** write the `.sst` + fsync → append a `VersionEdit` to the manifest (add file → L0) + fsync the manifest. The manifest edit is the commit point: crash **before** it ⇒ the `.sst` is an orphan and is ignored/GC'd; crash **after** ⇒ the file is live. Same ordering discipline as Phase 4 (data durable before the watermark advances).

---

## 7. Read path (ask #4)

**Point lookup `get(PK, snapshotSeqno)`** — first match wins, short-circuit:

```
active MemTable ─► immutable MemTables (newest→oldest)
   └─► L0 ssts (newest→oldest, overlapping) ─► L1..Ln (non-overlapping: binary-search to the ONE candidate file per level)
        each SSTable gated by its Bloom filter ↑↑
```

For each source, find the highest `seqno ≤ snapshotSeqno` for that PK; a **tombstone short-circuits to "not found."** **Bloom filters are the disk-bypass lever:** before reading any data block of an SSTable, probe its Bloom — "definitely absent" ⇒ skip the file with zero data-block I/O. Footer/index/Bloom blocks stay resident per open file (via the existing `FileHandlePool` for handles); a small LRU **block cache** covers hot data blocks.

**Range / full scan (no PK predicate):** a **k-way merge iterator** over the MemTable(s) + all SSTables, ordered by `InternalKey`, emitting the newest non-tombstone per user key. This replaces `DiskStorageEngine.ReadAllRows`. (Bloom helps only point reads.)

**MVCC for free:** a snapshot *is* a seqno — read at `seqno ≤ S`. The existing `RowVersion` / `RegisterUpdateVersion` machinery collapses into this single clock.

---

## 8. Compaction (ask #5)

**Strategy (approved): Hybrid = L0 tiered + L1…Ln leveled (the RocksDB default).**

- **L0 (tiered):** flushed files overlap in key range; when the L0 file count ≥ threshold (default 4), merge all of L0 + the overlapping L1 files down into L1. Absorbs flush bursts without write-stalls.
- **L1…Ln (leveled):** each level is non-overlapping and sorted; each level targets ~10× the previous. When a level exceeds its target size, pick a file (round-robin / most-overlap) and merge it into the next level's overlapping files.
- **Why hybrid:** leveled keeps ≤1 file per level below L0, so point reads touch few files and space amplification stays low — both fit DataVo's read-perf identity; L0-tiered avoids write-stalls on flush bursts. The cost is higher write amplification than pure size-tiered, judged the right trade here. Pure size-tiered remains the lower-write-amp alternative for write-saturated workloads (same merge machinery, different trigger).
- **Mechanics:** k-way merge of immutable inputs → keep the newest version per user key; **drop** versions below the oldest live snapshot seqno, and **drop tombstones once they reach the bottom level** (no older version can exist beneath ⇒ reclaim deleted space) → write output SSTables → fsync → manifest `VersionEdit` (remove inputs + add outputs atomically) → delete input files.
- **Non-blocking concurrency:** inputs are immutable; readers pin a refcounted **manifest Version**; an input file is physically deleted only when its last referencing reader releases. Writers only ever touch the MemTable. A background compaction scheduler (same thread pattern as the Phase-4 checkpointer) selects the highest-priority level, runs one compaction per level at a time, and is rate-limited.

---

## 9. Recovery & ACID (ask #6)

**Startup → rebuild the active MemTable from the WAL tail:**

1. Read `CURRENT` → manifest → reconstruct level assignment, open SSTables (load footers/index/Bloom), read the persisted checkpoint LSN.
2. **Replay the WAL tail** via Phase-4 `BinaryWalRecovery`: frames with `Lsn > checkpointLsn` apply into a **fresh active MemTable** (Insert/Update/Delete → Put/tombstone @ `seqno = Lsn`), stopping at the first torn frame (CRC failure). *This is Phase-4 recovery with the replay target swapped from `.dat` to the MemTable.*
3. Set the seqno counter to the max replayed `Lsn`. Engine ready. (Optionally flush the recovered MemTable immediately to shrink the WAL on a clean restart.)

**ACID:**
- **Atomicity** (multi-row txn): all of a transaction's frames carry the same commit seqno and are bounded by a `TxnCommit` frame; on replay, frames after the last complete `TxnCommit` (an un-terminated transaction) are discarded. (Already present in Phase 4.)
- **Consistency / Isolation:** snapshot reads by seqno (MVCC).
- **Durability:** the WAL group-commit fsync is the durability point (Phase 4). Flush and compaction are crash-atomic via the manifest, and the WAL still covers committed data until `checkpointLsn` advances past it.
- **Crash matrix:** mid-flush ⇒ SSTable absent from the manifest, ignored; WAL replays the data. Mid-compaction ⇒ outputs absent from the manifest, ignored; inputs still referenced and intact. Torn manifest edit ⇒ CRC-rejected, last good manifest state stands.
- **Closes a known gap:** recovery now rebuilds the integer fast lane *and* secondary indexes from merged state, so the zero-alloc update path and secondary lookups survive restart. (Today the integer fast lane is only populated by live typed inserts and silently degrades to the legacy path after every restart.)

---

## 10. Cross-cutting: secondary indexes & the RowId ripple

- **Secondary indexes become `value → PK`** (not `value → offset`), since physical location is no longer stable. A secondary lookup yields PKs, each resolved through the table's LSM by point lookup.
  - **Scalable form (target):** each secondary index is its own LSM tree keyed by `(indexedValue ‖ PK) → ∅`, maintained by emitting index Put/Delete entries alongside base-table writes at the same seqno.
  - **Pragmatic interim:** keep the in-memory B-trees but re-keyed to PK and rebuilt on recovery from a merged scan. The implementation plan will choose the staging.
- **RowId ripple — the largest integration surface.** Every current call site that consumes a byte-offset `RowId` (compiled queries, `FilterUsingIndex`, MVCC `RegisterUpdateVersion`/`ValidateCanModifyRow`, reactive change capture, snapshot/restore) must be audited and migrated to the PK-identity model behind the `IStorageEngine` seam. The implementation plan will enumerate these call sites explicitly before any are changed.

---

## 11. Components (units, each independently testable)

| Unit | Responsibility | Key dependencies |
|---|---|---|
| `InternalKey` (encode/compare) | order-preserving PK ‖ seqno ‖ type codec + comparator | `IndexKeyEncoder` |
| `Arena` | pooled bump-allocator slabs; reset-on-flush | — |
| `MemTable` | single-writer sorted skiplist over `Arena`; Put/Delete/Get/iterate | `InternalKey`, `Arena` |
| `BloomFilter` | alloc-free `byte[]` bit array; double-hash build + probe | xxHash64 |
| `SsTableWriter` | data/filter/index/footer layout; sequential flush | `BloomFilter`, `InternalKey` |
| `SsTableReader` | footer/index/Bloom load; point get + iterator | `BloomFilter`, `FileHandlePool`, block cache |
| `Manifest` | `VersionEdit` log; CURRENT pointer; refcounted `Version` | — |
| `FlushJob` | freeze immutable MemTable → `SsTableWriter` → manifest install | `MemTable`, `SsTableWriter`, `Manifest` |
| `Compactor` | hybrid trigger; k-way merge; manifest swap; file GC | `SsTableReader/Writer`, `Manifest` |
| `LsmTable` | per-table orchestration: active+immutable MemTables, levels | all of the above |
| `LsmStorageEngine` | `IStorageEngine` impl; routes ops to `LsmTable`s; recovery | `LsmTable`, `BinaryWalRecovery` |

---

## 12. Testing strategy (TDD throughout)

- **Unit:** `InternalKey` ordering (PK asc, seqno desc, type tie-break); `Arena` reuse + reset; `MemTable` newest-wins + tombstone + sorted iteration; `BloomFilter` no-false-negatives + measured FPR ≈ target + **zero-allocation probe** (allocation assertion); `SsTableWriter`/`Reader` round-trip incl. block boundaries; `Manifest` `VersionEdit` replay + torn-edit rejection.
- **Integration:** flush freezes without blocking concurrent writes; point read resolves across MemTable + multiple SSTable levels; Bloom filter measurably skips data-block I/O (counter assertion); compaction merges + drops tombstones at the bottom level + reclaims space; range scan merge-iterator newest-wins.
- **Recovery / ACID:** record survives restart via WAL replay into a fresh MemTable; mid-flush crash (SSTable not in manifest) replays from WAL; mid-compaction crash leaves inputs intact; torn manifest edit rejected; integer fast lane + secondary indexes rebuilt after restart.
- **Allocation:** steady-state write loop asserts ~0 GC (arena + pooled WAL) on the hot path.
- **Benchmark (success criteria):** single-threaded `disk-crud-wal` with `--engine datavo-lsm` A/B'd against `datavo-groupcommit` (B-tree) and SQLite WAL — UPDATE B/op and CPU/op must drop materially below the 4.9 KB/op residual; point-read latency must not regress.

---

## 13. Success criteria

- **C1.** Update/Delete perform **no `.dat` mutation** and **no synchronous B-tree delete** (verified by the absence of `DeleteRow`/`DeleteValues` on the hot path).
- **C2.** Steady-state write loop allocates **~0 GC** on the hot path (arena + pooled WAL).
- **C3.** Point reads use Bloom filters to skip non-matching SSTables (verified by a skipped-I/O counter) and do not regress vs the B-tree engine.
- **C4.** All Phase-4 ACID guarantees preserved (crash matrix tests green); WAL stays bounded via flush-driven checkpointing.
- **C5.** `LsmStorageEngine` runs side-by-side with `DiskStorageEngine`, selectable by config and benchmarkable in the same harness.
- **C6.** `disk-crud-wal` UPDATE B/op and CPU/op drop materially below the 4.9 KB/op residual.

---

## 14. Risks & mitigations

- **RowId ripple (highest risk):** audit and enumerate every byte-offset `RowId` consumer before migration; the `IStorageEngine` seam contains the blast radius; coexistence keeps `DiskStorageEngine` as a fallback.
- **Single-writer MemTable as a throughput ceiling:** acceptable for v1 because the group-commit pipeline is already the single funnel; a lock-free skiplist is a clean later upgrade behind the same `MemTable` interface.
- **Read amplification before compaction settles:** Bloom filters + leveled L1+ bound the number of files a point read touches; L0 file-count trigger prevents L0 pileup.
- **Manifest/Version refcounting correctness:** isolate in `Manifest`/`Version` units with dedicated crash and concurrency tests before wiring the compactor.
- **Scope creep into compression/column families:** explicitly out of scope for v1 (§1 non-goals).
