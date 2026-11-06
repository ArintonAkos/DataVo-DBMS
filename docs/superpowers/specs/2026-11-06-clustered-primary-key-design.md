# Clustered Primary Keys

> **Date:** 2026-11-06
> **Status:** Approved: option A (16-byte `RowKey`; INT and GUID keys clustered). Implementation in stages.
> **Scope:** LSM storage mode. Row identity across storage, indexes, MVCC, transactions, the SQL layer,
> compiled queries, live (reactive) queries, and the vector index.

## 1. Problem

Every LSM table stores rows under an internal 8-byte row id (allocated 1, 2, 3, ...) and keeps the
primary key in a separate index table mapping key -> row id. Measured on 1M rows at rest:

| Primary key | Rows | PK index | Total | SQLite (INTEGER PK) |
| --- | --- | --- | --- | --- |
| INT | 31.5 MB | 14.6 MB | 46 MB | 29 MB |
| GUID v4 | 43.1 MB | 27.9 MB | 71 MB | - |
| GUID v7 | 43.1 MB | 24.1 MB | 67 MB | - |

A primary-key read does two lookups (key -> row id, row id -> row). Clustering stores the row under its
primary key: the PK index disappears and a PK read is one lookup. This is how InnoDB, SQL Server
(clustered index), SQLite `WITHOUT ROWID`, CockroachDB and MyRocks store tables.

## 2. What depends on the row id today

The row id is a `long` in 88 files (~1,300 references):

| Area | Files | References | Role of the row id |
| --- | --- | --- | --- |
| Storage engine (LSM, in-memory, disk) | 24 | 417 | Row key, overlay slot, NextRowId, batches |
| Indexing (secondary, PK, HNSW) | 9 | 374 | Index value; HNSW ordinal map |
| SQL layer (parser, evaluators, joins) | 15 | 207 | Result sets (`HashedTable` keyed by row id) |
| Compiled queries | 4 | 107 | Point updates, fast lanes |
| Transactions, WAL | 6 | 73 | Write sets, undo |
| MVCC | 3 | 39 | Version chains per row id |
| Runtime, live queries | 14 | 49 | `RowChange` identity |

The overlay of unflushed rows (`LsmLatestRowStore`) is an array paged by row id: it assumes small,
dense, non-negative ids.

## 3. Options

### A. Fixed 16-byte row key (recommended)

Widen the row handle from `long` to a 16-byte value type `RowKey`, and make it the primary key itself
when the key fits in 16 bytes:

- INT/BIGINT key -> the integer (sign-extended); GUID key -> its 16 bytes.
- Any other key (VARCHAR, composite, no key): keep today's allocated surrogate, stored in the same
  16 bytes, plus today's PK index.
- Rows are stored and sorted by `RowKey`; the PK index is not written for clustered tables.
- Secondary indexes store `RowKey` (16 bytes instead of 8) as today they store the row id.

Fixed size keeps everything that holds handles simple and fast: arrays, the HNSW ordinal map, MVCC
dictionaries, and `HashedTable` all keep fixed-size keys. INT and GUID cover nearly all primary keys.

### B. Variable-length PK bytes (full InnoDB)

The handle becomes the encoded key bytes of any length. Clusters VARCHAR and composite keys too, but
every handle becomes a heap object or span with variable comparisons: much slower in the hot paths
(HashSet/Dictionary of handles, HNSW map), and a larger change than A.

### C. INT only

Handle stays `long`; only INT keys cluster. Small change, does nothing for GUID tables.

**Recommendation: A.** It is "general" for the key types applications use (integers and GUIDs),
keeps handles fixed-size, and leaves B possible later.

## 4. Design (option A)

### 4.1 `RowKey`

`readonly struct RowKey : IEquatable<RowKey>, IComparable<RowKey>` with two `ulong` halves, ordered as
unsigned big-endian bytes. Constructors: `FromInt64(long)` (order-preserving sign flip, like
`InternalKey.EncodeInt64UserKey`), `FromGuid(Guid)` (RFC 4122 byte order, so UUIDv7 keys sort by time),
and `FromSurrogate(long)`. Equality and hashing are two 64-bit compares.

### 4.2 Storage

- **Row SSTables, format v6.** The block format keyed by 16-byte keys: restart entries hold the full
  key, other entries hold a shared-prefix length plus the differing bytes (sequential integers and
  UUIDv7 keys share most bytes; random GUIDv4 keys share little). Bloom filter over the 16 bytes.
- **Overlay.** Replace the dense page array with a sparse map of pages keyed by the high bits of the
  key (dense for small surrogates, sparse for GUIDs). Reads stay lock-free: pages are published once
  and never move; the page map is copy-on-write.
- **Insert.** For a clustered table the engine derives the key from the row (the PK column) instead of
  allocating one. Uniqueness is "a live row with this key exists", checked under the table lock.
- **Commit log.** Frames carry the 16-byte key. Old frames (8-byte ids) are read as surrogates.

### 4.3 Indexes

- PK index: not written for clustered tables; PK lookups read the row directly.
- Secondary index entries: `key | RowKey` (16-byte suffix). Index format version bumps; old index
  tables are rebuilt from rows on first open (the existing "not built" path).
- HNSW: `long[] _rowIdByOrdinal` becomes `RowKey[]`; snapshots get a new format version (old ones are
  rebuilt, already supported).

### 4.4 Updates that change the primary key

Updating a clustered key moves the row: delete under the old key and insert under the new one in the
same commit frame, re-pointing secondary index entries, the vector index and MVCC versions. Foreign
keys reference key values, not row ids, and are unaffected.

### 4.5 Compatibility

Clustering applies to tables created after the change; existing tables keep surrogate ids (stored as
`RowKey.FromSurrogate`) and their PK index, and keep working. A later `VACUUM` could convert a table.

## 5. Delivery in stages (each merged green, with tests)

1. `RowKey` type and the `long` -> `RowKey` change across all layers, surrogates only. No behaviour
   change; the whole suite and the randomized tests must stay green. (Largest stage: mechanical but
   wide.)
2. Sparse overlay and 16-byte row SSTable format (v6), commit-log frames with 16-byte keys; read old
   formats. **Done.** Also moved here from stage 5: LSM index entries carry 16-byte row keys (old index
   tables are rebuilt on open), because GUID-keyed tables need them in stage 3. A v6 file stores blocks
   of integer keys in the v4 encoding and only GUID blocks in the wide encoding, so integer tables keep
   v4 size and speed. The engine has `TryInsertRowWithKey` (uniqueness by row existence); stage 3 wires
   it to SQL.
3. Clustered inserts for INT and GUID keys on new tables; uniqueness by row existence; no PK index.
   **Done**, together with 4 and 5: the catalog marks a table `Clustered` at CREATE TABLE (LSM, one INT
   or GUID key, `DataVoConfig.ClusteredPrimaryKeys`); the LSM backend derives each row's key from the row
   and the index store serves the PK index from the rows. 1M rows: INT 41.4 -> 25.1 MB, GUID 61.4 ->
   45.5 MB, inserts 2-2.5x faster.
4. PK-changing updates (row moves) and re-pointing. **Done in 3**: updates already delete and re-insert,
   so a changed key moves the row in one commit; MVCC metadata now handles a key reused after a delete.
5. HNSW format version with rebuild of old snapshots. **Done in 3** (a GUID-keyed vector table must not
   regress): vector indexes key nodes by RowKey (public `IVectorIndex` changed), snapshot format 2.
6. Measurements (space, read/write latency, allocation) against the numbers in section 1; docs.

## 6. Testing

- The LSM model test gains clustered tables: random INT and GUID keys (including negative and
  extreme integers), PK-changing updates, crashes and reopens.
- The SQLite differential test gains GUID-keyed and clustered tables (SQLite: `WITHOUT ROWID`).
- Format tests for v6 blocks (round trip, restarts, corruption, mixed old and new files).

## 7. Risks

- Stage 1 touches ~88 files; performance must not regress (16-byte handles double the memory of
  row-id lists; measured before merging).
- The overlay is on the lock-free read path tuned for p99; the sparse version must keep reads
  lock-free and allocation-free.
- Estimated effort: about 2 weeks.
