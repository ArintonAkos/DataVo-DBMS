# RowId Ripple Audit (LSM migration blast radius)

**Date:** 2026-06-29
**Purpose:** Enumerate every site that assumes `RowId == physical byte offset`, to scope the
`IStorageEngine` → `LsmStorageEngine` migration. Source of truth for Plan 5.

---

## Conventions

- **Current meaning** — what `RowId` (a `long`) represents today: the byte offset in the per-table `.dat` file at which the row's length-prefixed record was written.
- **LSM migration note** — what must change when the LSM engine replaces the disk engine. The LSM identity is a *logical PK + seqno*; there is no physical byte offset.
- **"Safe behind the seam"** — the call is inside `IStorageEngine` or `StorageContext` and the value never escapes upward as a durable identity. Swapping the implementation suffices.
- **"Leaks above the seam"** — the byte-offset RowId is stored, compared, or forwarded outside `IStorageEngine`/`StorageContext`, requiring explicit migration.

---

## 1. Storage-engine seam (IStorageEngine / ITypedRowStorageEngine / StorageContext)

These files define or implement the interface boundary. All RowId-returning methods here are the *definition* of byte-offset identity in the current system; replacing the `IStorageEngine` implementation is the primary task for Plan 5.

### 1a. IStorageEngine interface

| Site | Today | LSM migration |
|------|-------|---------------|
| `IStorageEngine.cs:16` `long InsertRow(...)` | Returns byte offset at which the row was appended | Returns a logical seqno assigned by the LSM MemTable; callers must treat return value as opaque |
| `IStorageEngine.cs:25` `List<long> InsertRows(...)` | Returns list of byte offsets in insertion order | Returns list of logical seqnos; order preserved |
| `IStorageEngine.cs:33` `byte[] ReadRow(string databaseName, string tableName, long rowId)` | Seeks to `rowId` as byte offset and reads length-prefixed bytes | LSM resolves by PK across MemTable + SSTables; `rowId` becomes a logical opaque key |
| `IStorageEngine.cs:40` `IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(...)` | Sequential file scan; RowId == offset of each record | Full merge scan across L0..LN; RowId == logical seqno of each entry |
| `IStorageEngine.cs:48` `void DeleteRow(string databaseName, string tableName, long rowId)` | Writes tombstone byte at `rowId` offset | Writes a tombstone entry keyed by logical seqno; compaction removes it |
| `IStorageEngine.cs:69` `List<(long NewRowId, byte[] RawRow)> CompactTable(...)` | Rewrites surviving rows contiguously; returns new byte offsets | LSM compaction is background; method may be no-op or trigger forced compaction; returned logical seqnos differ from old offsets |

### 1b. ITypedRowStorageEngine interface

| Site | Today | LSM migration |
|------|-------|---------------|
| `ITypedRowStorageEngine.cs:9` `bool TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row)` | Seeks by byte offset, deserializes typed row | LSM resolves by logical seqno |
| `ITypedRowStorageEngine.cs:11` `IEnumerable<(long RowId, StoredRow Row)> ReadAllTypedRows(...)` | Yields typed rows with their byte offsets | Yields typed rows with logical seqnos; interface unchanged, semantics shift |

### 1c. DiskStorageEngine implementation

| Site | Today | LSM migration |
|------|-------|---------------|
| `DiskStorageEngine.cs:148` (doc comment) | Explicitly documents "byte-offset RowId" | Will become "logical seqno assigned by MemTable" |
| `DiskStorageEngine.cs:160` `long rowId = RandomAccess.GetLength(handle)` | RowId == end-of-file offset before write | Removed; LSM assigns seqno |
| `DiskStorageEngine.cs:173` `long rawByteOffsetRowId = fileStream.Position` | RowId == stream position before write | Removed; LSM assigns seqno |
| `DiskStorageEngine.cs:182` `return rawByteOffsetRowId` | Returns the captured byte position | Returns LSM-assigned logical seqno |
| `DiskStorageEngine.cs:192` (doc comment) | "byte-offset RowIds written in insertion order" | Updated to "logical seqnos" |
| `DiskStorageEngine.cs:224` `long rowId = fileStream.Position` | Batch insert: captures offset before each row write | Removed in LSM |
| `DiskStorageEngine.cs:237,241` (doc comments) | "Reads a row payload by byte-offset RowId" | Updated semantics |
| `DiskStorageEngine.cs:243` `public byte[] ReadRow(string databaseName, string tableName, long rowId)` | `RandomAccess.Read` at byte offset `rowId` | LSM point-query by seqno → MemTable or SSTable lookup |
| `DiskStorageEngine.cs:288` `public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(...)` | Linear scan of `.dat` file; offset tracked per record | Merge-iterator scan across sorted runs |
| `DiskStorageEngine.cs:305` `long rowId = offset` | Assigns current file offset as RowId during scan | Removed; LSM uses per-entry seqno from index |
| `DiskStorageEngine.cs:335` `long rowId = fileStream.Position` | Offset tracking in sequential read | Removed |
| `DiskStorageEngine.cs:355,356` (doc+method) | `DeleteRow` seeks to byte offset, writes tombstone flag | LSM writes tombstone entry; no seek needed |
| `DiskStorageEngine.cs:450` `CompactTable(...)` | Rewrites .dat removing tombstoned rows; returns (newOffset, bytes) pairs | LSM compaction is SSTable merge; returns (newSeqno, bytes) or is redesigned as void |

### 1d. InMemoryStorageEngine implementation

| Site | Today | LSM migration |
|------|-------|---------------|
| `InMemoryStorageEngine.cs:63` `public long InsertRow(...)` | Assigns incrementing integer as RowId (not byte offset, but same contract) | Uses logical seqno; contract matches LSM |
| `InMemoryStorageEngine.cs:88` `public List<long> InsertRows(...)` | Returns list of incrementing integers | Same |
| `InMemoryStorageEngine.cs:117` `public byte[] ReadRow(string databaseName, string tableName, long rowId)` | Dictionary lookup by integer RowId | Dictionary lookup by logical seqno |
| `InMemoryStorageEngine.cs:156,158` `public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(...)` | Yields all (integer, bytes) pairs | Yields (seqno, bytes) pairs |
| `InMemoryStorageEngine.cs:188` `public void DeleteRow(string databaseName, string tableName, long rowId)` | Removes by integer key | Removes by logical seqno |
| `InMemoryStorageEngine.cs:242` `public List<(long NewRowId, byte[] RawRow)> CompactTable(...)` | Re-indexes rows with new sequential integers; returns (newId, bytes) | Same logic, new ids are seqnos |
| `InMemoryStorageEngine.cs:317` `bool TryReadTypedRow(string databaseName, string tableName, long rowId, ...)` | Typed read by integer key | Typed read by logical seqno |
| `InMemoryStorageEngine.cs:343,345` `IEnumerable<(long RowId, StoredRow Row)> ReadAllTypedRows(...)` | Yields (integer, StoredRow) | Yields (seqno, StoredRow) |

### 1e. Backend wrappers (pure delegation — safe behind the seam)

| Site | Today | LSM migration |
|------|-------|---------------|
| `DiskStorageBackend.cs:23-30` (all six methods) | Thin delegation to `DiskStorageEngine` | Replace delegatee with `LsmStorageEngine`; no logic change in wrapper |
| `WasmStorageBackend.cs:24-31` (all six methods) | Thin delegation to Wasm inner engine | Same — delegate to LSM variant when targeting Wasm |
| `InMemoryStorageBackend.cs:12-29` (all methods including typed) | Thin delegation to `InMemoryStorageEngine` | Replace delegatee; in-memory LSM maps cleanly |

### 1f. StorageContext (mediating DAO layer)

StorageContext calls `IStorageEngine` and returns `Dictionary<long, ...>` keyed by RowId. These are safe behind the seam if the RowId opaqueness contract is maintained throughout the call stack.

| Site | Today | LSM migration |
|------|-------|---------------|
| `StorageContext.cs:150` `_storageEngine.InsertRow(...)` | Delegates insert; returns byte offset | Returns logical seqno; safe if callers treat return value as opaque handle |
| `StorageContext.cs:162` `InsertSerializedRow(...)` | Delegates insert; returns byte offset (used by compiled UPDATE to get `newRowId`) | Returns logical seqno; **leaks above seam** — the returned value is stored in MVCC and index (see §3, §4) |
| `StorageContext.cs:178` `InsertIntoTable(List<...> rows, ...)` | Delegates batch insert; returns byte offsets | Returns logical seqnos |
| `StorageContext.cs:200` `InsertRows(...)` typed overload | Same | Same |
| `StorageContext.cs:215` `InsertOneIntoTable(...)` | Returns single byte offset | Returns logical seqno |
| `StorageContext.cs:228` `DeleteFromTable(...)` | Loops `_storageEngine.DeleteRow(id)` | Same loop; `id` is logical seqno in LSM |
| `StorageContext.cs:239` (TableContainsRow method) | Calls `TryReadRowBytes` to check if offset resolves | LSM checks MemTable + SSTables |
| `StorageContext.cs:324` `TryReadRowBytes(string tableName, string databaseName, long rowId)` | `_storageEngine.ReadRow(databaseName, tableName, rowId)` at byte offset | Passes logical seqno to LSM point-query |
| `StorageContext.cs:334` `_storageEngine.ReadRow(...)` inside TryReadRowBytes | Same as above | Same |
| `StorageContext.cs:346` `TryReadStoredRow(...)` | Calls TryReadRowBytes then deserializes | Same |
| `StorageContext.cs:355` `TryReadRowBytes` call inside TryReadStoredRow | Same | Same |
| `StorageContext.cs:373` iteration over `_storageEngine.ReadAllRows(...)` | Sequential scan with byte-offset RowIds | Merge-iterator scan with logical seqnos |
| `StorageContext.cs:385` `IsRowVisible(string tableName, string databaseName, long rowId)` | Calls `MvccCoordinator.EnsureRowVersionExists(...)` using `rowId` as MVCC key | **Leaks above seam** — RowId passed to MVCC layer (see §3) |
| `StorageContext.cs:394` `MvccCoordinator.EnsureRowVersionExists(engine, databaseName, tableName, rowId)` | MVCC lookup by byte-offset | Must use logical seqno key in LSM |
| `StorageContext.cs:424,438,460,474` iteration over `rows.Keys` with MVCC filter | Filters rows dict (keyed by byte offset) through MVCC visibility | Filters dict keyed by logical seqno |
| `StorageContext.cs:426,462` `MvccCoordinator.EnsureRowVersionExists(... rowId)` inside MVCC filter | MVCC lookup by byte offset | Must use logical seqno |
| `StorageContext.cs:500` `CompactTable(...)` | Delegates to `_storageEngine.CompactTable`; returns (newOffset, bytes) | Returns (newSeqno, bytes) |
| `StorageContext.cs:582,605,639,656` `_storageEngine.ReadRow(databaseName, tableName, rowId)` inside ReadRowsById / ReadAllRows | Byte-offset seek per row | LSM point-query per logical seqno |
| `StorageContext.cs:591,607,648,658` `byte[] rawRow = _storageEngine.ReadRow(...)` | Fetches raw bytes at byte offset | Fetches from MemTable/SSTable by seqno |
| `StorageContext.cs:678,711` `typedStorage.ReadAllTypedRows(...)` iteration | Scans typed rows with byte-offset keys | Scans typed rows with logical seqno keys |
| `StorageContext.cs:684,717` `_storageEngine.ReadAllRows(...)` iteration in ReadAllRows | Sequential file scan | Merge scan |

### 1g. Exception types

| Site | Today | LSM migration |
|------|-------|---------------|
| `Exceptions/RowDeletedException.cs:14,16,24,26` | "byte-offset row identifier that was tombstoned"; `long RowId` property | Rename doc comment; `RowId` value becomes logical seqno but exception semantics unchanged |
| `Exceptions/RowNotFoundException.cs:11,23,36` | `long RowId` property; "Row {rowId} in table '{tableName}' was not found." | Same — `RowId` value changes semantics to logical seqno; message format preserved |

---

## 2. Index → location mapping

The integer PK fast lane (`_integerPrimaryKeyMaps`) maps `(integer PK column value) → (byte-offset RowId)`. B-Tree and vector indexes also store `(string/vector key) → (byte-offset RowId)` lists. In the LSM, the PK value IS the location key; the fast lane becomes `(PK value) → (PK value)` or is eliminated.

### 2a. IndexManager — integer PK fast lane

| Site | Today | LSM migration |
|------|-------|---------------|
| `IndexManager.cs:112` `_integerPrimaryKeyMaps: ConcurrentDictionary<IndexCacheKey, ConcurrentDictionary<long, long>>` | Inner map: `(integer PK value) → (byte-offset RowId)` | Must change to `(integer PK value) → (logical seqno)` or be eliminated if LSM index is the authoritative location |
| `IndexManager.cs:886-904` `InsertIntegerPrimaryKeys(IReadOnlyList<(long Key, long RowId)> entries, ...)` | Upserts `key → rowId` into fast lane; `rowId` = byte offset | `rowId` becomes logical seqno; signature stays but semantics shift |
| `IndexManager.cs:898` `map[key] = rowId` | Stores byte offset as value | Stores logical seqno |
| `IndexManager.cs:948` `HasIntegerPrimaryKeyFastLane(...)` | Checks if fast lane exists for index | Safe — structural check only |
| `IndexManager.cs:967-974` `RemoveIntegerPrimaryKey(long key, ...)` | Removes key→rowId entry; `rowId` implicit via key removal | Safe — key removal, no offset embedded |
| `IndexManager.cs:1000-1009` `TryLookupIntegerPrimaryKey(..., out long rowId)` | Returns byte-offset RowId for a PK value | Returns logical seqno; callers that pass this to `StorageContext.ReadRow` must update contract |
| `IndexManager.cs:1070` `_integerPrimaryKeyMaps.ContainsKey(cacheKey)` inside FilterUsingIndex | Fast lane existence check | Safe — structural check |
| `IndexManager.cs:1095-1147` `FilterUsingIndex(string columnValue, string indexName, ...)` | Returns `IReadOnlyList<long>` of byte-offset RowIds | Returns `IReadOnlyList<long>` of logical seqnos; callers unchanged in signature but must treat values as opaque |
| `IndexManager.cs:1098` `TryLookupIntegerPrimaryKey(integerKey, ..., out long rowId)` | Returns byte offset | Returns logical seqno |
| `IndexManager.cs:1104,1128,1133,1147` fast lane existence/map checks | Structural | Safe |

### 2b. IndexManager — general index operations

| Site | Today | LSM migration |
|------|-------|---------------|
| `IndexManager.cs:445` `foreach (long rowId in entry.Value)` | Iterates byte-offset rowId set in B-Tree value list | Same loop; values become logical seqnos |
| `IndexManager.cs:727,767,773` `InsertIntoVectorIndex(... long rowId ...)` | Stores byte-offset RowId as the vector ordinal identifier | Must store logical seqno; ordinal mapping (seqno → ordinal) must be updated |
| `IndexManager.cs:838` `InsertIntoIndex(string value, long rowId, ...)` | Inserts `(string key) → (byte-offset rowId)` entry into B-Tree | Inserts `(string key) → (logical seqno)` |
| `IndexManager.cs:856,872` `InsertManyIntoIndex(IReadOnlyList<(string Value, long RowId)> entries, ...)` | Batch B-Tree insert; RowId = byte offset | RowId = logical seqno |
| `IndexManager.cs:887,901` `InsertIntegerPrimaryKeys(IReadOnlyList<(long Key, long RowId)> entries, ...)` | Covered in §2a | Same |
| `IndexManager.cs:910,931` `InsertIntegerIndexEntries(IReadOnlyList<(long Key, long RowId)> entries, ...)` | Stores `(integer key) → [byte-offset rowIds]` in scalar fast lane | Stores `(integer key) → [logical seqnos]` |
| `IndexManager.cs:980` `RemoveIntegerIndexEntry(long key, long rowId, ...)` | Removes specific `(key, byte-offset)` pair | Removes `(key, logical seqno)` pair |
| `IndexManager.cs:1154,1181` `long rowId = rowIds[i]` inside bulk index rebuild methods | Byte offsets from scan result | Logical seqnos |
| `IndexManager.cs:1230` `IndexContainsRow(long rowId, ...)` | Checks if byte-offset is in B-Tree value set | Checks if logical seqno is in B-Tree value set |
| `IndexManager.cs:1324` `List<(long RowId, float[] Vector)> vectors = []` inside `RebuildVectorIndex` | Collects (byte-offset, vector) pairs | Collects (logical seqno, vector) pairs |

### 2c. B-Tree index interfaces and implementations

| Site | Today | LSM migration |
|------|-------|---------------|
| `BTree/Core/IIndex.cs:56` `void Insert(string key, long rowId)` | Stores `(string key) → (byte-offset rowId)` | Stores `(string key) → (logical seqno)` |
| `BTree/Core/IIndex.cs:109` `bool ContainsValue(long rowId)` | Scans B-Tree values for byte offset | Scans for logical seqno |
| `BTree/Core/JsonBTreeIndex.cs:133` `ContainsValue(long rowId)` | B-Tree value scan by byte offset | By logical seqno |
| `BTree/Binary/BinaryBTreeIndex.cs:75` `void Insert(string key, long rowId)` | B-Tree insert with byte-offset value | Logical seqno value |
| `BTree/Binary/BinaryBTreeIndex.cs:202` `bool ContainsValue(long rowId)` | Scan by byte offset | Scan by logical seqno |
| `BTree/BPlus/BinaryBPlusTreeIndex.cs:30` `void Insert(string key, long rowId)` | B+ tree insert with byte-offset value | Logical seqno value |

### 2d. Vector index implementations

| Site | Today | LSM migration |
|------|-------|---------------|
| `Indexing/IVectorIndex.cs:11` `void Insert(long rowId, float[] vector)` | Associates byte-offset RowId with vector | Associates logical seqno with vector |
| `Indexing/ISpanVectorIndex.cs:5` `void Insert(long rowId, ReadOnlySpan<float> vector)` | Same | Same |
| `Indexing/Flat/FlatVectorIndex.cs:73,86` `Insert(long rowId, ...)` | Inserts byte-offset → ordinal mapping via `AcquireOrdinal` | Inserts logical seqno → ordinal mapping |
| `Indexing/Flat/FlatVectorIndex.cs:123` `foreach (long rowId in rowIds)` | Iterates byte-offset ids for deletion | Iterates logical seqnos |
| `Indexing/Flat/FlatVectorIndex.cs:213` `long rowId = _rowIdByOrdinal[ordinal]` | Reverse lookup: ordinal → byte offset | Ordinal → logical seqno |
| `Indexing/Flat/FlatVectorIndex.cs:310` `AcquireOrdinal(long rowId)` | Maps byte-offset RowId to dense ordinal | Maps logical seqno to dense ordinal |
| `Indexing/Flat/FlatVectorIndex.cs:384,388` `ExportEntries()` returning `List<(long RowId, float[] Vector)>` | Exports (byte-offset, vector) pairs | Exports (logical seqno, vector) pairs |
| `Indexing/Flat/FlatVectorIndex.cs:419` `ImportEntries(IEnumerable<(long RowId, float[] Vector)> entries)` | Imports (byte-offset, vector) pairs | Imports (logical seqno, vector) pairs |
| `Indexing/Flat/FlatVectorIndex.cs:433` `InsertSorted(Span<long> rowIds, ...)` | Sorted insertion of byte-offset ids | Sorted insertion of logical seqnos |
| `Indexing/HNSW/HNSWIndex.cs:362,373` `Insert(long rowId, ...)` / `InsertCore(long rowId, ...)` | Byte-offset → ordinal mapping | Logical seqno → ordinal |
| `Indexing/HNSW/HNSWIndex.cs:479` `foreach (long rowId in rowIds)` | Iterate byte offsets for deletion | Iterate logical seqnos |
| `Indexing/HNSW/HNSWIndex.cs:576,1505` `long rowId = _rowIdByOrdinal[ordinal]` | Reverse ordinal → byte offset | Reverse ordinal → logical seqno |
| `Indexing/HNSW/HNSWIndex.cs:899` `AcquireOrdinal(long rowId)` | Maps byte offset to ordinal | Maps logical seqno to ordinal |
| `Indexing/HNSW/HNSWIndexPersistence.cs:26` `public long RowId { get; set; }` | Serialized byte offset in HNSW persistence format | Must change serialized field to logical seqno; on-disk format migration needed |
| `Indexing/HNSW/BrowserFallbackVectorIndex.cs:10` `Insert(long rowId, float[] vector)` | Byte-offset → ordinal | Logical seqno → ordinal |
| `Indexing/HNSW/BrowserFallbackVectorIndex.cs:27` `foreach (long rowId in rowIds)` | Iterate byte offsets | Iterate logical seqnos |
| `Indexing/HNSW/BrowserFallbackVectorIndex.cs:63,70` `ExportEntries()` / `ImportEntries(...)` | (byte-offset, vector) pairs | (logical seqno, vector) pairs |

### 2e. FilterUsingIndex callers — upper-layer consumers of returned RowIds

All call sites that call `FilterUsingIndex` or `TryLookupIntegerPrimaryKey` receive a `long` or `IReadOnlyList<long>` of byte-offset RowIds and immediately pass them to `StorageContext` for row reads or lock acquisition. These sites **do not** store the RowIds persistently — they are transient within a single statement execution. They are safe if the contract shifts to logical seqnos consistently.

| Site | Today | LSM migration |
|------|-------|---------------|
| `CompiledQueries/DataVoCompiledQuery.cs:465,475` `TryLookupIntegerPrimaryKey(..., out long rowId)` | Gets byte offset; passed to MVCC, lock, storage | Gets logical seqno; passes through safely if all layers aligned |
| `CompiledQueries/DataVoCompiledQuery.cs:511` `InsertIntegerPrimaryKeys([(primaryKey, newRowId)], ...)` | Stores new byte offset in fast lane after UPDATE | Stores new logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:732,737,754,768,898,1356` `FilterUsingIndex(...)` calls | Returns byte-offset id lists for SELECT/UPDATE predicates | Returns logical seqno lists; downstream storage reads adapt |
| `CompiledQueries/DataVoCompiledQuery.cs:1539,1545` `RemoveIntegerPrimaryKey` / `InsertIntegerPrimaryKeys` | Swap byte offsets in fast lane during PK UPDATE | Swap logical seqnos |
| `CompiledQueries/DataVoPreparedSelectMany.cs:82` `FilterUsingIndex(...)` | Returns byte-offset ids for typed read | Returns logical seqnos |
| `CompiledQueries/DataVoPreparedSelectSingle.cs:71,90` `TryLookupIntegerPrimaryKey` / `FilterUsingIndex(...)` | Returns byte offset or list for single-row read | Returns logical seqno |
| `Parser/DML/InsertRowService.cs:982` `TryLookupIntegerPrimaryKey(...)` (uniqueness check) | Returns byte offset; used only as presence check (`out _`) | Safe — presence only, value discarded |
| `Parser/DML/InsertRowService.cs:1154,1211` `InsertIntegerPrimaryKeys(...)` | Stores new byte offsets in fast lane | Stores logical seqnos |
| `Parser/DML/DeleteFrom.cs:169` `FilterUsingIndex(...)` (FK cascade check) | Returns byte offsets of child rows | Returns logical seqnos; downstream DELETE uses them |
| `Parser/DML/Update.cs:405` `FilterUsingIndex(...)` (FK parent check) | Returns byte offsets of child rows | Returns logical seqnos |
| `Parser/Statements/Mechanism/StatementEvaluator.cs:133,146` `FilterUsingIndex(...)` | Returns byte-offset id lists for interpreter SELECT | Returns logical seqnos |
| `Parser/Statements/Mechanism/StatementEvaluatorWOJoin.cs:115,126` `FilterUsingIndex(...)` | Same | Same |

---

## 3. MVCC & reactive change capture

The MVCC layer uses the byte-offset RowId as the primary key for version metadata. The version chain (`VersionChain` field) stores the next RowId as a byte offset, encoding history as a linked list of file offsets. These must migrate to logical (PK + seqno) chaining.

### 3a. VersionStorageManager

| Site | Today | LSM migration |
|------|-------|---------------|
| `MVCC/VersionStorageManager.cs:16` `ConcurrentDictionary<(string, string, long), RowVersion> _versionMetadata` | Keyed by `(databaseName, tableName, byte-offset rowId)` | Must rekey to `(databaseName, tableName, logical seqno)` or be replaced by LSM native MVCC (seqno visibility is native to LSM) |
| `MVCC/VersionStorageManager.cs:27` `AllocateVersion(string databaseName, string tableName, long rowId, long xmin)` | Creates version entry for a byte-offset RowId | Creates version entry for logical seqno |
| `MVCC/VersionStorageManager.cs:55` `AllocateInsertVersions(... IReadOnlyList<long> rowIds ...)` | Batch allocation for byte-offset ids | Batch allocation for logical seqnos |
| `MVCC/VersionStorageManager.cs:74` `GetVersion(string databaseName, string tableName, long rowId)` | Looks up version by byte offset | Looks up by logical seqno |
| `MVCC/VersionStorageManager.cs:93` `MarkVersionObsolete(string databaseName, string tableName, long rowId, long xmax)` | Marks byte-offset version as deleted by xmax | Marks logical seqno version as deleted |
| `MVCC/VersionStorageManager.cs:116` `LinkVersionChain(... long oldRowId, long newRowId)` | Stores `newRowId` (new byte offset) in `VersionChain` field of `oldRowId`'s entry | Must store logical new seqno; **the version chain is entirely offset-based today** |
| `MVCC/VersionStorageManager.cs:139-165` `GetVersionChain(... long startRowId)` | Traverses linked chain via `VersionChain` byte-offset pointers | Traverses linked chain via logical seqno pointers; LSM may encode this natively |
| `MVCC/VersionStorageManager.cs:216,230` `List<(string DatabaseName, string TableName, long RowId)>` in `VacuumTable` | Enumerates (db, table, byte-offset) triples to check physical existence | Enumerates (db, table, logical seqno) triples; existence check calls LSM |

### 3b. RowVersion struct

| Site | Today | LSM migration |
|------|-------|---------------|
| `MVCC/RowVersion.cs:11` `public struct RowVersion` | Stores `xmin`, `xmax`, `versionChain` fields | `versionChain` is a byte-offset RowId pointer; must become logical seqno or be removed if LSM tracks chains natively |
| `MVCC/RowVersion.cs:41` `RowVersion(long xmin, long xmax = 0, long versionChain = 0)` | `versionChain` stores next byte-offset RowId | `versionChain` stores next logical seqno |

### 3c. MvccCoordinator

| Site | Today | LSM migration |
|------|-------|---------------|
| `MVCC/MvccCoordinator.cs:29` `EnsureRowVersionExists(DataVoEngine engine, ..., long rowId)` | Looks up / bootstraps version by byte offset | Uses logical seqno |
| `MVCC/MvccCoordinator.cs:47` `ValidateCanModifyRow(..., long rowId, ...)` | MVCC conflict check keyed by byte offset | Keyed by logical seqno |
| `MVCC/MvccCoordinator.cs:67` `RegisterInsertVersion(..., long rowId, ...)` | Allocates version at byte offset | At logical seqno |
| `MVCC/MvccCoordinator.cs:82` `RegisterInsertVersions(..., IReadOnlyList<long> rowIds, ...)` | Batch allocation at byte offsets | Batch at logical seqnos |
| `MVCC/MvccCoordinator.cs:88-99` `RegisterUpdateVersion(..., long oldRowId, long newRowId, ...)` | Marks old byte offset obsolete; allocates version at new byte offset; links chain | **Critical**: old+new are byte offsets; both must become logical seqnos; chain link updated accordingly |
| `MVCC/MvccCoordinator.cs:105-108` `RegisterDeleteVersion(..., long rowId, ...)` | Marks byte offset obsolete | Marks logical seqno obsolete |

### 3d. SnapshotVisibilityEvaluator (safe)

| Site | Today | LSM migration |
|------|-------|---------------|
| `MVCC/SnapshotVisibilityEvaluator.cs:15,30,45,66,77` `IsVersionVisible / CanUpdateRow / CanDeleteRow / IsRowOrDeletedVisible` | Operates on `RowVersion` struct fields (`xmin`, `xmax`) — never touches `rowId` directly | Safe; no RowId in arguments; operates purely on transaction IDs |
| `MVCC/SnapshotVisibilityEvaluator.cs:94` `foreach (long rowId in rowIds)` | Iterates byte-offset ids to call `GetVersion(... rowId ...)` | Iterates logical seqnos; call chain safe when seqno is consistent |
| `MVCC/TransactionSnapshot.cs:37,39` `CanSee(RowVersion version)` | Delegates to RowVersion; no RowId used | Safe |

### 3e. RowChange and ChangeRecorder

| Site | Today | LSM migration |
|------|-------|---------------|
| `Runtime/Changes/RowChange.cs:23,39,60` constructors `RowChange(string table, long rowId, ...)` | `rowId` = physical byte offset; documented as unstable (update = delete+reinsert assigns new id) | `rowId` = logical seqno; still unstable across updates, but now matches LSM identity |
| `Runtime/Changes/RowChange.cs:82` `public long RowId { get; }` | Physical byte offset exposed to consumers | **Reactive operators are documented not to key on this field** (see XML docs); value changes semantics to logical seqno but no consumer change required |
| `Runtime/Changes/ChangeRecorder.cs:41` `RecordInsert(string table, long rowId, ...)` | Passes byte offset to `RowChange` | Passes logical seqno |
| `Runtime/Changes/ChangeRecorder.cs:49` `RecordTypedInsert(string table, long rowId, ...)` | Same | Same |
| `Runtime/Changes/ChangeRecorder.cs:53` `RecordDelete(string table, long rowId, ...)` | Same | Same |
| `Runtime/Changes/ChangeRecorder.cs:57` `RecordUpdate(string table, long rowId, ...)` | Same | Same |

### 3f. Reactive query Seed methods

All reactive queries implement `IReactiveQuery.Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)`. The `long RowId` in the tuple is used to populate an in-memory match-set during initial seeding. Subsequent `RowChange` events carry RowIds that are cross-referenced against this match-set.

`ReactiveSubscription._matchSet` stores RowIds from Seed and from Insert/Update RowChange events; Delete RowChange events remove RowIds from the set. This is **safe** because:
1. The match-set is in-memory only (not persisted); it is rebuilt via `Seed` on each engine start from whatever RowIds the current storage engine assigns.
2. RowChange events must carry RowIds consistent with what Seed received; this invariant is maintained as long as the storage engine and change recorder use the same RowId semantics (byte offset today, logical seqno after migration).
3. The comment in `Apply()` at line 112-113 explicitly documents robustness to out-of-place updates: decisions are predicate-based on before/after images, and the RowId in the match-set tracks the *current* slot identity (old slot removed on Delete, new slot added on Insert).

| Site | Today | LSM migration |
|------|-------|---------------|
| `Runtime/Reactive/IReactiveQuery.cs:28` Seed interface | `(long RowId, ...)` in seed tuple | Safe — RowId value shifts to logical seqno; Seed rebuilds match-set on each start |
| `Runtime/Reactive/ReactiveSubscription.cs:86,88,92,122,129,140,145,150` | Seed populates `_matchSet`; Apply adds/removes by RowId | **Conditionally safe**: self-consistent within a session; migration note: Delete RowChange must carry the same seqno that was in the match-set at seed time or from a prior Insert RowChange |
| `Runtime/Reactive/RecursiveCteReactiveQuery.cs:129` | Seed override | Safe |
| `Runtime/Reactive/DistinctReactiveQuery.cs:71` | Seed override | Safe |
| `Runtime/Reactive/VipExposureReactiveQuery.cs:54` | Seed override | Safe |
| `Runtime/Reactive/SubqueryReactiveQuery.cs:79` | Seed override | Safe |
| `Runtime/Reactive/JoinReactiveQuery.Engine.cs:49` `SeedSide(...)` | Seed helper | Safe |
| `Runtime/Reactive/JoinReactiveQuery.cs:105` | Seed override | Safe |
| `Runtime/Reactive/AggregateReactiveQuery.cs:135` | Seed override | Safe |
| `Runtime/Reactive/TopKReactiveQuery.cs:120` | Seed override | Safe |
| `Runtime/Reactive/UnionReactiveQuery.cs:73` | Seed override | Safe |

### 3g. DataVoEngine — VersionStorageManager lifecycle (safe)

| Site | Today | LSM migration |
|------|-------|---------------|
| `Runtime/DataVoEngine.cs:54` `VersionStorageManager = new VersionStorageManager()` | Construction | Safe — instantiation |
| `Runtime/DataVoEngine.cs:131` `public VersionStorageManager VersionStorageManager { get; }` | Exposed property | Safe — container |
| `Runtime/DataVoEngine.cs:423` `List<(long RowId, float[] Vector)> vectors = []` inside `RebuildVectorIndexFromRows` | Collects (byte-offset, vector) pairs from existing rows during index rebuild | Collects (logical seqno, vector) pairs; downstream vector index insert changes semantics |
| `Runtime/DataVoEngine.cs:464` `VersionStorageManager.Clear()` | Clears all MVCC state | Safe — structural |
| `Runtime/DataVoEngine.cs:686` `VersionStorageManager.Dispose()` | Releases MVCC state | Safe — structural |

---

## 4. Compiled query fast paths

The compiled query engine (`DataVoCompiledQuery`, `DataVoPreparedSelectSingle`, `DataVoPreparedSelectMany`) uses RowIds obtained from the PK fast lane or B-Tree indexes to directly call `StorageContext.TryReadRowBytes` / `TryReadStoredRow`. These paths bypass the interpreter and thus tightly couple the index-returned value to the storage read API. They are the most performance-sensitive sites and carry byte-offset RowIds across multiple layers within a single statement.

### 4a. DataVoCompiledQuery — SELECT path

| Site | Today | LSM migration |
|------|-------|---------------|
| `CompiledQueries/DataVoCompiledQuery.cs:176` `IReadOnlyList<long>? rowIds = TryResolveMatchingRowIds(...)` | Resolves byte-offset ids from PK fast lane or B-Tree | Resolves logical seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:209` `foreach (long rowId in rowIds)` | Iterates byte offsets | Iterates logical seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:211` `StorageContext.IsRowVisible(plan.TableName, databaseName, rowId)` | MVCC visibility by byte offset | By logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:216` `StorageContext.TryReadStoredRow(plan.TableName, databaseName, rowId, ...)` | Typed read by byte offset | Typed read by logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:223` `StorageContext.TryReadRowBytes(plan.TableName, databaseName, rowId)` | Raw bytes by byte offset | Raw bytes by logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:726` `TryResolveMatchingRowIds(...)` declaration | Central RowId resolution helper | Must return logical seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:863` `foreach ((long rowId, StoredRow row) in scanned)` | Scanned typed rows with byte offsets | Same pattern with logical seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:879` `scannedMatches.Add(new KeyValuePair<long, StoredRow>(rowId, row))` | Pairs byte offset with typed row | Pairs logical seqno with typed row |
| `CompiledQueries/DataVoCompiledQuery.cs:951-970` `RevalidateMatchingRowIdsAfterLock(...)` | Intersects candidate byte-offset set with lock-covered set | Intersects logical seqno sets |

### 4b. DataVoCompiledQuery — UPDATE path (critical: out-of-place update semantics)

The UPDATE fast path performs an explicit delete-old + insert-new to get a new byte-offset RowId. This is the most deeply entangled site because it chains MVCC registration, PK fast lane update, index update, WAL write, and change recording around the old/new byte offset pair.

| Site | Today | LSM migration |
|------|-------|---------------|
| `CompiledQueries/DataVoCompiledQuery.cs:375-413` `ReplaceRows(...)` orchestration | Deletes old byte offsets, inserts new ones, links version chain | **Major change**: LSM does in-place update (new version at same PK); delete+reinsert pattern is eliminated |
| `CompiledQueries/DataVoCompiledQuery.cs:387` `GetTableContents(revalidatedRowIds, ...)` | Fetches current row images by byte offsets | Fetches by logical seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:397,399` MVCC validate in ordered update loop | `ValidateCanModifyRow(..., rowId, ...)` by byte offset | By logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:465` `TryLookupIntegerPrimaryKey(..., out long rowId)` | PK value → byte offset | PK value → logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:471` `AcquireRowWriteLocks(databaseName, plan.TableName, [rowId])` | Row lock keyed by byte offset | Row lock keyed by logical seqno or PK (see §locking) |
| `CompiledQueries/DataVoCompiledQuery.cs:475` `TryLookupIntegerPrimaryKey(...)` retry after lock | Same | Same |
| `CompiledQueries/DataVoCompiledQuery.cs:481` `ValidateCanModifyRow(... rowId ...)` | MVCC by byte offset | By logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:483` `TryReadRowBytes(plan.TableName, databaseName, rowId)` | Raw bytes by byte offset | Raw bytes by logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:505` `DeleteFromTable([rowId], plan.TableName, databaseName)` | Delete by byte offset | Logical delete by seqno (LSM tombstone) |
| `CompiledQueries/DataVoCompiledQuery.cs:506` `long newRowId = InsertSerializedRow(rowBytes, plan.TableName, databaseName)` | Reinsert returns new byte offset | **Eliminated** in LSM: in-place update returns same PK identity; `newRowId` concept goes away |
| `CompiledQueries/DataVoCompiledQuery.cs:508` `MvccCoordinator.RegisterUpdateVersion(... rowId, newRowId, ...)` | Links old byte offset → new byte offset | Links old seqno → new seqno; or merged into LSM native MVCC |
| `CompiledQueries/DataVoCompiledQuery.cs:511` `InsertIntegerPrimaryKeys([(primaryKey, newRowId)], ...)` | Updates fast lane with new byte offset | Updates fast lane with new logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:515` `CommitBinaryUpdateFrameThroughGroupCommit(... rowId, rowBytes)` | WAL frame embeds byte offset | WAL frame embeds logical seqno; see §WAL below |
| `CompiledQueries/DataVoCompiledQuery.cs:1192-1207` `ValidateUpdatedRows(IReadOnlyList<long> rowIds, ...)` | FK/uniqueness re-check by byte-offset ids | By logical seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:1435-1581` `ReplaceRows(IReadOnlyList<long> oldRowIds, ..., long[] newRowIds, ...)` | Full delete-old/insert-new cycle with index maintenance | **Redesign required**: LSM in-place update eliminates the old/new offset duality |
| `CompiledQueries/DataVoCompiledQuery.cs:1443` `DeleteFromTable(oldRowIds.ToList(), ...)` | Delete old byte offsets | Logical delete by seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:1448` `long assignedRowId = InsertOneIntoTable(...)` | Returns new byte offset | Returns new logical seqno (or same PK under LSM) |
| `CompiledQueries/DataVoCompiledQuery.cs:1449,1450` store `newRowIds[i]` / `RegisterUpdateVersion(... oldRowIds[i], assignedRowId, ...)` | Chain old→new byte offsets | Chain old→new logical seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:1455` `MaintainIndexForUpdate(...)` | Updates all indexes with old/new byte offsets | Updates with old/new logical seqnos |
| `CompiledQueries/DataVoCompiledQuery.cs:1467` `RowId = oldRowIds[i]` in ChangeRecorder call | Passes old byte offset to change recorder | Passes old logical seqno |
| `CompiledQueries/DataVoCompiledQuery.cs:1487-1581` `MaintainIndexForUpdate(IReadOnlyList<long> oldRowIds, ..., IReadOnlyList<long> newRowIds, ...)` | Per-index: delete old byte offsets, insert new byte offsets | Per-index: delete old seqnos, insert new seqnos; or LSM handles via compaction |

### 4c. DataVoPreparedSelectSingle (zero-alloc single-row select)

| Site | Today | LSM migration |
|------|-------|---------------|
| `CompiledQueries/DataVoPreparedSelectSingle.cs:71,76` `TryLookupIntegerPrimaryKey(..., out long rowId)` | Gets byte offset from fast lane | Gets logical seqno |
| `CompiledQueries/DataVoPreparedSelectSingle.cs:78` `TryProjectRow(rowId, ...)` | Projects by byte offset | Projects by logical seqno |
| `CompiledQueries/DataVoPreparedSelectSingle.cs:90` `FilterUsingIndex(...)` | Returns byte-offset id list for secondary index path | Returns logical seqno list |
| `CompiledQueries/DataVoPreparedSelectSingle.cs:96,98` iteration over rowIds / `TryProjectRow(rowIds[i], ...)` | Iterates byte offsets | Iterates logical seqnos |
| `CompiledQueries/DataVoPreparedSelectSingle.cs:112` `TryProjectRow(long rowId, ...)` | Projects single row by byte offset | By logical seqno |
| `CompiledQueries/DataVoPreparedSelectSingle.cs:115` `StorageContext.IsRowVisible(... rowId)` | Visibility by byte offset | By logical seqno |
| `CompiledQueries/DataVoPreparedSelectSingle.cs:120` `TryReadStoredRow(... rowId, ...)` | Typed read by byte offset | By logical seqno |
| `CompiledQueries/DataVoPreparedSelectSingle.cs:127` `TryReadRowBytes(... rowId)` | Raw bytes by byte offset | Raw bytes by logical seqno |

### 4d. DataVoPreparedSelectMany (zero-alloc multi-row select)

| Site | Today | LSM migration |
|------|-------|---------------|
| `CompiledQueries/DataVoPreparedSelectMany.cs:59,61,62,64` `TryMatchWithIndex(... out IReadOnlyList<long> rowIds)` and iteration | Byte-offset id list from index | Logical seqno list |
| `CompiledQueries/DataVoPreparedSelectMany.cs:82,88,89,91` `FilterUsingIndex(...)` and iteration | Same | Same |
| `CompiledQueries/DataVoPreparedSelectMany.cs:107` `TryProjectRow(long rowId, ...)` | Projects by byte offset | By logical seqno |
| `CompiledQueries/DataVoPreparedSelectMany.cs:110` `IsRowVisible(... rowId)` | Visibility by byte offset | By logical seqno |
| `CompiledQueries/DataVoPreparedSelectMany.cs:115` `TryReadStoredRow(... rowId, ...)` | Typed read by byte offset | By logical seqno |
| `CompiledQueries/DataVoPreparedSelectMany.cs:122` `TryReadRowBytes(... rowId)` | Raw bytes by byte offset | By logical seqno |

---

## 5. Snapshot / restore / catalog rebuild

These sites are DML, DDL, and transaction-commit paths that call `GetTableContents`, `CompactTable`, and related methods, or that manage RowIds across table rewrites.

### 5a. Interpreter SELECT path

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/DQL/Select.cs:584` `foreach (long rowId in rowIds)` | Iterates byte-offset ids from B-Tree lookup in interpreter SELECT | Iterates logical seqnos |
| `Parser/DQL/Select.cs:728` `foreach (long rowId in rowIds)` | Same — second SELECT code path | Same |
| `Parser/DQL/Select.cs:867` `foreach (long rowId in candidateRowIds)` | Iterates candidate byte offsets during join/filter | Iterates logical seqnos |

### 5b. InsertRowService

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/DML/InsertRowService.cs:103` `long rowId = rowIds[i]` | Extracts byte offset from batch insert result for index registration | Extracts logical seqno |
| `Parser/DML/InsertRowService.cs:207` `long rowId = context.InsertTypedRow(...)` | Gets byte offset from typed insert for index and MVCC registration | Gets logical seqno |
| `Parser/DML/InsertRowService.cs:1062,1106` `long rowId,` parameters in index entry builder methods | Byte offset plumbed through to index insert | Logical seqno |
| `Parser/DML/InsertRowService.cs:1195,1205` `List<(string Value, long RowId)>` / `List<(long Key, long RowId)>` entry lists | Pairs index key with byte-offset RowId | Pairs with logical seqno |
| `Parser/DML/InsertRowService.cs:1215,1245,1253` `TryBuildIntegerIndexEntries(... out List<(long Key, long RowId)>? entries)` | Builds (integer key, byte offset) pairs for bulk index insert | Builds (integer key, logical seqno) pairs |
| `Parser/DML/InsertRowService.cs:974,1023,1043` `context.GetTableContents(tableName/reference, databaseName)` | Reads all rows (keyed by byte offset) for UNIQUE/FK constraint checks | Reads all rows keyed by logical seqno; check logic unchanged |

### 5c. DeleteFrom

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/DML/DeleteFrom.cs:40,63,201,210,234` `foreach (long rowId in toBeDeleted/revalidatedRowIds)` | Iterates byte-offset ids for physical deletion and FK cascade | Iterates logical seqnos; physical delete becomes LSM tombstone |
| `Parser/DML/DeleteFrom.cs:42,65` `MvccCoordinator.ValidateCanModifyRow(... rowId ...)` | MVCC check by byte offset | MVCC check by logical seqno |
| `Parser/DML/DeleteFrom.cs:151,209,253` `Context.GetTableContents(toBeDeleted/childTable, ...)` | Reads rows by byte-offset id list | Reads rows by logical seqno list |
| `Parser/DML/DeleteFrom.cs:169` `FilterUsingIndex(...)` (FK child lookup) | Returns byte-offset ids of child rows | Returns logical seqnos |

### 5d. Update

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/DML/Update.cs:61,103` `foreach (long rowId in toBeUpdated/oldRowIds)` | Iterates byte-offset ids for UPDATE and FK cascade recheck | Iterates logical seqnos |
| `Parser/DML/Update.cs:63,105` `MvccCoordinator.ValidateCanModifyRow(... rowId ...)` | MVCC check by byte offset | By logical seqno |
| `Parser/DML/Update.cs:467` `MvccCoordinator.RegisterUpdateVersion(... oldRowId, assignedRowId, ...)` | Old→new byte offset version chain | Old→new logical seqno chain |
| `Parser/DML/Update.cs:405` `FilterUsingIndex(...)` (FK parent check) | Returns byte-offset ids of dependent rows | Returns logical seqnos |

### 5e. DDL — ALTER TABLE column changes

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/DDL/AlterTableModifyColumn.cs:27` `Context.GetTableContents(tableName, databaseName)` | Reads all rows (keyed by byte offset) for type-conversion pre-pass | Reads keyed by logical seqno; `.OrderBy(row => row.Key)` ordering semantics change (byte offset was insertion-order; logical seqno is also insertion-ordered but different values) |
| `Parser/DDL/AlterTableAddColumn.cs:25` `Context.GetTableContents(tableName, databaseName)` | Reads all rows for backfill | Reads keyed by logical seqno |
| `Parser/DDL/AlterTableDropColumn.cs:26` `Context.GetTableContents(tableName, databaseName)` | Same | Same |

### 5f. DDL — CREATE INDEX

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/DDL/CreateIndex.cs:64` `Context.GetTableContents(_model.TableName, databaseName)` | Reads all rows keyed by byte offset to build initial index | Reads keyed by logical seqno |
| `Parser/DDL/CreateIndex.cs:79` `List<(long RowId, float[] Vector)> vectors = []` | Collects (byte-offset, vector) pairs for vector index seeding | Collects (logical seqno, vector) pairs |

### 5g. VACUUM

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/DML/Vacuum.cs:54` `Context.CompactTable(tableName, databaseName, allowIndexedCompaction: true)` | Rewrites .dat removing tombstones; returns (newByteOffset, rawRow) pairs | LSM compaction is asynchronous background merge; VACUUM becomes a hint or force-merge; returned pairs use logical seqnos |
| `Parser/DML/Vacuum.cs:68` `foreach (var (newRowId, rawRow) in compactedRows)` | Rebuilds all B-Tree and vector indexes using new byte offsets | Rebuilds indexes using new logical seqnos |
| `Parser/DML/Vacuum.cs:81` `indexData[indexKey].Add(newRowId)` | Maps index key to new byte offset | Maps to new logical seqno |

### 5h. Transaction COMMIT flush

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/Transactions/Commit.cs:84` `VersionStorageManager.VacuumTable(databaseName, storageContext)` | Prunes MVCC entries for physically deleted byte offsets | Prunes entries for logically deleted seqnos |
| `Parser/Transactions/Commit.cs:123` `context.UpdatedRows.TryGetValue(...)` | Buffered `(long RowId, Dictionary<...> UpdatedColumns)` per table | `(long LogicalSeqno, ...)` — buffer structure unchanged but semantics shift |
| `Parser/Transactions/Commit.cs:196,205` `foreach (long rowId in rowIdList)` / MVCC + recorder calls | Delete flush iterates byte offsets | Iterates logical seqnos |
| `Parser/Transactions/Commit.cs:198` `ValidateCanModifyRow(... rowId ...)` | MVCC by byte offset | By logical seqno |
| `Parser/Transactions/Commit.cs:204` `GetTableContents(rowIdList, tableName, databaseName)` | Reads rows by byte-offset list for change recorder | Reads by logical seqno list |
| `Parser/Transactions/Commit.cs:236` `ValidateCanModifyRow(...)` in update flush | By byte offset | By logical seqno |
| `Parser/Transactions/Commit.cs:239` `GetTableContents([rowId], ...)` in update flush | Reads single row by byte offset | By logical seqno |
| `Parser/Transactions/Commit.cs:256` `RegisterUpdateVersion(... rowId, newRowId, ...)` | Old→new byte offset | Old→new logical seqno |

### 5i. TransactionContext — buffered DML state

| Site | Today | LSM migration |
|------|-------|---------------|
| `Transactions/TransactionContext.cs:22,51` `Dictionary<string, List<(long RowId, Dictionary<string, object?> UpdatedColumns)>> UpdatedRows` | Buffers (byte-offset, column-delta) pairs for deferred UPDATE flush | Buffers (logical seqno, column-delta) pairs |
| `Transactions/TransactionContext.cs:71` `BufferDelete(string tableName, long rowId)` | Adds byte offset to pending delete set | Adds logical seqno |
| `Transactions/TransactionContext.cs:85` `BufferUpdate(string tableName, long rowId, ...)` | Adds (byte-offset, columns) to pending update list | Adds (logical seqno, columns) |
| `Transactions/TransactionContext.cs:183,213-217` `CloneUpdatedRows(...)` | Deep-copies the (byte-offset, columns) buffer | Deep-copies (logical seqno, columns) buffer |

### 5j. WAL appender

| Site | Today | LSM migration |
|------|-------|---------------|
| `Transactions/WalAppender.cs:50` `long RowId,` in `WalFrameHeader` record | WAL frame header embeds byte-offset RowId at byte 20-27 | Must embed logical seqno; **on-disk WAL format change** — recovery reader must be updated to match |
| `Transactions/WalAppender.cs:78` `Reserve(WalFrameOperationType opType, int tableId, long rowId, ...)` | Accepts byte-offset RowId; writes it at byte 20 of frame | Accepts logical seqno |
| `Transactions/WalAppender.cs:271` `long rowId,` in `WalFrameReservation` constructor | Stores byte offset in reservation | Stores logical seqno |
| `Transactions/WalAppender.cs:295` `internal long RowId { get; }` | Byte-offset RowId property on reservation | Logical seqno property |

### 5k. LockManager

| Site | Today | LSM migration |
|------|-------|---------------|
| `Transactions/LockManager.cs:132` `AcquireRowReadLock(string databaseName, string tableName, long rowId)` | Lock key built from byte offset: `{db}:{table}:{rowId}` | Lock key must use logical identity (PK or seqno); byte-offset-keyed locks would be invalid after LSM reassignment |
| `Transactions/LockManager.cs:167` `AcquireRowWriteLock(string databaseName, string tableName, long rowId)` | Same | Same |
| `Transactions/LockManager.cs:202` `ReleaseRowReadLock(string databaseName, string tableName, long rowId)` | Same | Same |
| `Transactions/LockManager.cs:221` `ReleaseRowWriteLock(string databaseName, string tableName, long rowId)` | Same | Same |
| `Transactions/LockManager.cs:488` `BuildRowKey(string databaseName, string tableName, long rowId)` | `$"{databaseName}.{tableName}#row:{rowId}"` — byte offset in key string | Must become `$"{databaseName}.{tableName}#row:{pk}"` or equivalent PK-based key |

### 5l. DataVoContext (application API level)

| Site | Today | LSM migration |
|------|-------|---------------|
| `DataVoContext.cs:308` `long rowId = service.InsertTypedRow(...)` | **Leaks above the seam**: byte offset returned from storage is captured at the DataVoContext API level | DataVoContext should not need the physical identity; this site should be reviewed — rowId is used for index registration and MVCC, both of which should be encapsulated inside InsertTypedRow |
| `DataVoContext.cs:509` `Engine.StorageContext.GetTableContents(rowIds, tableName, databaseName)` | Passes byte-offset list to GetTableContents | Passes logical seqno list; seqnos obtained from calling context |

### 5m. Model utility types

| Site | Today | LSM migration |
|------|-------|---------------|
| `Models/Statement/Utils/TableData.cs:4` doc comment "keyed by their physical long RowId" | `Dictionary<long, Record>` keyed by byte offset | Rekeyed by logical seqno; doc comment updated |
| `Models/Statement/Utils/TableData.cs:6` `public class TableData : Dictionary<long, Record>` | `long` key = byte offset | `long` key = logical seqno; type unchanged but semantics shift |
| `Models/Statement/Utils/Record.cs:8` `public class Record(long rowId, ...)` | Carries byte offset | Carries logical seqno |
| `Models/Statement/Utils/Record.cs:13` `public long RowId { get; set; } = rowId` | Exposes byte offset | Exposes logical seqno |
| `Models/Statement/Utils/TableDetails.cs:159` `DataVoEngine.Current().StorageContext.GetTableContents(TableName, DatabaseName)` | Reads all rows keyed by byte offset | Reads keyed by logical seqno |

### 5n. Parser JOIN utilities

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/Types/JoinedRowId.cs:50` `JoinedRowId Append(long rowId)` | Appends constituent byte-offset RowId to composite join identity | Appends logical seqno; composite key semantics preserved |
| `Parser/Types/JoinedRowId.cs:61` `JoinedRowId Prepend(long rowId)` | Prepends byte offset | Prepends logical seqno |

### 5o. Volcano execution layer — synthetic RowIds (NOT physical byte offsets)

These three sites assign a `long rowId` starting at `1` as a synthetic incrementing counter. They are **not** physical byte offsets and do not interact with `IStorageEngine`. They carry no migration burden.

| Site | Today | LSM migration |
|------|-------|---------------|
| `Parser/DQL/Select.Cte.cs:39` `long rowId = 1` | Synthetic counter for CTE materialization; gives each result-set row a temporary dictionary key | No change needed — counter has no relationship to storage identity |
| `Parser/DQL/Select.WindowHaving.cs:86` `long rowId = 1` | Synthetic counter for window function operator | No change needed |
| `Execution/Volcano/HashAggregateOperator.cs:180` `long rowId = 1` | Synthetic counter for aggregate grouping rows | No change needed |

### 5p. Volcano ExecutionRow and TypedExecutionRow (carries rowId from storage)

| Site | Today | LSM migration |
|------|-------|---------------|
| `Execution/Volcano/ExecutionRow.cs:11,20` `ExecutionRow(long rowId, ...)` / `public long RowId { get; }` | Carries byte-offset RowId from storage scan for use in update/delete operators | Must carry logical seqno; operators that call `DeleteFromTable` or `InsertSerializedRow` pass this through |
| `Execution/Volcano/TypedExecutionRow.cs:13,22` `TypedExecutionRow(long rowId, ...)` / `public long RowId { get; }` | Same | Same |

---

## 6. Verdict

### Sites safe behind the IStorageEngine seam — no caller-side change required

These sites are either within `IStorageEngine`/`StorageContext`/backend wrapper implementations (which **are** the migration target), or they pass RowIds through transiently within a single statement without persisting them as durable identities external to the storage layer.

1. `IStorageEngine.cs` — the interface definition; replace the implementation
2. `ITypedRowStorageEngine.cs` — replace the implementation
3. `DiskStorageEngine.cs` — the entire disk backend is replaced by `LsmStorageEngine`
4. `InMemoryStorageEngine.cs` — replaced by LSM in-memory variant
5. `DiskStorageBackend.cs`, `WasmStorageBackend.cs`, `InMemoryStorageBackend.cs` — thin delegation; change the delegatee
6. `StorageContext.cs` (internal read/write methods) — safe; the methods receive opaque `long` ids and delegate to `IStorageEngine`; once the engine returns logical seqnos, this layer adapts transparently
7. `Exceptions/RowDeletedException.cs`, `Exceptions/RowNotFoundException.cs` — carry RowId for diagnostics only; no semantic use
8. `Runtime/Reactive/IReactiveQuery.cs` and all `Seed` implementations — RowId populates in-memory match-sets rebuilt on each session start via Seed; safe as long as RowChange events carry consistent logical seqnos. `ReactiveSubscription._matchSet` stores RowIds transiently but self-corrects on each event.
9. `Runtime/Changes/RowChange.cs:RowId` — explicitly documented as unstable; reactive operators use before/after row images for predicate evaluation and do not key identity on RowId
10. `MVCC/SnapshotVisibilityEvaluator.cs` — operates on `RowVersion.xmin/xmax` fields (transaction IDs), never on `rowId` directly
11. `MVCC/TransactionSnapshot.cs` — delegates to RowVersion fields; no RowId
12. `Parser/DQL/Select.Cte.cs:39`, `Parser/DQL/Select.WindowHaving.cs:86`, `Execution/Volcano/HashAggregateOperator.cs:180` — synthetic `rowId = 1` counters; not physical byte offsets

### Sites that leak the offset above the seam — must change for Plan 5

These sites store, persist, or forward the byte-offset RowId outside the `IStorageEngine`/`StorageContext` boundary.

1. **`IndexManager._integerPrimaryKeyMaps`** — `(integer PK) → (byte-offset RowId)` map; must become `(integer PK) → (logical seqno)`. This is the primary index fast lane and is consulted on every indexed read.

2. **`IndexManager.FilterUsingIndex` return contract** — returns byte-offset ids today; once migrated, returns logical seqnos. All callers (§2e) must treat the return value as a logical handle without arithmetic assumptions.

3. **All B-Tree index value slots** (`IIndex.Insert`, `BinaryBTreeIndex`, `BinaryBPlusTreeIndex`, `JsonBTreeIndex`) — store byte-offset RowIds as B-Tree values; must store logical seqnos. **On-disk B-Tree serialization** must be migrated or rebuilt.

4. **All vector index ordinal maps** (`FlatVectorIndex`, `HNSWIndex`, `BrowserFallbackVectorIndex`, `HNSWIndexPersistence`) — `long rowId → ordinal` mapping uses byte offsets; must use logical seqnos. **`HNSWIndexPersistence.RowId` is serialized to disk** — breaking change to the HNSW persistence format.

5. **`VersionStorageManager._versionMetadata`** — keyed by `(db, table, byte-offset rowId)`; must rekey to `(db, table, logical seqno)`. The `VersionChain` field in each `RowVersion` stores the next byte-offset RowId as a forward pointer; must store next logical seqno.

6. **`MvccCoordinator.RegisterUpdateVersion(... long oldRowId, long newRowId ...)`** — encodes old/new byte offsets; must encode old/new logical seqnos. In the LSM, in-place update semantics may allow simplification (same PK across versions).

7. **`LockManager.BuildRowKey(... long rowId ...)` and all `Acquire/Release*` methods** — lock key includes byte offset; must use PK or logical seqno so lock identity survives the storage layer change.

8. **`WalAppender.WalFrameHeader.RowId` and `Reserve(... long rowId ...)`** — byte offset embedded in WAL frame binary at offset 20; **on-disk WAL format change** requiring recovery reader update.

9. **`TransactionContext.UpdatedRows: Dictionary<string, List<(long RowId, ...)>>`** — buffers byte offsets for deferred UPDATE flush; must buffer logical seqnos.

10. **`CompiledQueries/DataVoCompiledQuery.cs` UPDATE fast path (§4b)** — the delete-old/reinsert-new pattern is tightly coupled to byte-offset identity. LSM in-place update eliminates this; the entire `ReplaceRows` / `MaintainIndexForUpdate` pattern must be redesigned around LSM update semantics.

11. **`Models/Statement/Utils/Record.cs` and `TableData.cs`** — `Record.RowId` and the `Dictionary<long, Record>` key surface physical byte offsets to the parser layer; must surface logical seqnos.

12. **`DataVoContext.cs:308`** — `long rowId = service.InsertTypedRow(...)` leaks the storage-assigned RowId up to the public API context; consider encapsulating MVCC and index registration inside `InsertTypedRow` to avoid surfacing the identity at this level.

13. **`Parser/Types/JoinedRowId.Append/Prepend(long rowId)`** — composite join keys built from per-table byte offsets; must be built from per-table logical seqnos. The `JoinedRowId` is used for join result identity within a single query execution, so the migration is contained to how each table's RowId is sourced.

14. **`Execution/Volcano/ExecutionRow.RowId` and `TypedExecutionRow.RowId`** — these carry byte offsets sourced from storage scans and pass them to update/delete operators; must carry logical seqnos.

### Recommended migration order for Plan 5

1. **Define the LSM seqno contract** — specify whether `long` remains the RowId type (as an opaque logical seqno) or is replaced by a value type `(long Pk, long Seqno)`. A transparent `long` type maximizes diff surface reduction; a struct would be cleaner but requires touching all signatures.

2. **Migrate IStorageEngine + StorageContext** — implement `LsmStorageEngine : IStorageEngine` returning logical seqnos. All of §1 is resolved.

3. **Migrate IndexManager + B-Tree + vector indexes** — update all `(key) → (rowId)` maps and `Insert(key, rowId)` implementations to use logical seqnos (§2). Rebuild on-disk B-Tree and HNSW persistence formats.

4. **Migrate MVCC layer** — rekey `VersionStorageManager._versionMetadata` and `RowVersion.VersionChain` to logical seqnos (§3). Evaluate whether LSM-native MVCC via seqno visibility can replace `VersionStorageManager` entirely.

5. **Migrate LockManager** — update `BuildRowKey` and all lock methods to use PK-based key strings (§5k).

6. **Migrate WAL frame format** — update `WalFrameHeader`, `WalAppender.Reserve`, and the recovery reader to use logical seqnos (§5j).

7. **Migrate CompiledQuery UPDATE paths** — redesign `ReplaceRows` / `MaintainIndexForUpdate` around LSM in-place update semantics, eliminating the delete-old/reinsert-new pattern (§4b).

8. **Migrate TransactionContext** — update `UpdatedRows` buffer to carry logical seqnos (§5i).

9. **Migrate model utilities and interpreter** — update `Record`, `TableData`, `ExecutionRow`, `TypedExecutionRow`, `JoinedRowId`, `Select.cs` loops, and `StatementEvaluator` to use logical seqnos (§5m–§5p, §5a).

10. **Verify change-capture and reactive layer** — confirm `RowChange.RowId` consumers still satisfy their documented invariant (key on PK, not RowId) under logical seqno semantics (§3e–§3f).
