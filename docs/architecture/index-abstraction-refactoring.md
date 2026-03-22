# Index Abstraction Refactoring

> Comprehensive plan to separate index implementations for scalability and maintainability
> Status: DESIGN
> Priority: HIGH (blocking future algorithms: B25, hybrid indices, etc.)

## Current Architecture Problems

### 1. HNSW Hardcoded in IndexManager

```csharp
// ❌ Bad: Special-cased in IndexManager
private Dictionary<string, VectorIndexSnapshot> _vectorCache = [];

public void CreateVectorIndex(...) { ... }
public List<long> SearchVector(...) { ... }
public void InsertIntoVectorIndex(...) { ... }
```

- HNSW methods mixed with BTree methods
- Serialization logic (JSON) tightly coupled
- No abstraction for "what is an index?"
- Can't add B25, graph indices, etc. without more special cases

### 2. BTree Uses IIndex, HNSW Doesn't

```csharp
// ✅ Good abstraction for BTree
public interface IIndex
{
    void Insert(string value, long rowId);
    HashSet<long> Search(string value);
    bool ContainsValue(long rowId);
    void DeleteValues(List<long> rowIds);
}

// ❌ HNSW is NOT an IIndex
// It bypasses the abstraction entirely
```

### 3. Storage Backend is Implicit

- `RowSerializer` knows about VECTOR type
- Disk vs. memory hardcoded in `StorageContext`
- No way to plug in WASM origin private file system
- Browser support will require invasive changes

## Proposed Architecture

### Layer 1: Index Abstraction

```
DataVo.Core.Indexing/
├── IIndex.cs                 (shared interface for all indices)
├── IIndexPersistence.cs      (abstraction for serialization)
├── IndexManager.cs           (factory + coordinator, refactored)
└── IndexMetadata.cs          (unified metadata for all types)
```

**IIndex** (unified interface for all index types):

```csharp
public interface IIndex
{
    string IndexType { get; }           // "BTREE", "HNSW", "B25"
    void Insert(object key, long rowId);
    HashSet<long> Search(object key);
    List<long> SearchTopK(object query, int k);  // NEW: for approximate indices
    bool ContainsValue(long rowId);
    void DeleteValues(List<long> rowIds);
    void Clear();
}
```

**IIndexPersistence** (abstraction for storage):

```csharp
public interface IIndexPersistence
{
    void Save(IIndex index, string filePath);
    IIndex Load(string filePath, IndexMetadata metadata);
}
```

### Layer 2: Concrete Implementations

```
DataVo.Core.Indexing.BTree/
├── BTreeIndex.cs            (adapts existing BTree → IIndex)
├── BTreeIndexPersistence.cs (handles JSON serialization)
└── BTreeIndexFactory.cs     (creates BTree instances)

DataVo.Core.Indexing.HNSW/
├── HNSWIndex.cs             (adapts HNSW search → IIndex)
├── HNSWIndexPersistence.cs  (JSON snapshot serialization)
├── HNSWIndexFactory.cs      (creates HNSW instances)
└── HNSWNode.cs              (graph node structure)
```

### Layer 3: Index Manager (Unified)

```csharp
public class IndexManager
{
    private readonly Dictionary<string, IIndex> _cache = [];
    private readonly Dictionary<string, IIndexPersistence> _persistenceStrategies = [];
    
    public void CreateIndex(IndexFile metadata, string indexType, 
                           object[] initialData, string ...params)
    {
        IIndex index = _CreateIndexByType(indexType, metadata, initialData);
        _cache[cacheKey] = index;
        _PersistIndex(index, indexType);
    }
    
    private IIndex _CreateIndexByType(string indexType, IndexFile metadata, object[] data)
    {
        return indexType.ToUpper() switch
        {
            "BTREE" => new BTreeIndexFactory().Create(metadata, data),
            "HNSW" => new HNSWIndexFactory().Create(metadata, data),
            _ => throw new NotSupportedException($"Unknown index type: {indexType}")
        };
    }
}
```

## Phase-by-Phase Implementation

### Phase 1: Abstraction Layer (2 commits)

#### Commit 1a: Create IIndex interface + metadata
- Add `DataVo.Core/Indexing/IIndex.cs`
- Add `DataVo.Core/Indexing/IIndexPersistence.cs`
- Add `DataVo.Core/Indexing/IndexMetadata.cs` (unified metadata)
- Update `IndexFile.cs` to include `IndexType` if missing

#### Commit 1b: Create BTree adapter
- Add `DataVo.Core/Indexing.BTree/BTreeIndex.cs` wrapping existing `BTreeNode`
- Add `DataVo.Core/Indexing.BTree/BTreeIndexFactory.cs`
- Add `DataVo.Core/Indexing.BTree/BTreeIndexPersistence.cs` (delegates to existing code)

### Phase 2: HNSW Refactor (2 commits)

#### Commit 2a: Extract HNSW to separate implementation
- Add `DataVo.Core/Indexing.HNSW/HNSWIndex.cs` (implements IIndex)
- Add `DataVo.Core/Indexing.HNSW/HNSWIndexFactory.cs`
- Add `DataVo.Core/Indexing.HNSW/HNSWIndexPersistence.cs`
- Move `VectorIndexSnapshot` to HNSW namespace

#### Commit 2b: Update IndexManager to use factory pattern
- Refactor `IndexManager` to delegate to `IIndexPersistence` per type
- Add type routing: `"BTREE" → BTreeFactory`, `"HNSW" → HNSWFactory`
- Remove special-cased `_vectorCache`, use unified `_cache`

### Phase 3: Validation (1 commit)

#### Commit 3: Update tests + build
- Verify existing tests still pass
- Add tests for factory pattern

### Phase 4: Future-Proofing (planning only)

For B25, Graph, Hybrid indices:
- Create `DataVo.Core/Indexing.B25/B25Index.cs`
- Create factory, persistence
- Register in `IndexManager` type switch
- One-line additions; no core changes

## Storage Backend Abstraction (Related)

While refactoring indices, also plan storage backend abstraction:

```
DataVo.Core.StorageEngine.Abstractions/
├── IStorageBackend.cs       (Read/Write row data)
└── IStorageTransaction.cs   (Transaction support)

DataVo.Core.StorageEngine.InMemory/
└── InMemoryStorageBackend.cs

DataVo.Core.StorageEngine.Disk/
└── DiskStorageBackend.cs

DataVo.Core.StorageEngine.WASM/
└── WasmStorageBackend.cs    (origin private file system)
```

This allows:
- Pluggable storage without changing query engine
- WASM support by registering different backend
- Testing with mock backends

## Impact on Vector WHERE Predicate Work

The WHERE predicate extraction (`ExpressionExtractor`) will:
- **NOT change** - works at SQL semantic level
- Integrate better once HNSW is a proper `IIndex`
- Can call `SearchTopK()` on any index type

## Timeline

- **Phase 1-2**: 4-5 hours (refactoring with commits)
- **Phase 3**: 1 hour (validation)
- **Phase 4**: 1 hour (planning B25, hybrid future work)

**Total**: ~6 hours for production-ready index abstraction

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Breaking existing code | Run full test suite after each phase |
| Performance regression | Benchmark before/after (IIndex call overhead minimal) |
| Incomplete HNSW extraction | Carefully verify SearchTopK() semantic equivalence |
| Storage backend incompatibility | Don't change StorageEngine in this PR (Phase 4) |

## Files to Delete (Cleanup)

After refactoring:
- `DataVo.Core/BTree/IndexKeyEncoder.cs` → move to BTree namespace
- `DataVo.Core/BTree/IndexManager.cs` → split and move to appropriate namespaces

## Rollout Plan

1. **Week 1**: Phase 1-2 (abstraction + HNSW refactor)
2. **Week 2**: Phase 3 (validation + docs)
3. **Week 3**: Phase 4 (future algorithms planning)

This lays groundwork for:
- B25 indices for range queries
- Hybrid vector+scalar indices
- Custom user-defined index types
- WASM browser support

---

## Next Decision Point

**Should we implement Phase 1-3 now (6 hours), or document the plan for later?**

Recommendation: **Implement now** - blocks vector+WHERE work until this is clean, affects storage layer architecture for WASM support, and prevents technical debt accumulation.
