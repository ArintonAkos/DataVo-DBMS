# Unity Slice A: Core Vector Interop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Add Unity-independent, allocation-aware vector ownership, copying, and nearest-search APIs to DataVo.Core, with identical results for array and stack-backed span callers.

**Architecture:** CellValue keeps its one owned float[] representation but adds safe span creation and copying APIs. DataVoContext and IndexManager add span overloads that use an internal ISpanVectorIndex capability; Flat and HNSW consume the caller span directly, while an index without that capability receives exactly one isolated array fallback.

**Tech Stack:** C# 13, .NET 10, explicit netstandard2.1 portable target, xUnit 2.9.3, DataVo Flat and HNSW indexes.

## Global Constraints

- DataVo.Core remains Unity-free and targets net10.0 by default; DataVoEnablePortableTarget=true additionally compiles netstandard2.1.
- CellValue.From(float[]), CellValue.AsVector(), IVectorIndex, and existing array SearchNearest APIs stay source- and binary-compatible.
- CellValue.FromVector(ReadOnlySpan<float>) owns exactly one copied array.
- CellValue.TakeVectorOwnership(float[]) stores the supplied non-null array without cloning; callers must not mutate it after transfer.
- CellValue.CopyVectorTo(Span<float>) copies exactly VectorLength values and never exposes the stored array.
- Span search never retains the caller span. Flat and HNSW receive it through the internal capability; other indexes receive queryVector.ToArray() only at the manager boundary.
- Every behavior change follows red-green-refactor. HNSW quality/default work remains out of scope.
- Do not add Unity, Burst, Collections, Unity.Mathematics, or unofficial Unity NuGet dependencies to existing projects.

---

## File Structure

| File | Responsibility |
| --- | --- |
| DataVo.Core/Runtime/Reactive/CellValue.cs | Public vector span creation, ownership transfer, length, and safe copy APIs. |
| DataVo.Tests/Reactive/CellValueVectorTests.cs | Public API and error-contract tests. |
| DataVo.Core/Indexing/ISpanVectorIndex.cs | Internal direct span insert and top-K capability. |
| DataVo.Core/Indexing/Flat/FlatVectorIndex.cs | Array wrapper and direct span search. |
| DataVo.Core/Indexing/HNSW/HNSWIndex.cs | Array wrapper and direct span search. |
| DataVo.Core/Indexing/IndexManager.cs | Span dispatch and one-copy fallback. |
| DataVo.Core/DataVoContext.cs | Public span nearest-search API sharing current diagnostics/materialization behavior. |
| DataVo.Tests/Indexing/FlatVectorIndexTests.cs | Flat stack-span regression coverage. |
| DataVo.Tests/Indexing/HNSWIndexTests.cs | HNSW stack-span regression coverage. |
| DataVo.Tests/E2E/VectorContextTests.cs | Context stack-span coverage through both index types. |

## Task 1: Public CellValue vector interop API

**Files:**
- Modify: DataVo.Tests/Reactive/CellValueVectorTests.cs
- Modify: DataVo.Core/Runtime/Reactive/CellValue.cs

**Interfaces:**
- Produces CellValue.FromVector(ReadOnlySpan<float> values).
- Produces CellValue.TakeVectorOwnership(float[] values).
- Produces public int CellValue.VectorLength and void CellValue.CopyVectorTo(Span<float> destination).
- Preserves internal FromVectorOwned(float[]? value) and AsVectorReadOnlySpan().

- [ ] **Step 1: Write the failing public API tests**

Append these tests to DataVo.Tests/Reactive/CellValueVectorTests.cs:

    [Fact]
    public void FromVector_CopiesSpan_AndCopyVectorToPreservesDestinationTail()
    {
        float[] source = [1f, 2f, 3f];
        CellValue cell = CellValue.FromVector(source.AsSpan());
        source[0] = 99f;

        float[] destination = [-1f, -1f, -1f, -1f];
        cell.CopyVectorTo(destination);

        Assert.Equal(CellType.Vector, cell.Type);
        Assert.Equal(3, cell.VectorLength);
        Assert.Equal(new[] { 1f, 2f, 3f, -1f }, destination);
    }

    [Fact]
    public void TakeVectorOwnership_StoresTransferredBuffer_AndAsVectorRemainsDefensive()
    {
        float[] transferred = [4f, 5f, 6f];
        CellValue cell = CellValue.TakeVectorOwnership(transferred);

        float[] defensiveRead = cell.AsVector();
        defensiveRead[1] = 99f;

        float[] destination = new float[cell.VectorLength];
        cell.CopyVectorTo(destination);
        Assert.Equal(new[] { 4f, 5f, 6f }, destination);
    }

    [Fact]
    public void VectorInterop_RejectsNullOwnership_NonVectorLength_AndUndersizedDestination()
    {
        Assert.Throws<ArgumentNullException>(() => CellValue.TakeVectorOwnership(null!));

        CellValue scalar = CellValue.From(42);
        Assert.Throws<InvalidOperationException>(() => _ = scalar.VectorLength);
        Assert.Throws<InvalidOperationException>(() => scalar.CopyVectorTo(new float[1]));

        CellValue vector = CellValue.FromVector([1f, 2f, 3f]);
        Assert.Throws<ArgumentException>(() => vector.CopyVectorTo(new float[2]));
    }

- [ ] **Step 2: Run the focused tests and verify they fail for missing symbols**

Run: dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "FullyQualifiedName~CellValueVectorTests"

Expected: compilation fails because FromVector, TakeVectorOwnership, public VectorLength, and CopyVectorTo do not exist.

- [ ] **Step 3: Write the minimal CellValue implementation**

Add these members beside From(float[]?) in DataVo.Core/Runtime/Reactive/CellValue.cs, then replace the internal VectorLength property with the public version below:

    /// <summary>Creates a VECTOR cell by taking one owned copy of values.</summary>
    public static CellValue FromVector(ReadOnlySpan<float> values) =>
        new(CellType.Vector, 0L, 0m, values.ToArray());

    /// <summary>
    /// Creates a VECTOR cell that owns values without cloning it.
    /// The caller must not mutate the array after this call.
    /// </summary>
    public static CellValue TakeVectorOwnership(float[] values)
    {
        ThrowHelper.ThrowIfNull(values);
        return new CellValue(CellType.Vector, 0L, 0m, values);
    }

    /// <summary>Gets the number of components in this VECTOR cell.</summary>
    public int VectorLength =>
        _type == CellType.Vector ? ((float[])_reference!).Length : throw Mismatch(CellType.Vector);

    /// <summary>Copies vector components into destination without exposing stored state.</summary>
    public void CopyVectorTo(Span<float> destination)
    {
        ReadOnlySpan<float> source = AsVectorReadOnlySpan();
        if (destination.Length < source.Length)
        {
            throw new ArgumentException("Destination is shorter than the vector length.", nameof(destination));
        }

        source.CopyTo(destination);
    }

Keep FromVectorOwned(float[]? value) unchanged: its internal null-to-Null behavior is used by existing engine code. Keep AsVectorReadOnlySpan() internal as the no-clone engine fast path.

- [ ] **Step 4: Run the focused tests and verify they pass**

Run: dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "FullyQualifiedName~CellValueVectorTests"

Expected: every CellValueVectorTests case passes.

- [ ] **Step 5: Commit the tested API boundary**

Run:

    git add DataVo.Core/Runtime/Reactive/CellValue.cs DataVo.Tests/Reactive/CellValueVectorTests.cs
    git commit -m "feat: add core vector interop APIs"

## Task 2: Internal span top-K capability for Flat and HNSW

**Files:**
- Modify: DataVo.Core/Indexing/ISpanVectorIndex.cs
- Modify: DataVo.Core/Indexing/Flat/FlatVectorIndex.cs
- Modify: DataVo.Core/Indexing/HNSW/HNSWIndex.cs
- Modify: DataVo.Tests/Indexing/FlatVectorIndexTests.cs
- Modify: DataVo.Tests/Indexing/HNSWIndexTests.cs

**Interfaces:**
- Extends ISpanVectorIndex with List<long> SearchTopK(ReadOnlySpan<float> queryVector, int topK).
- Produces public FlatVectorIndex.SearchTopK(ReadOnlySpan<float>, int) and HNSWIndex.SearchTopK(ReadOnlySpan<float>, int).
- Existing SearchTopK(float[], int) methods keep their null error contract and forward to the span versions.

- [ ] **Step 1: Write failing stack-span search tests**

Append to DataVo.Tests/Indexing/FlatVectorIndexTests.cs:

    [Fact]
    public void SearchTopK_ReadOnlySpan_ReturnsExactNearestRow()
    {
        var index = new FlatVectorIndex { Metric = "euclidean" };
        index.Insert(10L, [1f, 0f, 0f]);
        index.Insert(20L, [0f, 1f, 0f]);

        ReadOnlySpan<float> query = stackalloc float[] { 0.9f, 0.1f, 0f };
        Assert.Equal([10L], index.SearchTopK(query, topK: 1));
    }

Append to DataVo.Tests/Indexing/HNSWIndexTests.cs:

    [Fact]
    public void SearchTopK_ReadOnlySpan_ReturnsNearestRow()
    {
        var index = new HNSWIndex { Metric = "euclidean", M = 8, EfSearch = 32 };
        index.Insert(10L, [1f, 0f, 0f]);
        index.Insert(20L, [0f, 1f, 0f]);

        ReadOnlySpan<float> query = stackalloc float[] { 0.9f, 0.1f, 0f };
        Assert.Equal([10L], index.SearchTopK(query, topK: 2).Take(1));
    }

- [ ] **Step 2: Run the two focused tests and verify they fail for overload resolution**

Run: dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "FullyQualifiedName~FlatVectorIndexTests.SearchTopK_ReadOnlySpan|FullyQualifiedName~HNSWIndexTests.SearchTopK_ReadOnlySpan"

Expected: compilation fails because neither index accepts ReadOnlySpan<float>.

- [ ] **Step 3: Add the capability and make the array methods wrappers**

Replace DataVo.Core/Indexing/ISpanVectorIndex.cs with:

    namespace DataVo.Core.Indexing;

    internal interface ISpanVectorIndex
    {
        void Insert(long rowId, ReadOnlySpan<float> vector);
        List<long> SearchTopK(ReadOnlySpan<float> queryVector, int topK);
    }

In both FlatVectorIndex and HNSWIndex, retain the array compatibility method as:

    public List<long> SearchTopK(float[] queryVector, int topK)
    {
        if (queryVector == null)
        {
            throw new ArgumentException("Query vector cannot be null or empty", nameof(queryVector));
        }

        return SearchTopK(queryVector.AsSpan(), topK);
    }

Move each existing method body after its null check to a public ReadOnlySpan<float> overload. Its first validation must be:

    if (queryVector.Length == 0)
    {
        throw new ArgumentException("Query vector cannot be null or empty", nameof(queryVector));
    }

Preserve the existing finite-value validation, dimension checks, locks, metric behavior, ranking, allocation, and HNSW graph traversal unchanged below that validation. Do not modify BrowserFallbackVectorIndex; it intentionally exercises the manager fallback.

- [ ] **Step 4: Run the focused index suites and verify they pass**

Run: dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "FullyQualifiedName~FlatVectorIndexTests|FullyQualifiedName~HNSWIndexTests"

Expected: all selected Flat/HNSW tests pass, including the new stack-span calls.

- [ ] **Step 5: Commit the internal index capability**

Run:

    git add DataVo.Core/Indexing/ISpanVectorIndex.cs DataVo.Core/Indexing/Flat/FlatVectorIndex.cs DataVo.Core/Indexing/HNSW/HNSWIndex.cs DataVo.Tests/Indexing/FlatVectorIndexTests.cs DataVo.Tests/Indexing/HNSWIndexTests.cs
    git commit -m "feat: route built-in vector searches through spans"

## Task 3: DataVoContext span search and manager fallback

**Files:**
- Modify: DataVo.Core/Indexing/IndexManager.cs
- Modify: DataVo.Core/DataVoContext.cs
- Modify: DataVo.Tests/E2E/VectorContextTests.cs

**Interfaces:**
- Produces internal IndexManager.SearchVector(ReadOnlySpan<float>, int, string, string, string, string).
- Produces public DataVoContext.SearchNearest(string, string, ReadOnlySpan<float>, int).
- Array APIs forward to span APIs after their existing null validation.
- An IVectorIndex that is not an ISpanVectorIndex receives exactly one queryVector.ToArray() fallback.

- [ ] **Step 1: Write failing public span-context tests**

Append this theory to DataVo.Tests/E2E/VectorContextTests.cs:

    [Theory]
    [InlineData("HNSW", "idx_span_hnsw")]
    [InlineData("FLAT", "idx_span_flat")]
    public void DataVoContext_SearchNearest_ReadOnlySpan_ReturnsRankedRows(string indexKind, string indexName)
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string dbName = $"VecCtxSpan_{Guid.NewGuid():N}";

        context.Execute($"CREATE DATABASE {dbName}");
        context.Execute($"USE {dbName}");
        context.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR)");
        context.Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (1, '[1,0,0]', 'A')");
        context.Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (2, '[0,1,0]', 'B')");
        context.Execute($"CREATE INDEX {indexName} ON Embeddings (Emb) USING {indexKind}");

        ReadOnlySpan<float> query = stackalloc float[] { 0.9f, 0.1f, 0f };
        List<Dictionary<string, object?>> results = context.SearchNearest("Embeddings", indexName, query, topK: 1);

        Assert.Single(results);
        Assert.Equal("A", results[0]["Label"]);
    }

- [ ] **Step 2: Run the focused context test and verify it fails for the missing overload**

Run: dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "FullyQualifiedName~VectorContextTests.DataVoContext_SearchNearest_ReadOnlySpan"

Expected: compilation fails because DataVoContext.SearchNearest does not accept ReadOnlySpan<float>.

- [ ] **Step 3: Add shared manager and context span paths**

In DataVo.Core/Indexing/IndexManager.cs, preserve the public float[] method as a null-checking wrapper and add:

    internal List<long> SearchVector(
        ReadOnlySpan<float> queryVector,
        int topK,
        string indexName,
        string tableName,
        string databaseName,
        string indexType = "HNSW")
    {
        if (topK <= 0)
        {
            return [];
        }

        IVectorIndex vectorIndex = GetOrLoadVectorIndex(indexName, tableName, databaseName, indexType);
        List<long> results = vectorIndex is ISpanVectorIndex spanVectorIndex
            ? spanVectorIndex.SearchTopK(queryVector, topK)
            : vectorIndex.SearchTopK(queryVector.ToArray(), topK);
        RuntimeQueryDiagnosticsScope.RecordVectorSearch(indexName, topK, expansionPasses: 0);
        return results;
    }

In DataVo.Core/DataVoContext.cs, make the current float[] overload validate with ThrowHelper.ThrowIfNull(queryVector) and call a private SearchNearestCore helper. Add this public overload:

    public List<Dictionary<string, object?>> SearchNearest(
        string tableName,
        string indexName,
        ReadOnlySpan<float> queryVector,
        int topK = 10) =>
        SearchNearestCore(tableName, indexName, queryVector, topK);

Move the current array-overload body into:

    private List<Dictionary<string, object?>> SearchNearestCore(
        string tableName,
        string indexName,
        ReadOnlySpan<float> queryVector,
        int topK)

Keep its diagnostics, database resolution, index-kind lookup, materialization, and catch blocks intact; change only the manager call to Engine.IndexManager.SearchVector(queryVector, topK, indexName, tableName, databaseName, indexKind). Make the string overload pass its parsed vector to SearchNearestCore as well. Do not add any member to public IVectorIndex.

- [ ] **Step 4: Run the context and diagnostics suites**

Run: dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "FullyQualifiedName~VectorContextTests|FullyQualifiedName~RuntimeDiagnosticsTests"

Expected: every selected test passes; Flat/HNSW span calls return the same top row and existing diagnostics behavior stays unchanged.

- [ ] **Step 5: Commit the public search boundary**

Run:

    git add DataVo.Core/Indexing/IndexManager.cs DataVo.Core/DataVoContext.cs DataVo.Tests/E2E/VectorContextTests.cs
    git commit -m "feat: add span nearest-neighbor search"

## Task 4: Portable-target and package-candidate verification

**Files:**
- No source changes expected.

**Interfaces:**
- Verifies Tasks 1-3 compile under net10.0 and opt-in netstandard2.1.
- Verifies normal DataVo.Core packaging remains net10.0-only.

- [ ] **Step 1: Restore and build both Core targets**

Run:

    dotnet restore DataVo.Core/DataVo.Core.csproj -p:DataVoEnablePortableTarget=true
    dotnet build DataVo.Core/DataVo.Core.csproj --no-restore -p:DataVoEnablePortableTarget=true

Expected: successful net10.0 and netstandard2.1 builds, with no warning introduced by this slice.

- [ ] **Step 2: Run focused Core/vector regression coverage**

Run: dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "FullyQualifiedName~CellValueVectorTests|FullyQualifiedName~FlatVectorIndexTests|FullyQualifiedName~HNSWIndexTests|FullyQualifiedName~VectorContextTests"

Expected: all selected tests pass.

- [ ] **Step 3: Verify the portable package candidate and normal package contract**

Run: bash scripts/verify-portable-candidate.sh

Expected: the candidate contains lib/netstandard2.1/DataVo.Core.dll and the normal package remains net10.0-only.

- [ ] **Step 4: Run the full test project**

Run: dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore

Expected: the full suite passes. Existing baseline warnings are recorded separately; unrelated warning cleanup is out of scope.

- [ ] **Step 5: Record the verified commit boundary**

Run:

    git status --short
    git log -3 --oneline

Expected: only the three feature commits above are new for Slice A. Do not add generated packages, bin, obj, or user-owned untracked benchmark/image files.

## Plan Self-Review

- Spec coverage: Core vector creation, ownership transfer, length/copy, span search, Flat/HNSW direct routing, one-copy fallback, portable build, normal package restriction, and test-first work map to Tasks 1-4.
- Intentional deferrals: UPM packages, UnityEngine/Mathematics adapters, NativeArray/NativeSlice, Jobs/Burst drain, Unity Editor proof, derived NuGet, Windows IL2CPP, and benchmarks are later slices from the approved design.
- Type consistency: public context methods use ReadOnlySpan<float>; IndexManager's span overload is internal; IVectorIndex stays float[]-based; ISpanVectorIndex carries the internal span top-K capability.
- Placeholder scan: no unresolved markers, unnamed verification, or vague validation instruction remains.
