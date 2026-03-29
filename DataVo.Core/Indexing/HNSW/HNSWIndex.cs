using DataVo.Core.Utils;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;

namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Implements an in-memory HNSW (Hierarchical Navigable Small World) vector index.
/// </summary>
public class HNSWIndex : IVectorIndex
{
    private const int DefaultM = 16;
    private const int DefaultEfConstruction = 64;
    private const int DefaultEfSearch = 64;
    private const int MaxSupportedLevels = 33;

    private readonly Random _random = Random.Shared;
    private readonly Dictionary<long, int> _rowIdToOrdinal = [];
    private readonly Stack<int> _freeOrdinals = [];
    private readonly object _stateGate = new();

    [ThreadStatic] private static SearchWorkspace? _threadSearchWorkspace;
    [ThreadStatic] private static SelectionWorkspace? _threadSelectionWorkspace;

    private float[] _vectorData = [];
    private long[] _rowIdByOrdinal = [];
    private bool[] _isActive = [];
    private int[] _nodeLevels = [];
    private int[] _graphLinks = [];

    private int[] _levelStrides = [];
    private int[] _levelOffsets = [];
    private int _nodeGraphStride;
    private object[] _nodeLocks = [];

    private int _vectorDimension = -1;
    private int _ordinalCapacity;
    private int _nextOrdinal;
    private int _count;

    private sealed class SearchWorkspace
    {
        public PriorityQueue<(int Ordinal, float Distance), float> CandidateQueue { get; } = new();
        public PriorityQueue<(int Ordinal, float Distance), float> ResultQueue { get; } = new();
        public int[] VisitedEpochByOrdinal = [];
        public int Epoch { get; set; } = 1;

        public int[] ResultOrdinals = [];
        public float[] ResultDistances = [];

        public void EnsureVisitedCapacity(int capacity)
        {
            if (VisitedEpochByOrdinal.Length < capacity)
            {
                Array.Resize(ref VisitedEpochByOrdinal, Math.Max(capacity, VisitedEpochByOrdinal.Length * 2 + 64));
            }
        }

        public void EnsureResultCapacity(int capacity)
        {
            if (ResultOrdinals.Length < capacity)
            {
                Array.Resize(ref ResultOrdinals, Math.Max(capacity, ResultOrdinals.Length * 2 + 16));
                Array.Resize(ref ResultDistances, ResultOrdinals.Length);
            }
        }

        public void Begin()
        {
            Epoch++;
            if (Epoch == int.MaxValue)
            {
                Array.Clear(VisitedEpochByOrdinal);
                Epoch = 1;
            }

            CandidateQueue.Clear();
            ResultQueue.Clear();
        }
    }

    private sealed class SelectionWorkspace
    {
        public int[] CandidateOrdinals = [];
        public float[] CandidateDistances = [];
        public int[] ExistingNeighbors = [];

        public int[] SeenEpochByOrdinal = [];
        public int SeenEpoch { get; set; } = 1;

        public void EnsureCandidateCapacity(int capacity)
        {
            if (CandidateOrdinals.Length < capacity)
            {
                int newSize = Math.Max(capacity, CandidateOrdinals.Length * 2 + 16);
                Array.Resize(ref CandidateOrdinals, newSize);
                Array.Resize(ref CandidateDistances, newSize);
                Array.Resize(ref ExistingNeighbors, newSize);
            }
        }

        public void EnsureSeenCapacity(int capacity)
        {
            if (SeenEpochByOrdinal.Length < capacity)
            {
                Array.Resize(ref SeenEpochByOrdinal, Math.Max(capacity, SeenEpochByOrdinal.Length * 2 + 64));
            }
        }

        public void BeginSeenPass()
        {
            SeenEpoch++;
            if (SeenEpoch == int.MaxValue)
            {
                Array.Clear(SeenEpochByOrdinal);
                SeenEpoch = 1;
            }
        }
    }

    internal sealed class FlatState
    {
        public required string Metric { get; init; }
        public required int M { get; init; }
        public required int EfConstruction { get; init; }
        public required bool EnableAdaptiveEfConstruction { get; init; }
        public required double AdaptiveEfConstructionMultiplier { get; init; }
        public required bool EnableInsertionCandidateExpansion { get; init; }
        public required double InsertionCandidateExpansionFactor { get; init; }
        public required bool EnableAdaptiveInsertionCandidateExpansion { get; init; }
        public required double AdaptiveInsertionExpansionMinFactor { get; init; }
        public required double AdaptiveInsertionExpansionMaxFactor { get; init; }
        public required bool EnableInsertionNeighborhoodPruning { get; init; }
        public required double InsertionNeighborhoodPruningThreshold { get; init; }
        public required int InsertionNeighborhoodPruneHops { get; init; }
        public required int EfSearch { get; init; }
        public required bool EnableDiversityHeuristic { get; init; }
        public required bool EnableDeleteGraphRepair { get; init; }
        public required bool EnableAdaptiveEfSearch { get; init; }
        public required double AdaptiveEfSearchMultiplier { get; init; }
        public required long? EntryPointId { get; init; }
        public required int MaxLevel { get; init; }
        public required int Count { get; init; }

        public required int VectorDimension { get; init; }
        public required int OrdinalCapacity { get; init; }
        public required int NextOrdinal { get; init; }

        public required float[] VectorData { get; init; }
        public required long[] RowIdByOrdinal { get; init; }
        public required bool[] IsActive { get; init; }
        public required int[] NodeLevels { get; init; }
        public required int[] GraphLinks { get; init; }
        public required int[] LevelStrides { get; init; }
        public required int[] LevelOffsets { get; init; }

        public required long[] RowIds { get; init; }
        public required int[] Ordinals { get; init; }
    }

    /// <summary>
    /// Gets the index family identifier.
    /// </summary>
    public string IndexType => "HNSW";

    /// <summary>
    /// Gets or sets the distance metric used by the index (for example, <c>cosine</c> or <c>euclidean</c>).
    /// </summary>
    public string Metric { get; set; } = "cosine";

    /// <summary>
    /// Gets or sets the maximum number of neighbors per node for upper layers.
    /// Layer 0 uses up to <c>2 * M</c> neighbors.
    /// </summary>
    public int M { get; set; } = DefaultM;

    /// <summary>
    /// Gets or sets the base construction search width used while inserting vectors.
    /// </summary>
    public int EfConstruction { get; set; } = DefaultEfConstruction;

    /// <summary>
    /// Gets or sets a value indicating whether insertion uses an adaptive construction search width.
    /// </summary>
    public bool EnableAdaptiveEfConstruction { get; set; } = true;

    /// <summary>
    /// Gets or sets the multiplier used when adaptive construction search width is enabled.
    /// </summary>
    public double AdaptiveEfConstructionMultiplier { get; set; } = 1.25d;

    /// <summary>
    /// Gets or sets a value indicating whether insertion candidate sets are expanded before neighbor selection.
    /// </summary>
    public bool EnableInsertionCandidateExpansion { get; set; } = true;

    /// <summary>
    /// Gets or sets the fixed expansion factor for insertion candidates when adaptive expansion is disabled.
    /// </summary>
    public double InsertionCandidateExpansionFactor { get; set; } = 1.5d;

    /// <summary>
    /// Gets or sets a value indicating whether insertion candidate expansion is computed adaptively.
    /// </summary>
    public bool EnableAdaptiveInsertionCandidateExpansion { get; set; } = true;

    /// <summary>
    /// Gets or sets the lower bound for adaptive insertion expansion.
    /// </summary>
    public double AdaptiveInsertionExpansionMinFactor { get; set; } = 1.0d;

    /// <summary>
    /// Gets or sets the upper bound for adaptive insertion expansion.
    /// </summary>
    public double AdaptiveInsertionExpansionMaxFactor { get; set; } = 2.5d;

    /// <summary>
    /// Gets or sets a value indicating whether neighborhood pruning runs during insertion.
    /// </summary>
    public bool EnableInsertionNeighborhoodPruning { get; set; } = true;

    /// <summary>
    /// Gets or sets the pruning threshold used by insertion neighborhood pruning.
    /// </summary>
    public double InsertionNeighborhoodPruningThreshold { get; set; } = 0.85d;

    /// <summary>
    /// Gets or sets how many graph hops are considered during insertion neighborhood pruning.
    /// </summary>
    public int InsertionNeighborhoodPruneHops { get; set; } = 1;

    /// <summary>
    /// Gets or sets the base search width used for query-time exploration.
    /// </summary>
    public int EfSearch { get; set; } = DefaultEfSearch;

    /// <summary>
    /// Gets or sets a value indicating whether query-time search width is adapted to query demand.
    /// </summary>
    public bool EnableAdaptiveEfSearch { get; set; } = true;

    /// <summary>
    /// Gets or sets the multiplier used when adaptive query-time search width is enabled.
    /// </summary>
    public double AdaptiveEfSearchMultiplier { get; set; } = 1.5d;

    /// <summary>
    /// Gets or sets a value indicating whether diversity-aware neighbor selection is enabled.
    /// </summary>
    public bool EnableDiversityHeuristic { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether graph repair heuristics run after deletions.
    /// </summary>
    public bool EnableDeleteGraphRepair { get; set; } = true;

    /// <summary>
    /// Gets the current entry-point row id used for top-layer navigation.
    /// </summary>
    public long? EntryPointId { get; private set; }

    /// <summary>
    /// Gets the highest level currently present in the graph.
    /// </summary>
    public int MaxLevel { get; private set; } = -1;

    /// <summary>
    /// Gets the number of active vectors currently indexed.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Pre-allocates storage and thread-local scratch buffers for an expected index size.
    /// </summary>
    /// <param name="expectedCount">Expected number of indexed vectors.</param>
    /// <param name="vectorDimension">Dimension of each vector.</param>
    public void Reserve(int expectedCount, int vectorDimension)
    {
        if (expectedCount <= 0)
        {
            return;
        }

        lock (_stateGate)
        {
            EnsureVectorDimension(vectorDimension);
            EnsureCapacity(expectedCount);
        }

        // Warm up current-thread scratch buffers once so insert hot path does not resize repeatedly.
        var searchWorkspace = GetSearchWorkspace();
        searchWorkspace.EnsureVisitedCapacity(expectedCount + 1);
        searchWorkspace.EnsureResultCapacity(Math.Max(16, Math.Max(EfSearch, EfConstruction) * 4));

        var selectionWorkspace = GetSelectionWorkspace();
        selectionWorkspace.EnsureSeenCapacity(expectedCount + 1);
        selectionWorkspace.EnsureCandidateCapacity(Math.Max(64, ResolveNeighborLimit(0) * 4));
    }

    /// <summary>
    /// Inserts a batch of vectors using parallel workers.
    /// </summary>
    /// <param name="rowIds">Row ids mapped one-to-one to vectors.</param>
    /// <param name="vectors">Flat vector buffer of size <c>rowIds.Length * vectorDimension</c>.</param>
    /// <param name="vectorDimension">Dimension of each vector.</param>
    /// <param name="maxDegreeOfParallelism">Optional parallelism override; when zero or negative, processor count is used.</param>
    public void InsertBatchParallel(long[] rowIds, float[] vectors, int vectorDimension, int maxDegreeOfParallelism = 0)
    {
        if (rowIds == null)
        {
            throw new ArgumentNullException(nameof(rowIds));
        }

        if (vectors == null)
        {
            throw new ArgumentNullException(nameof(vectors));
        }

        if (vectorDimension <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(vectorDimension));
        }

        if (rowIds.Length * vectorDimension != vectors.Length)
        {
            throw new ArgumentException("Vector buffer length does not match rowId count * vectorDimension.", nameof(vectors));
        }

        Reserve(_count + rowIds.Length, vectorDimension);

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism > 0
                ? maxDegreeOfParallelism
                : Environment.ProcessorCount
        };

        Parallel.For(0, rowIds.Length, options, i =>
        {
            int offset = i * vectorDimension;
            InsertCore(rowIds[i], vectors.AsSpan(offset, vectorDimension));
        });
    }

    /// <summary>
    /// Inserts or replaces a single vector for a row id.
    /// </summary>
    /// <param name="rowId">Row id to associate with the vector.</param>
    /// <param name="vector">Vector payload to index.</param>
    public void Insert(long rowId, float[] vector)
    {
        if (vector == null || vector.Length == 0)
        {
            throw new ArgumentException("Vector cannot be null or empty", nameof(vector));
        }

        ValidateFiniteVector(vector, nameof(vector));
        InsertCore(rowId, vector);
    }

    private void InsertCore(long rowId, ReadOnlySpan<float> vector)
    {
        lock (_stateGate)
        {
            EnsureVectorDimension(vector.Length);
        }

        int ordinal;
        int sampledLevel;

        lock (_stateGate)
        {
            if (_rowIdToOrdinal.ContainsKey(rowId))
            {
                Delete([rowId]);
            }

            ordinal = AcquireOrdinal(rowId);
            _isActive[ordinal] = true;
            _count++;

            sampledLevel = SampleLevel();
            _nodeLevels[ordinal] = sampledLevel;
            WriteVector(ordinal, vector);
            ClearNodeLinks(ordinal);

            if (!EntryPointId.HasValue)
            {
                EntryPointId = rowId;
                MaxLevel = sampledLevel;
                return;
            }
        }

        int entryOrdinal = TryGetEntryOrdinal();
        if (entryOrdinal < 0)
        {
            return;
        }

        int topLevel;
        lock (_stateGate)
        {
            topLevel = MaxLevel;
        }
        int targetLevel = sampledLevel;

        for (int level = topLevel; level > targetLevel; level--)
        {
            entryOrdinal = SearchGreedy(vector, entryOrdinal, level);
        }

        var selectWorkspace = GetSelectionWorkspace();
        int maxStride = ResolveNeighborLimit(0);
        selectWorkspace.EnsureCandidateCapacity(Math.Max(64, maxStride * 4));
        Span<int> destinationBuffer = stackalloc int[128];

        for (int level = Math.Min(targetLevel, topLevel); level >= 0; level--)
        {
            int ef = ResolveEffectiveEfConstruction(level);
            var searchWorkspace = GetSearchWorkspace();
            int candidateCount = SearchLayer(vector, entryOrdinal, ef, level, searchWorkspace);

            int neighborLimit = ResolveNeighborLimit(level);
            if (neighborLimit > destinationBuffer.Length)
            {
                int[] temp = new int[neighborLimit];
                SelectNeighbors(level, ordinal, searchWorkspace.ResultOrdinals.AsSpan(0, candidateCount), temp.AsSpan(0, neighborLimit));
                int selected = CountSelected(temp);
                ConnectBidirectional(ordinal, temp.AsSpan(0, selected), level);
            }
            else
            {
                Span<int> destination = destinationBuffer.Slice(0, neighborLimit);
                SelectNeighbors(level, ordinal, searchWorkspace.ResultOrdinals.AsSpan(0, candidateCount), destination.Slice(0, neighborLimit));
                int selected = CountSelected(destination);
                ConnectBidirectional(ordinal, destination.Slice(0, selected), level);
            }

            if (candidateCount > 0)
            {
                entryOrdinal = searchWorkspace.ResultOrdinals[0];
            }
        }

        lock (_stateGate)
        {
            if (sampledLevel > MaxLevel)
            {
                EntryPointId = rowId;
                MaxLevel = sampledLevel;
            }
        }
    }

    /// <summary>
    /// Deletes indexed vectors for the provided row ids.
    /// </summary>
    /// <param name="rowIds">Row ids to remove.</param>
    public void Delete(List<long> rowIds)
    {
        if (rowIds == null || rowIds.Count == 0)
        {
            return;
        }

        foreach (long rowId in rowIds)
        {
            if (!_rowIdToOrdinal.TryGetValue(rowId, out int ordinal) || !_isActive[ordinal])
            {
                continue;
            }

            int nodeLevel = _nodeLevels[ordinal];
            for (int level = 0; level <= nodeLevel; level++)
            {
                Span<int> neighbors = GetNeighborSpan(ordinal, level);
                for (int i = 0; i < neighbors.Length; i++)
                {
                    int neighbor = neighbors[i];
                    if (neighbor < 0)
                    {
                        break;
                    }

                    RemoveEdgeOneWay(neighbor, ordinal, level);
                }
            }

            _isActive[ordinal] = false;
            _nodeLevels[ordinal] = 0;
            _rowIdByOrdinal[ordinal] = 0;
            _rowIdToOrdinal.Remove(rowId);
            _freeOrdinals.Push(ordinal);
            _count--;
            ClearNodeLinks(ordinal);
        }

        if (_count == 0)
        {
            EntryPointId = null;
            MaxLevel = -1;
            return;
        }

        if (!EntryPointId.HasValue || !_rowIdToOrdinal.TryGetValue(EntryPointId.Value, out int epOrd) || !_isActive[epOrd])
        {
            ResolveFallbackEntryPoint();
        }
    }

    /// <summary>
    /// Returns the nearest row ids for the supplied query vector.
    /// </summary>
    /// <param name="queryVector">Query vector.</param>
    /// <param name="topK">Maximum number of matches to return.</param>
    /// <returns>A list of row ids ordered from nearest to farthest.</returns>
    public List<long> SearchTopK(float[] queryVector, int topK)
    {
        if (queryVector == null || queryVector.Length == 0)
        {
            throw new ArgumentException("Query vector cannot be null or empty", nameof(queryVector));
        }

        ValidateFiniteVector(queryVector, nameof(queryVector));

        if (_count == 0 || topK <= 0)
        {
            return [];
        }

        if (_vectorDimension != queryVector.Length)
        {
            return [];
        }

        if (topK >= _count)
        {
            return ExactSearch(queryVector, topK);
        }

        if (!EntryPointId.HasValue || !_rowIdToOrdinal.TryGetValue(EntryPointId.Value, out int entryOrdinal))
        {
            return ExactSearch(queryVector, topK);
        }

        int current = entryOrdinal;
        for (int level = MaxLevel; level > 0; level--)
        {
            current = SearchGreedy(queryVector, current, level);
        }

        int ef = ResolveEffectiveEfSearch(topK);
        var workspace = GetSearchWorkspace();
        int resultCount = SearchLayer(queryVector, current, ef, 0, workspace);

        int take = Math.Min(topK, resultCount);
        var result = new List<long>(take);
        for (int i = 0; i < take; i++)
        {
            int ordinal = workspace.ResultOrdinals[i];
            if ((uint)ordinal < (uint)_rowIdByOrdinal.Length)
            {
                long rowId = _rowIdByOrdinal[ordinal];
                if (rowId != 0)
                {
                    result.Add(rowId);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Removes all vectors and graph state from the index.
    /// </summary>
    public void Clear()
    {
        _rowIdToOrdinal.Clear();
        _freeOrdinals.Clear();

        _vectorData = [];
        _rowIdByOrdinal = [];
        _isActive = [];
        _nodeLevels = [];
        _graphLinks = [];

        _vectorDimension = -1;
        _ordinalCapacity = 0;
        _nextOrdinal = 0;
        _count = 0;

        _levelOffsets = [];
        _levelStrides = [];
        _nodeGraphStride = 0;
        _nodeLocks = [];

        EntryPointId = null;
        MaxLevel = -1;
    }

    internal FlatState ExportFlatState()
    {
        long[] rowIds = new long[_rowIdToOrdinal.Count];
        int[] ordinals = new int[_rowIdToOrdinal.Count];
        int idx = 0;
        foreach (var pair in _rowIdToOrdinal)
        {
            rowIds[idx] = pair.Key;
            ordinals[idx] = pair.Value;
            idx++;
        }

        return new FlatState
        {
            Metric = Metric,
            M = M,
            EfConstruction = EfConstruction,
            EnableAdaptiveEfConstruction = EnableAdaptiveEfConstruction,
            AdaptiveEfConstructionMultiplier = AdaptiveEfConstructionMultiplier,
            EnableInsertionCandidateExpansion = EnableInsertionCandidateExpansion,
            InsertionCandidateExpansionFactor = InsertionCandidateExpansionFactor,
            EnableAdaptiveInsertionCandidateExpansion = EnableAdaptiveInsertionCandidateExpansion,
            AdaptiveInsertionExpansionMinFactor = AdaptiveInsertionExpansionMinFactor,
            AdaptiveInsertionExpansionMaxFactor = AdaptiveInsertionExpansionMaxFactor,
            EnableInsertionNeighborhoodPruning = EnableInsertionNeighborhoodPruning,
            InsertionNeighborhoodPruningThreshold = InsertionNeighborhoodPruningThreshold,
            InsertionNeighborhoodPruneHops = InsertionNeighborhoodPruneHops,
            EfSearch = EfSearch,
            EnableDiversityHeuristic = EnableDiversityHeuristic,
            EnableDeleteGraphRepair = EnableDeleteGraphRepair,
            EnableAdaptiveEfSearch = EnableAdaptiveEfSearch,
            AdaptiveEfSearchMultiplier = AdaptiveEfSearchMultiplier,
            EntryPointId = EntryPointId,
            MaxLevel = MaxLevel,
            Count = _count,
            VectorDimension = _vectorDimension,
            OrdinalCapacity = _ordinalCapacity,
            NextOrdinal = _nextOrdinal,
            VectorData = [.. _vectorData],
            RowIdByOrdinal = [.. _rowIdByOrdinal],
            IsActive = [.. _isActive],
            NodeLevels = [.. _nodeLevels],
            GraphLinks = [.. _graphLinks],
            LevelStrides = [.. _levelStrides],
            LevelOffsets = [.. _levelOffsets],
            RowIds = rowIds,
            Ordinals = ordinals
        };
    }

    internal void ImportFlatState(FlatState state)
    {
        Metric = string.IsNullOrWhiteSpace(state.Metric) ? "cosine" : state.Metric;
        M = Math.Max(1, state.M);
        EfConstruction = Math.Max(1, state.EfConstruction);
        EnableAdaptiveEfConstruction = state.EnableAdaptiveEfConstruction;
        AdaptiveEfConstructionMultiplier = state.AdaptiveEfConstructionMultiplier > 0d ? state.AdaptiveEfConstructionMultiplier : 1.25d;
        EnableInsertionCandidateExpansion = state.EnableInsertionCandidateExpansion;
        InsertionCandidateExpansionFactor = state.InsertionCandidateExpansionFactor > 0d ? state.InsertionCandidateExpansionFactor : 1.5d;
        EnableAdaptiveInsertionCandidateExpansion = state.EnableAdaptiveInsertionCandidateExpansion;
        AdaptiveInsertionExpansionMinFactor = state.AdaptiveInsertionExpansionMinFactor > 0d ? state.AdaptiveInsertionExpansionMinFactor : 1.0d;
        AdaptiveInsertionExpansionMaxFactor = state.AdaptiveInsertionExpansionMaxFactor > 0d ? state.AdaptiveInsertionExpansionMaxFactor : 2.5d;
        EnableInsertionNeighborhoodPruning = state.EnableInsertionNeighborhoodPruning;
        InsertionNeighborhoodPruningThreshold = state.InsertionNeighborhoodPruningThreshold > 0d ? state.InsertionNeighborhoodPruningThreshold : 0.85d;
        InsertionNeighborhoodPruneHops = Math.Max(1, state.InsertionNeighborhoodPruneHops);
        EfSearch = Math.Max(1, state.EfSearch);
        EnableDiversityHeuristic = state.EnableDiversityHeuristic;
        EnableDeleteGraphRepair = state.EnableDeleteGraphRepair;
        EnableAdaptiveEfSearch = state.EnableAdaptiveEfSearch;
        AdaptiveEfSearchMultiplier = state.AdaptiveEfSearchMultiplier > 0d ? state.AdaptiveEfSearchMultiplier : 1.5d;

        _vectorDimension = state.VectorDimension;
        _ordinalCapacity = state.OrdinalCapacity;
        _nextOrdinal = state.NextOrdinal;
        _count = state.Count;

        _vectorData = state.VectorData ?? [];
        _rowIdByOrdinal = state.RowIdByOrdinal ?? [];
        _isActive = state.IsActive ?? [];
        _nodeLevels = state.NodeLevels ?? [];
        _graphLinks = state.GraphLinks ?? [];
        _levelStrides = state.LevelStrides ?? [];
        _levelOffsets = state.LevelOffsets ?? [];
        _nodeGraphStride = _levelStrides.Sum();

        _rowIdToOrdinal.Clear();
        if (state.RowIds != null && state.Ordinals != null)
        {
            int mapCount = Math.Min(state.RowIds.Length, state.Ordinals.Length);
            for (int i = 0; i < mapCount; i++)
            {
                _rowIdToOrdinal[state.RowIds[i]] = state.Ordinals[i];
            }
        }

        _freeOrdinals.Clear();
        for (int ordinal = _nextOrdinal - 1; ordinal >= 0; ordinal--)
        {
            if ((uint)ordinal < (uint)_isActive.Length && !_isActive[ordinal])
            {
                _freeOrdinals.Push(ordinal);
            }
        }

        EntryPointId = state.EntryPointId;
        MaxLevel = state.MaxLevel;
    }

    private void EnsureVectorDimension(int dimension)
    {
        if (_vectorDimension < 0)
        {
            _vectorDimension = dimension;
            RecomputeGraphLayout();
            return;
        }

        if (_vectorDimension != dimension)
        {
            throw new ArgumentException($"Vector dimension mismatch. Expected {_vectorDimension}, got {dimension}.");
        }
    }

    private void RecomputeGraphLayout()
    {
        int safeM = Math.Max(1, M);
        _levelStrides = new int[MaxSupportedLevels];
        _levelOffsets = new int[MaxSupportedLevels];

        int offset = 0;
        for (int level = 0; level < MaxSupportedLevels; level++)
        {
            _levelOffsets[level] = offset;
            _levelStrides[level] = level == 0 ? Math.Max(2, safeM * 2) : safeM;
            offset += _levelStrides[level];
        }

        _nodeGraphStride = offset;
    }

    private int AcquireOrdinal(long rowId)
    {
        int ordinal = _freeOrdinals.Count > 0 ? _freeOrdinals.Pop() : _nextOrdinal++;
        EnsureCapacity(ordinal + 1);
        _rowIdToOrdinal[rowId] = ordinal;
        _rowIdByOrdinal[ordinal] = rowId;
        return ordinal;
    }

    private void EnsureCapacity(int requiredOrdinals)
    {
        if (requiredOrdinals <= _ordinalCapacity)
        {
            return;
        }

        int newCapacity = Math.Max(requiredOrdinals, Math.Max(256, _ordinalCapacity * 2));
        int previousCapacity = _ordinalCapacity;
        _ordinalCapacity = newCapacity;

        Array.Resize(ref _rowIdByOrdinal, newCapacity);
        Array.Resize(ref _isActive, newCapacity);
        Array.Resize(ref _nodeLevels, newCapacity);
        Array.Resize(ref _nodeLocks, newCapacity);

        for (int i = previousCapacity; i < newCapacity; i++)
        {
            _nodeLocks[i] = new object();
        }

        if (_vectorDimension > 0)
        {
            Array.Resize(ref _vectorData, newCapacity * _vectorDimension);
        }

        int oldGraphLength = _graphLinks.Length;
        Array.Resize(ref _graphLinks, newCapacity * _nodeGraphStride);
        if (_graphLinks.Length > oldGraphLength)
        {
            Array.Fill(_graphLinks, -1, oldGraphLength, _graphLinks.Length - oldGraphLength);
        }

        if (oldGraphLength == 0 && _graphLinks.Length > 0)
        {
            Array.Fill(_graphLinks, -1);
        }
    }

    private void WriteVector(int ordinal, ReadOnlySpan<float> vector)
    {
        int offset = ordinal * _vectorDimension;
        vector.CopyTo(_vectorData.AsSpan(offset, _vectorDimension));
    }

    private ReadOnlySpan<float> GetVector(int ordinal)
    {
        int offset = ordinal * _vectorDimension;
        return _vectorData.AsSpan(offset, _vectorDimension);
    }

    private int SampleLevel()
    {
        int level = 0;
        while (_random.NextDouble() < 1.0 / Math.E && level < MaxSupportedLevels - 1)
        {
            level++;
        }

        return level;
    }

    private Span<int> GetNeighborSpan(int ordinal, int level)
    {
        int offset = (ordinal * _nodeGraphStride) + _levelOffsets[level];
        int stride = _levelStrides[level];
        return _graphLinks.AsSpan(offset, stride);
    }

    private void ClearNodeLinks(int ordinal)
    {
        int offset = ordinal * _nodeGraphStride;
        _graphLinks.AsSpan(offset, _nodeGraphStride).Fill(-1);
    }

    private int ResolveNeighborLimit(int level)
    {
        return level == 0 ? Math.Max(2, M * 2) : Math.Max(1, M);
    }

    private int ResolveEffectiveEfSearch(int topK)
    {
        int baseEf = Math.Max(topK, Math.Max(1, EfSearch));
        if (!EnableAdaptiveEfSearch)
        {
            return Math.Max(1, baseEf);
        }

        int adaptive = (int)Math.Ceiling(Math.Max(baseEf, Math.Log2(_count + 1) * topK * AdaptiveEfSearchMultiplier));
        return Math.Clamp(adaptive, baseEf, Math.Max(baseEf, _count));
    }

    private int ResolveEffectiveEfConstruction(int level)
    {
        int baseEf = Math.Max(ResolveNeighborLimit(level), Math.Max(1, EfConstruction));
        if (!EnableAdaptiveEfConstruction)
        {
            return baseEf;
        }

        int adaptive = (int)Math.Ceiling(Math.Max(baseEf, Math.Log2(_count + 1) * ResolveNeighborLimit(level) * AdaptiveEfConstructionMultiplier));
        return Math.Clamp(adaptive, baseEf, Math.Max(baseEf, _count));
    }

    private static void ValidateFiniteVector(ReadOnlySpan<float> vector, string parameter)
    {
        for (int i = 0; i < vector.Length; i++)
        {
            if (!float.IsFinite(vector[i]))
            {
                throw new ArgumentException($"Vector contains non-finite value at index {i}.", parameter);
            }
        }
    }

    private SearchWorkspace GetSearchWorkspace()
    {
        _threadSearchWorkspace ??= new SearchWorkspace();
        _threadSearchWorkspace.EnsureVisitedCapacity(_nextOrdinal + 1);
        _threadSearchWorkspace.EnsureResultCapacity(Math.Max(16, Math.Min(_count + 8, Math.Max(1, EfSearch) * 4)));
        return _threadSearchWorkspace;
    }

    private SelectionWorkspace GetSelectionWorkspace()
    {
        _threadSelectionWorkspace ??= new SelectionWorkspace();
        _threadSelectionWorkspace.EnsureSeenCapacity(_nextOrdinal + 1);
        return _threadSelectionWorkspace;
    }

    private int SearchGreedy(ReadOnlySpan<float> query, int entryOrdinal, int level)
    {
        int current = entryOrdinal;
        float currentDistance = Distance(query, GetVector(current));

        bool moved;
        do
        {
            moved = false;
            Span<int> neighbors = GetNeighborSpan(current, level);
            for (int i = 0; i < neighbors.Length; i++)
            {
                int neighbor = neighbors[i];
                if (neighbor < 0)
                {
                    break;
                }

                if (i + 1 < neighbors.Length)
                {
                    PrefetchOrdinalVector(neighbors[i + 1]);
                }

                if (!_isActive[neighbor] || _nodeLevels[neighbor] < level)
                {
                    continue;
                }

                float distance = Distance(query, GetVector(neighbor));
                if (distance < currentDistance)
                {
                    current = neighbor;
                    currentDistance = distance;
                    moved = true;
                }
            }
        } while (moved);

        return current;
    }

    private int SearchLayer(ReadOnlySpan<float> query, int entryOrdinal, int ef, int level, SearchWorkspace workspace)
    {
        workspace.Begin();
        workspace.EnsureVisitedCapacity(_nextOrdinal + 1);
        workspace.EnsureResultCapacity(Math.Max(ef, 16));

        if (entryOrdinal < 0 || entryOrdinal >= _nextOrdinal || !_isActive[entryOrdinal])
        {
            return 0;
        }

        float startDistance = Distance(query, GetVector(entryOrdinal));
        workspace.CandidateQueue.Enqueue((entryOrdinal, startDistance), startDistance);
        workspace.ResultQueue.Enqueue((entryOrdinal, startDistance), -startDistance);
        workspace.VisitedEpochByOrdinal[entryOrdinal] = workspace.Epoch;

        while (workspace.CandidateQueue.TryDequeue(out var current, out float currentDistance))
        {
            if (workspace.ResultQueue.Count >= ef && workspace.ResultQueue.TryPeek(out _, out float worstPriority))
            {
                float worstDistance = -worstPriority;
                if (currentDistance > worstDistance)
                {
                    break;
                }
            }

            Span<int> neighbors = GetNeighborSpan(current.Ordinal, level);
            for (int i = 0; i < neighbors.Length; i++)
            {
                int neighbor = neighbors[i];
                if (neighbor < 0)
                {
                    break;
                }

                if (i + 1 < neighbors.Length)
                {
                    PrefetchOrdinalVector(neighbors[i + 1]);
                }

                if (!_isActive[neighbor] || _nodeLevels[neighbor] < level)
                {
                    continue;
                }

                if (neighbor >= workspace.VisitedEpochByOrdinal.Length)
                {
                    workspace.EnsureVisitedCapacity(neighbor + 1);
                }

                if (workspace.VisitedEpochByOrdinal[neighbor] == workspace.Epoch)
                {
                    continue;
                }

                workspace.VisitedEpochByOrdinal[neighbor] = workspace.Epoch;
                float distance = Distance(query, GetVector(neighbor));

                if (workspace.ResultQueue.Count >= ef && workspace.ResultQueue.TryPeek(out _, out float worst))
                {
                    if (distance >= -worst)
                    {
                        continue;
                    }
                }

                workspace.CandidateQueue.Enqueue((neighbor, distance), distance);
                workspace.ResultQueue.Enqueue((neighbor, distance), -distance);
                if (workspace.ResultQueue.Count > ef)
                {
                    workspace.ResultQueue.Dequeue();
                }
            }
        }

        int resultCount = workspace.ResultQueue.Count;
        workspace.EnsureResultCapacity(resultCount);
        int idx = 0;
        while (workspace.ResultQueue.TryDequeue(out var element, out _))
        {
            workspace.ResultOrdinals[idx] = element.Ordinal;
            workspace.ResultDistances[idx] = element.Distance;
            idx++;
        }

        Array.Sort(workspace.ResultDistances, workspace.ResultOrdinals, 0, idx);
        return idx;
    }

    private void ConnectBidirectional(int source, ReadOnlySpan<int> neighbors, int level)
    {
        for (int i = 0; i < neighbors.Length; i++)
        {
            int neighbor = neighbors[i];
            if (neighbor < 0 || neighbor == source)
            {
                continue;
            }

            AddEdgeOneWay(source, neighbor, level);
            AddEdgeOneWay(neighbor, source, level);
        }
    }

    private void AddEdgeOneWay(int source, int target, int level)
    {
        if (!_isActive[source] || !_isActive[target] || _nodeLevels[source] < level || _nodeLevels[target] < level)
        {
            return;
        }

        WithNodeLock(source, () =>
        {
            Span<int> neighbors = GetNeighborSpan(source, level);
            for (int i = 0; i < neighbors.Length; i++)
            {
                if (neighbors[i] == target)
                {
                    return;
                }

                if (neighbors[i] < 0)
                {
                    neighbors[i] = target;
                    return;
                }
            }

            var workspace = GetSelectionWorkspace();
            workspace.EnsureCandidateCapacity(neighbors.Length + 1);

            int candidateCount = 0;
            for (int i = 0; i < neighbors.Length; i++)
            {
                int neighbor = neighbors[i];
                if (neighbor < 0)
                {
                    break;
                }

                workspace.ExistingNeighbors[candidateCount++] = neighbor;
            }

            workspace.ExistingNeighbors[candidateCount++] = target;

            SelectNeighbors(level, source, workspace.ExistingNeighbors.AsSpan(0, candidateCount), neighbors);
        });
    }

    private void RemoveEdgeOneWay(int source, int target, int level)
    {
        WithNodeLock(source, () =>
        {
            Span<int> neighbors = GetNeighborSpan(source, level);
            int foundAt = -1;
            int count = 0;

            for (int i = 0; i < neighbors.Length; i++)
            {
                if (neighbors[i] < 0)
                {
                    break;
                }

                if (neighbors[i] == target)
                {
                    foundAt = i;
                }

                count++;
            }

            if (foundAt < 0)
            {
                return;
            }

            for (int i = foundAt; i < count - 1; i++)
            {
                neighbors[i] = neighbors[i + 1];
            }

            neighbors[count - 1] = -1;
        });
    }

    // Zero-allocation selection path over spans. It writes the selected ordinals directly to destination.
    private void SelectNeighbors(int level, int targetOrdinal, ReadOnlySpan<int> candidates, Span<int> destination)
    {
        destination.Fill(-1);
        if (destination.Length == 0)
        {
            return;
        }

        var workspace = GetSelectionWorkspace();
        workspace.EnsureCandidateCapacity(candidates.Length);
        workspace.EnsureSeenCapacity(_nextOrdinal + 1);
        workspace.BeginSeenPass();

        int uniqueCount = 0;
        for (int i = 0; i < candidates.Length; i++)
        {
            int candidate = candidates[i];
            if (candidate < 0 || candidate == targetOrdinal)
            {
                continue;
            }

            if ((uint)candidate >= (uint)_isActive.Length || !_isActive[candidate] || _nodeLevels[candidate] < level)
            {
                continue;
            }

            if (candidate >= workspace.SeenEpochByOrdinal.Length)
            {
                workspace.EnsureSeenCapacity(candidate + 1);
            }

            if (workspace.SeenEpochByOrdinal[candidate] == workspace.SeenEpoch)
            {
                continue;
            }

            workspace.SeenEpochByOrdinal[candidate] = workspace.SeenEpoch;
            workspace.CandidateOrdinals[uniqueCount] = candidate;
            workspace.CandidateDistances[uniqueCount] = Distance(GetVector(targetOrdinal), GetVector(candidate));
            uniqueCount++;
        }

        if (uniqueCount == 0)
        {
            return;
        }

        // In-place insertion sort in workspace arrays (stable enough for tiny M-sized neighborhoods).
        for (int i = 1; i < uniqueCount; i++)
        {
            int ord = workspace.CandidateOrdinals[i];
            float dist = workspace.CandidateDistances[i];
            int j = i - 1;
            while (j >= 0 && workspace.CandidateDistances[j] > dist)
            {
                workspace.CandidateOrdinals[j + 1] = workspace.CandidateOrdinals[j];
                workspace.CandidateDistances[j + 1] = workspace.CandidateDistances[j];
                j--;
            }

            workspace.CandidateOrdinals[j + 1] = ord;
            workspace.CandidateDistances[j + 1] = dist;
        }

        int selectedCount = 0;

        if (!EnableDiversityHeuristic)
        {
            int toTake = Math.Min(destination.Length, uniqueCount);
            for (int i = 0; i < toTake; i++)
            {
                destination[i] = workspace.CandidateOrdinals[i];
            }

            return;
        }

        for (int i = 0; i < uniqueCount && selectedCount < destination.Length; i++)
        {
            int candidate = workspace.CandidateOrdinals[i];
            float candidateToTarget = workspace.CandidateDistances[i];

            bool occluded = false;
            for (int s = 0; s < selectedCount; s++)
            {
                int existing = destination[s];
                if (existing < 0)
                {
                    continue;
                }

                float candidateToExisting = Distance(GetVector(candidate), GetVector(existing));
                if (candidateToExisting < candidateToTarget)
                {
                    occluded = true;
                    break;
                }
            }

            if (occluded)
            {
                continue;
            }

            destination[selectedCount++] = candidate;
        }

        if (selectedCount >= destination.Length)
        {
            return;
        }

        for (int i = 0; i < uniqueCount && selectedCount < destination.Length; i++)
        {
            int candidate = workspace.CandidateOrdinals[i];
            bool alreadySelected = false;
            for (int s = 0; s < selectedCount; s++)
            {
                if (destination[s] == candidate)
                {
                    alreadySelected = true;
                    break;
                }
            }

            if (alreadySelected)
            {
                continue;
            }

            destination[selectedCount++] = candidate;
        }
    }

    private static int CountSelected(ReadOnlySpan<int> selected)
    {
        int count = 0;
        for (int i = 0; i < selected.Length; i++)
        {
            if (selected[i] < 0)
            {
                break;
            }

            count++;
        }

        return count;
    }

    private List<long> ExactSearch(ReadOnlySpan<float> query, int topK)
    {
        int take = Math.Min(topK, _count);
        var maxHeap = new PriorityQueue<(int Ordinal, float Distance), (float NegDistance, int NegOrdinal)>();

        for (int ordinal = 0; ordinal < _nextOrdinal; ordinal++)
        {
            PrefetchOrdinalVector(ordinal + 1);

            if ((uint)ordinal >= (uint)_isActive.Length || !_isActive[ordinal])
            {
                continue;
            }

            float distance = Distance(query, GetVector(ordinal));
            if (maxHeap.Count < take)
            {
                maxHeap.Enqueue((ordinal, distance), (-distance, -ordinal));
                continue;
            }

            if (!maxHeap.TryPeek(out _, out var worst))
            {
                continue;
            }

            float worstDistance = -worst.NegDistance;
            if (distance >= worstDistance)
            {
                continue;
            }

            maxHeap.Dequeue();
            maxHeap.Enqueue((ordinal, distance), (-distance, -ordinal));
        }

        var ordinals = new List<int>(maxHeap.Count);
        var distances = new List<float>(maxHeap.Count);
        while (maxHeap.TryDequeue(out var item, out _))
        {
            ordinals.Add(item.Ordinal);
            distances.Add(item.Distance);
        }

        for (int i = 1; i < ordinals.Count; i++)
        {
            int ord = ordinals[i];
            float dist = distances[i];
            int j = i - 1;
            while (j >= 0 && distances[j] > dist)
            {
                ordinals[j + 1] = ordinals[j];
                distances[j + 1] = distances[j];
                j--;
            }

            ordinals[j + 1] = ord;
            distances[j + 1] = dist;
        }

        var result = new List<long>(ordinals.Count);
        for (int i = 0; i < ordinals.Count; i++)
        {
            result.Add(_rowIdByOrdinal[ordinals[i]]);
        }

        return result;
    }

    private void ResolveFallbackEntryPoint()
    {
        int bestOrdinal = -1;
        int bestLevel = -1;
        long bestRowId = long.MaxValue;

        for (int ordinal = 0; ordinal < _nextOrdinal; ordinal++)
        {
            if ((uint)ordinal >= (uint)_isActive.Length || !_isActive[ordinal])
            {
                continue;
            }

            int level = _nodeLevels[ordinal];
            long rowId = _rowIdByOrdinal[ordinal];
            if (level > bestLevel || (level == bestLevel && rowId < bestRowId))
            {
                bestOrdinal = ordinal;
                bestLevel = level;
                bestRowId = rowId;
            }
        }

        if (bestOrdinal < 0)
        {
            EntryPointId = null;
            MaxLevel = -1;
            return;
        }

        EntryPointId = _rowIdByOrdinal[bestOrdinal];
        MaxLevel = bestLevel;
    }

    private float Distance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        return Metric == "cosine"
            ? SimdDistanceKernels.CosineDistance(a, b)
            : SimdDistanceKernels.EuclideanDistance(a, b);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int TryGetEntryOrdinal()
    {
        lock (_stateGate)
        {
            if (!EntryPointId.HasValue)
            {
                return -1;
            }

            return _rowIdToOrdinal.TryGetValue(EntryPointId.Value, out int ordinal) ? ordinal : -1;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private unsafe void PrefetchOrdinalVector(int ordinal)
    {
        if (!Sse.IsSupported || _vectorDimension <= 0 || ordinal < 0 || ordinal >= _nextOrdinal)
        {
            return;
        }

        int offset = ordinal * _vectorDimension;
        if ((uint)offset >= (uint)_vectorData.Length)
        {
            return;
        }

        fixed (float* ptr = &_vectorData[offset])
        {
            Sse.Prefetch0(ptr);
        }
    }

    private void WithNodeLock(int ordinal, Action action)
    {
        if ((uint)ordinal >= (uint)_nodeLocks.Length)
        {
            return;
        }

        object? nodeLock = _nodeLocks[ordinal];
        if (nodeLock == null)
        {
            return;
        }

        lock (nodeLock)
        {
            action();
        }
    }
}
