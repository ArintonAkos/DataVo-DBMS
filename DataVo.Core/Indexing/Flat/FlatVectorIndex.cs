using DataVo.Core.Utils;

namespace DataVo.Core.Indexing.Flat;

/// <summary>
/// Exact brute-force vector index optimized for fast inserts and SIMD-accelerated scans.
/// </summary>
public sealed class FlatVectorIndex : IVectorIndex, IReservableVectorIndex, ISpanVectorIndex
{
    private readonly Dictionary<long, int> _rowIdToOrdinal = [];
    private readonly object _stateGate = new();
    private float[] _vectorData = [];
    private float[] _inverseNormByOrdinal = [];
    private long[] _rowIdByOrdinal = [];
    private bool[] _isActive = [];
    private int[] _freeOrdinals = [];
    private int _vectorDimension = -1;
    private int _ordinalCapacity;
    private int _nextOrdinal;
    private int _freeCount;
    private int _count;

    /// <summary>
    /// Gets the index family identifier.
    /// </summary>
    public string IndexType => "FLAT";

    /// <summary>
    /// Gets or sets the distance metric used by the flat scan.
    /// </summary>
    public string Metric { get; set; } = "cosine";

    /// <summary>
    /// Gets the number of indexed vectors.
    /// </summary>
    public int Count
    {
        get
        {
            lock (_stateGate)
            {
                return _count;
            }
        }
    }

    /// <summary>
    /// Pre-allocates contiguous storage for the expected vector count and dimension.
    /// </summary>
    public void Reserve(int expectedCount, int vectorDimension)
    {
        if (expectedCount <= 0)
        {
            return;
        }

        if (vectorDimension <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(vectorDimension));
        }

        lock (_stateGate)
        {
            EnsureVectorDimension(vectorDimension);
            EnsureCapacity(expectedCount);
            _rowIdToOrdinal.EnsureCapacity(expectedCount);
        }
    }

    /// <summary>
    /// Inserts or replaces a vector for the supplied row id.
    /// </summary>
    public void Insert(long rowId, float[] vector)
    {
        if (vector == null)
        {
            throw new ArgumentNullException(nameof(vector));
        }

        Insert(rowId, vector.AsSpan());
    }

    /// <summary>
    /// Inserts or replaces a vector for the supplied row id.
    /// </summary>
    public void Insert(long rowId, ReadOnlySpan<float> vector)
    {
        if (vector.Length == 0)
        {
            throw new ArgumentException("Vector cannot be empty", nameof(vector));
        }

        ValidateFiniteVector(vector, nameof(vector));

        lock (_stateGate)
        {
            EnsureVectorDimension(vector.Length);
            if (_rowIdToOrdinal.TryGetValue(rowId, out int existingOrdinal))
            {
                vector.CopyTo(GetVectorSpan(existingOrdinal));
                _inverseNormByOrdinal[existingOrdinal] = ComputeInverseNorm(vector);
                return;
            }

            int ordinal = AcquireOrdinal(rowId);
            vector.CopyTo(GetVectorSpan(ordinal));
            _inverseNormByOrdinal[ordinal] = ComputeInverseNorm(vector);
        }
    }

    /// <summary>
    /// Deletes vectors for the supplied row ids.
    /// </summary>
    public void Delete(List<long> rowIds)
    {
        if (rowIds == null || rowIds.Count == 0)
        {
            return;
        }

        lock (_stateGate)
        {
            foreach (long rowId in rowIds)
            {
                if (!_rowIdToOrdinal.Remove(rowId, out int ordinal) || !_isActive[ordinal])
                {
                    continue;
                }

                _isActive[ordinal] = false;
                _rowIdByOrdinal[ordinal] = 0;
                _inverseNormByOrdinal[ordinal] = 0f;
                PushFreeOrdinal(ordinal);
                _count--;
            }
        }
    }

    /// <summary>
    /// Returns the exact nearest row ids for the supplied query vector.
    /// </summary>
    public List<long> SearchTopK(float[] queryVector, int topK)
    {
        if (queryVector == null || queryVector.Length == 0)
        {
            throw new ArgumentException("Query vector cannot be null or empty", nameof(queryVector));
        }

        ValidateFiniteVector(queryVector, nameof(queryVector));

        if (topK <= 0)
        {
            return [];
        }

        lock (_stateGate)
        {
            if (_count == 0 || _vectorDimension != queryVector.Length)
            {
                return [];
            }

            int take = Math.Min(topK, _count);
            int resultCount = 0;
            bool useEuclidean = UsesEuclideanMetric();
            float queryInvNorm = useEuclidean ? 0f : ComputeInverseNorm(queryVector);

            if (take <= 128)
            {
                Span<long> rowIds = stackalloc long[take];
                Span<float> distances = stackalloc float[take];
                SearchIntoTopK(queryVector, useEuclidean, queryInvNorm, rowIds, distances, ref resultCount);
                return BuildResult(rowIds, resultCount);
            }

            long[] rentedRowIds = System.Buffers.ArrayPool<long>.Shared.Rent(take);
            float[] rentedDistances = System.Buffers.ArrayPool<float>.Shared.Rent(take);
            try
            {
                Span<long> rowIds = rentedRowIds.AsSpan(0, take);
                Span<float> distances = rentedDistances.AsSpan(0, take);
                SearchIntoTopK(queryVector, useEuclidean, queryInvNorm, rowIds, distances, ref resultCount);
                return BuildResult(rowIds, resultCount);
            }
            finally
            {
                System.Buffers.ArrayPool<long>.Shared.Return(rentedRowIds);
                System.Buffers.ArrayPool<float>.Shared.Return(rentedDistances);
            }
        }
    }

    private void SearchIntoTopK(
        ReadOnlySpan<float> queryVector,
        bool useEuclidean,
        float queryInvNorm,
        Span<long> rowIds,
        Span<float> distances,
        ref int resultCount)
    {
        for (int ordinal = 0; ordinal < _nextOrdinal; ordinal++)
        {
            if (!_isActive[ordinal])
            {
                continue;
            }

            ReadOnlySpan<float> candidate = GetVectorReadOnlySpan(ordinal);
            float distance = useEuclidean
                ? SimdDistanceKernels.EuclideanDistance(queryVector, candidate)
                : CosineDistanceFromDot(queryVector, candidate, queryInvNorm, _inverseNormByOrdinal[ordinal]);

            long rowId = _rowIdByOrdinal[ordinal];
            if (resultCount < rowIds.Length)
            {
                InsertSorted(rowIds, distances, resultCount, rowId, distance);
                resultCount++;
                continue;
            }

            int worstIndex = rowIds.Length - 1;
            if (IsBetter(distance, rowId, distances[worstIndex], rowIds[worstIndex]))
            {
                InsertSorted(rowIds, distances, worstIndex, rowId, distance);
            }
        }
    }

    private static List<long> BuildResult(ReadOnlySpan<long> rowIds, int resultCount)
    {
        var result = new List<long>(resultCount);
        for (int i = 0; i < resultCount; i++)
        {
            result.Add(rowIds[i]);
        }

        return result;
    }

    internal FlatVectorState ExportFlatState()
    {
        lock (_stateGate)
        {
            return new FlatVectorState
            {
                Metric = Metric,
                VectorDimension = _vectorDimension,
                OrdinalCapacity = _ordinalCapacity,
                NextOrdinal = _nextOrdinal,
                Count = _count,
                VectorData = _ordinalCapacity == 0 || _vectorDimension <= 0
                    ? []
                    : _vectorData.AsSpan(0, _ordinalCapacity * _vectorDimension).ToArray(),
                RowIds = [.. _rowIdByOrdinal],
                IsActive = [.. _isActive]
            };
        }
    }

    internal void ImportFlatState(FlatVectorState state)
    {
        ArgumentNullException.ThrowIfNull(state);

        lock (_stateGate)
        {
            Clear();

            Metric = string.IsNullOrWhiteSpace(state.Metric) ? "cosine" : state.Metric;
            _vectorDimension = state.VectorDimension;
            _ordinalCapacity = state.OrdinalCapacity;
            _nextOrdinal = state.NextOrdinal;
            _count = state.Count;
            _vectorData = state.VectorData ?? [];
            _rowIdByOrdinal = state.RowIds ?? [];
            _isActive = state.IsActive ?? [];
            _inverseNormByOrdinal = new float[Math.Max(0, _ordinalCapacity)];
            _freeOrdinals = new int[Math.Max(4, _ordinalCapacity)];
            _freeCount = 0;

            _rowIdToOrdinal.Clear();
            _rowIdToOrdinal.EnsureCapacity(Math.Max(0, _count));
            for (int ordinal = _nextOrdinal - 1; ordinal >= 0; ordinal--)
            {
                if ((uint)ordinal >= (uint)_isActive.Length || !_isActive[ordinal])
                {
                    PushFreeOrdinal(ordinal);
                    continue;
                }

                _rowIdToOrdinal[_rowIdByOrdinal[ordinal]] = ordinal;
                _inverseNormByOrdinal[ordinal] = ComputeInverseNorm(GetVectorReadOnlySpan(ordinal));
            }

            EnsureCapacity(_ordinalCapacity);
        }
    }

    internal sealed class FlatVectorState
    {
        public required string Metric { get; init; }
        public required int VectorDimension { get; init; }
        public required int OrdinalCapacity { get; init; }
        public required int NextOrdinal { get; init; }
        public required int Count { get; init; }
        public required float[] VectorData { get; init; }
        public required long[] RowIds { get; init; }
        public required bool[] IsActive { get; init; }
    }

    private int AcquireOrdinal(long rowId)
    {
        int ordinal;
        if (_freeCount > 0)
        {
            ordinal = _freeOrdinals[--_freeCount];
        }
        else
        {
            ordinal = _nextOrdinal++;
            EnsureCapacity(_nextOrdinal);
        }

        _rowIdToOrdinal[rowId] = ordinal;
        _rowIdByOrdinal[ordinal] = rowId;
        _isActive[ordinal] = true;
        _count++;
        return ordinal;
    }

    private void EnsureCapacity(int requiredOrdinals)
    {
        if (requiredOrdinals <= 0)
        {
            return;
        }

        if (_vectorDimension <= 0)
        {
            throw new InvalidOperationException("Vector dimension must be set before reserving flat vector storage.");
        }

        if (requiredOrdinals <= _ordinalCapacity
            && _vectorData.Length >= _ordinalCapacity * _vectorDimension
            && _rowIdByOrdinal.Length >= _ordinalCapacity
            && _isActive.Length >= _ordinalCapacity)
        {
            return;
        }

        int newCapacity = Math.Max(requiredOrdinals, Math.Max(4, _ordinalCapacity * 2));
        Array.Resize(ref _vectorData, checked(newCapacity * _vectorDimension));
        Array.Resize(ref _inverseNormByOrdinal, newCapacity);
        Array.Resize(ref _rowIdByOrdinal, newCapacity);
        Array.Resize(ref _isActive, newCapacity);

        if (_freeOrdinals.Length < newCapacity)
        {
            Array.Resize(ref _freeOrdinals, newCapacity);
        }

        _ordinalCapacity = newCapacity;
    }

    private Span<float> GetVectorSpan(int ordinal)
    {
        return _vectorData.AsSpan(ordinal * _vectorDimension, _vectorDimension);
    }

    private ReadOnlySpan<float> GetVectorReadOnlySpan(int ordinal)
    {
        return _vectorData.AsSpan(ordinal * _vectorDimension, _vectorDimension);
    }

    private void PushFreeOrdinal(int ordinal)
    {
        if (_freeCount == _freeOrdinals.Length)
        {
            Array.Resize(ref _freeOrdinals, Math.Max(4, _freeOrdinals.Length * 2));
        }

        _freeOrdinals[_freeCount++] = ordinal;
    }

    internal List<(long RowId, float[] Vector)> ExportEntries()
    {
        lock (_stateGate)
        {
            var entries = new List<(long RowId, float[] Vector)>(_count);
            for (int ordinal = 0; ordinal < _nextOrdinal; ordinal++)
            {
                if (_isActive[ordinal])
                {
                    entries.Add((_rowIdByOrdinal[ordinal], GetVectorReadOnlySpan(ordinal).ToArray()));
                }
            }

            return entries;
        }
    }

    /// <summary>
    /// Removes all vectors from the index.
    /// </summary>
    public void Clear()
    {
        lock (_stateGate)
        {
            _rowIdToOrdinal.Clear();
            Array.Clear(_isActive, 0, _isActive.Length);
            Array.Clear(_rowIdByOrdinal, 0, _rowIdByOrdinal.Length);
            Array.Clear(_inverseNormByOrdinal, 0, _inverseNormByOrdinal.Length);
            _vectorDimension = -1;
            _nextOrdinal = 0;
            _freeCount = 0;
            _count = 0;
        }
    }

    internal void ImportEntries(IEnumerable<(long RowId, float[] Vector)> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        lock (_stateGate)
        {
            Clear();
            foreach (var entry in entries)
            {
                Insert(entry.RowId, entry.Vector);
            }
        }
    }

    private static void InsertSorted(Span<long> rowIds, Span<float> distances, int lastIndex, long rowId, float distance)
    {
        int index = lastIndex;
        while (index > 0 && IsBetter(distance, rowId, distances[index - 1], rowIds[index - 1]))
        {
            rowIds[index] = rowIds[index - 1];
            distances[index] = distances[index - 1];
            index--;
        }

        rowIds[index] = rowId;
        distances[index] = distance;
    }

    private static bool IsBetter(float leftDistance, long leftRowId, float rightDistance, long rightRowId)
    {
        int distanceComparison = leftDistance.CompareTo(rightDistance);
        return distanceComparison < 0 || distanceComparison == 0 && leftRowId < rightRowId;
    }

    private void EnsureVectorDimension(int dimension)
    {
        if (_vectorDimension < 0)
        {
            _vectorDimension = dimension;
            return;
        }

        if (_vectorDimension != dimension)
        {
            throw new ArgumentException($"Vector dimension mismatch. Expected {_vectorDimension}, got {dimension}.");
        }
    }

    private bool UsesEuclideanMetric()
    {
        return Metric.Equals("l2", StringComparison.OrdinalIgnoreCase)
            || Metric.Equals("euclidean", StringComparison.OrdinalIgnoreCase);
    }

    private static float CosineDistanceFromDot(
        ReadOnlySpan<float> query,
        ReadOnlySpan<float> candidate,
        float queryInvNorm,
        float candidateInvNorm)
    {
        if (queryInvNorm <= 0f || candidateInvNorm <= 0f)
        {
            return 1f;
        }

        return 1f - SimdDistanceKernels.Dot(query, candidate) * queryInvNorm * candidateInvNorm;
    }

    private static float ComputeInverseNorm(ReadOnlySpan<float> vector)
    {
        float norm = System.Numerics.Tensors.TensorPrimitives.Norm(vector);
        return norm > 0f ? 1f / norm : 0f;
    }

    private static void ValidateFiniteVector(ReadOnlySpan<float> vector, string parameterName)
    {
        for (int i = 0; i < vector.Length; i++)
        {
            if (float.IsNaN(vector[i]) || float.IsInfinity(vector[i]))
            {
                throw new ArgumentException("Vector values must be finite.", parameterName);
            }
        }
    }
}
