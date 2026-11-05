namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Binary snapshot of the whole graph, so opening a database does not rebuild it from the rows.
/// </summary>
/// <remarks>
/// Layout: settings, per-node arrays (row id, active flag, level, prune mask), the vectors page by page,
/// then each active node's links for levels 0..its level only. Upper-level slots are mostly empty, so
/// writing the full fixed stride would make the file several times larger. Arrays are written as raw
/// little-endian memory; the caller checks <see cref="BitConverter.IsLittleEndian"/>.
/// </remarks>
public partial class HNSWIndex
{
    private const int SnapshotMaxNodes = int.MaxValue / 2;

    private long _mutations;

    /// <summary>Changes on every insert, delete or clear; equal values mean an unchanged graph.</summary>
    internal long MutationCount => Interlocked.Read(ref _mutations);

    /// <summary>
    /// Compares a loaded snapshot with the table's live rows. <paramref name="scanLiveRows"/> must report
    /// every live row's id and version sequence. Returns the rows to (re)index from the table (written
    /// after <paramref name="watermark"/>) and the indexed rows the table no longer has.
    /// </summary>
    internal (List<long> Reindex, List<long> Removed) PlanCatchUp(Action<Action<long, ulong>> scanLiveRows, long watermark)
    {
        var seen = new bool[_nextOrdinal];
        var reindex = new List<long>();
        ulong unchangedThrough = (ulong)watermark;
        scanLiveRows((rowId, sequence) =>
        {
            if (_rowIdToOrdinal.TryGetValue(rowId, out int ordinal))
            {
                seen[ordinal] = true;
            }

            // A version at or below the watermark is exactly what the snapshot saw: indexed if it had a
            // vector, absent if not. Only newer versions need a look at the row.
            if (sequence > unchangedThrough)
            {
                reindex.Add(rowId);
            }
        });

        var removed = new List<long>();
        for (int ordinal = 0; ordinal < seen.Length; ordinal++)
        {
            if (_isActive[ordinal] && !seen[ordinal])
            {
                removed.Add(_rowIdByOrdinal[ordinal]);
            }
        }

        return (reindex, removed);
    }

    /// <summary>Whether a row is in the index.</summary>
    internal bool ContainsRow(long rowId) => _rowIdToOrdinal.TryGetValue(rowId, out int ordinal) && _isActive[ordinal];

    /// <summary>Writes the index. Must not run concurrently with writes to it.</summary>
    internal void WriteSnapshot(ChecksummedWriter writer)
    {
        writer.WriteString(Metric);
        writer.WriteInt32(M);
        writer.WriteInt32(EfConstruction);
        writer.WriteInt32(MaxAdaptiveEfConstruction);
        writer.WriteInt32(InsertionNeighborhoodPruneHops);
        writer.WriteInt32(EfSearch);
        writer.WriteDouble(AdaptiveEfConstructionMultiplier);
        writer.WriteDouble(InsertionCandidateExpansionFactor);
        writer.WriteDouble(AdaptiveInsertionExpansionMinFactor);
        writer.WriteDouble(AdaptiveInsertionExpansionMaxFactor);
        writer.WriteDouble(InsertionNeighborhoodPruningThreshold);
        writer.WriteDouble(AdaptiveEfSearchMultiplier);
        writer.WriteBoolean(EnableAdaptiveEfConstruction);
        writer.WriteBoolean(EnableInsertionCandidateExpansion);
        writer.WriteBoolean(EnableAdaptiveInsertionCandidateExpansion);
        writer.WriteBoolean(EnableInsertionNeighborhoodPruning);
        writer.WriteBoolean(EnableDiversityHeuristic);
        writer.WriteBoolean(EnableDeleteGraphRepair);
        writer.WriteBoolean(EnableAdaptiveEfSearch);

        writer.WriteBoolean(EntryPointId.HasValue);
        writer.WriteInt64(EntryPointId ?? 0);
        writer.WriteInt32(MaxLevel);
        writer.WriteInt32(_count);
        writer.WriteInt32(_vectorDimension);
        writer.WriteInt32(_nextOrdinal);
        writer.WriteInt32(_levelStrides.Length);
        writer.WriteSpan<int>(_levelStrides);
        writer.WriteSpan<int>(_levelOffsets);

        int nodes = _nextOrdinal;
        writer.WriteSpan<long>(_rowIdByOrdinal.AsSpan(0, nodes));
        writer.WriteSpan<bool>(_isActive.AsSpan(0, nodes));
        writer.WriteSpan<int>(_nodeLevels.AsSpan(0, nodes));
        writer.WriteSpan<ulong>(_diversityPrunedLevelMaskByOrdinal.AsSpan(0, nodes));

        if (_vectorDimension > 0)
        {
            for (int first = 0; first < nodes; first += _vectorPageOrdinals)
            {
                int inPage = Math.Min(_vectorPageOrdinals, nodes - first);
                writer.WriteSpan<float>(_vectorPages[first >> _vectorPageShift].AsSpan(0, inPage * _vectorDimension));
            }
        }

        for (int ordinal = 0; ordinal < nodes; ordinal++)
        {
            if (_isActive[ordinal])
            {
                writer.WriteSpan<int>(NodeLinksThroughLevel(ordinal, _nodeLevels[ordinal]));
            }
        }
    }

    /// <summary>
    /// Reads an index written by <see cref="WriteSnapshot"/>. Throws <see cref="InvalidDataException"/>
    /// for any inconsistency, so a damaged file can never produce a graph that fails at query time.
    /// </summary>
    internal static HNSWIndex ReadSnapshot(ChecksummedReader reader)
    {
        var index = new HNSWIndex
        {
            Metric = reader.ReadString(maxBytes: 64),
            M = Positive(reader.ReadInt32()),
            EfConstruction = Positive(reader.ReadInt32()),
            MaxAdaptiveEfConstruction = Positive(reader.ReadInt32()),
            InsertionNeighborhoodPruneHops = Positive(reader.ReadInt32()),
            EfSearch = Positive(reader.ReadInt32()),
            AdaptiveEfConstructionMultiplier = reader.ReadDouble(),
            InsertionCandidateExpansionFactor = reader.ReadDouble(),
            AdaptiveInsertionExpansionMinFactor = reader.ReadDouble(),
            AdaptiveInsertionExpansionMaxFactor = reader.ReadDouble(),
            InsertionNeighborhoodPruningThreshold = reader.ReadDouble(),
            AdaptiveEfSearchMultiplier = reader.ReadDouble(),
            EnableAdaptiveEfConstruction = reader.ReadBoolean(),
            EnableInsertionCandidateExpansion = reader.ReadBoolean(),
            EnableAdaptiveInsertionCandidateExpansion = reader.ReadBoolean(),
            EnableInsertionNeighborhoodPruning = reader.ReadBoolean(),
            EnableDiversityHeuristic = reader.ReadBoolean(),
            EnableDeleteGraphRepair = reader.ReadBoolean(),
            EnableAdaptiveEfSearch = reader.ReadBoolean(),
        };

        bool hasEntryPoint = reader.ReadBoolean();
        long entryPoint = reader.ReadInt64();
        int maxLevel = reader.ReadInt32();
        int count = reader.ReadInt32();
        int dimension = reader.ReadInt32();
        int nodes = reader.ReadInt32();
        int levels = reader.ReadInt32();
        if (nodes < 0 || nodes > SnapshotMaxNodes || count < 0 || count > nodes
            || levels <= 0 || levels > MaxSupportedLevels
            || (nodes > 0 && dimension <= 0) || dimension > 1 << 20
            || maxLevel < -1 || maxLevel >= levels)
        {
            throw new InvalidDataException("HNSW snapshot header is out of range.");
        }

        var strides = new int[levels];
        var offsets = new int[levels];
        reader.ReadSpan<int>(strides);
        reader.ReadSpan<int>(offsets);
        int stride = 0;
        for (int level = 0; level < levels; level++)
        {
            if (strides[level] <= 0 || offsets[level] != stride)
            {
                throw new InvalidDataException("HNSW snapshot level layout is inconsistent.");
            }

            stride = checked(stride + strides[level]);
        }

        index._vectorDimension = dimension;
        index._levelStrides = strides;
        index._levelOffsets = offsets;
        index._nodeGraphStride = stride;
        index.ComputePageGeometry();
        index.EnsureCapacity(nodes);
        index._nextOrdinal = nodes;

        reader.ReadSpan<long>(index._rowIdByOrdinal.AsSpan(0, nodes));
        var active = new byte[nodes];
        reader.ReadBytes(active);
        reader.ReadSpan<int>(index._nodeLevels.AsSpan(0, nodes));
        reader.ReadSpan<ulong>(index._diversityPrunedLevelMaskByOrdinal.AsSpan(0, nodes));

        int activeCount = 0;
        for (int ordinal = 0; ordinal < nodes; ordinal++)
        {
            if (active[ordinal] > 1 || (uint)index._nodeLevels[ordinal] >= (uint)levels)
            {
                throw new InvalidDataException("HNSW snapshot node table is inconsistent.");
            }

            bool isActive = active[ordinal] == 1;
            index._isActive[ordinal] = isActive;
            if (isActive)
            {
                activeCount++;
                if (!index._rowIdToOrdinal.TryAdd(index._rowIdByOrdinal[ordinal], ordinal))
                {
                    throw new InvalidDataException("HNSW snapshot maps one row to two nodes.");
                }
            }
        }

        if (activeCount != count)
        {
            throw new InvalidDataException("HNSW snapshot node count does not match its active nodes.");
        }

        if (dimension > 0)
        {
            for (int first = 0; first < nodes; first += index._vectorPageOrdinals)
            {
                int inPage = Math.Min(index._vectorPageOrdinals, nodes - first);
                reader.ReadSpan<float>(index._vectorPages[first >> index._vectorPageShift].AsSpan(0, inPage * dimension));
            }
        }

        for (int ordinal = 0; ordinal < nodes; ordinal++)
        {
            if (!index._isActive[ordinal])
            {
                continue;
            }

            Span<int> links = index.NodeLinksThroughLevel(ordinal, index._nodeLevels[ordinal]);
            reader.ReadSpan<int>(links);
            foreach (int neighbor in links)
            {
                // A link may still name a deleted node (edges are not always symmetric); search skips those.
                if (neighbor < -1 || neighbor >= nodes)
                {
                    throw new InvalidDataException("HNSW snapshot links to a missing node.");
                }
            }
        }

        if (hasEntryPoint != (count > 0)
            || (hasEntryPoint && !index._rowIdToOrdinal.ContainsKey(entryPoint)))
        {
            throw new InvalidDataException("HNSW snapshot entry point is missing.");
        }

        for (int ordinal = nodes - 1; ordinal >= 0; ordinal--)
        {
            if (!index._isActive[ordinal])
            {
                index._freeOrdinals.Push(ordinal);
            }
        }

        index._count = count;
        index.EntryPointId = hasEntryPoint ? entryPoint : null;
        index.MaxLevel = maxLevel;
        index.RebuildInverseNorms();
        return index;
    }

    // Levels 0..level of one node are contiguous in its stride (level 0 first).
    private Span<int> NodeLinksThroughLevel(int ordinal, int level)
    {
        int slot = (ordinal & _graphPageMask) * _nodeGraphStride;
        int length = _levelOffsets[level] + _levelStrides[level];
        return _graphPages[ordinal >> _graphPageShift].AsSpan(slot, length);
    }

    private static int Positive(int value) =>
        value > 0 ? value : throw new InvalidDataException("HNSW snapshot setting must be positive.");
}
