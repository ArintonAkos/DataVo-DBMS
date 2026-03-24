namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// HNSW (Hierarchical Navigable Small World) index backend.
/// Implements approximate nearest-neighbor search for vector data.
/// </summary>
/// <remarks>
/// <para>
/// Current implementation maintains layered graph connectivity with configurable M / ef parameters,
/// adaptive search breadth, diversified neighbor selection, and local delete repair.
///
/// The implementation keeps exact-search fallback paths where needed to preserve correctness.
/// </para>
/// </remarks>
public class HNSWIndex : IVectorIndex
{
    private const int DefaultM = 16;
    private const int DefaultEfConstruction = 64;
    private const int DefaultEfSearch = 64;
    private readonly Random _random = new(1337);

    /// <summary>
    /// Gets the index type identifier.
    /// </summary>
    public string IndexType => "HNSW";

    /// <summary>
    /// Gets the distance metric used for this index ("cosine" or "euclidean").
    /// </summary>
    public string Metric { get; set; } = "cosine";

    /// <summary>
    /// Gets the vector entries: rowId -> float array.
    /// </summary>
    public Dictionary<long, float[]> Entries { get; set; } = [];

    /// <summary>
    /// Gets or sets node levels for the HNSW hierarchy.
    /// </summary>
    public Dictionary<long, int> NodeLevels { get; set; } = [];

    /// <summary>
    /// Gets or sets adjacency lists grouped by level, then by node.
    /// </summary>
    public Dictionary<int, Dictionary<long, List<long>>> Layers { get; set; } = [];

    /// <summary>
    /// Gets or sets the current HNSW entry point node identifier.
    /// </summary>
    public long? EntryPointId { get; set; }

    /// <summary>
    /// Gets or sets the highest populated level.
    /// </summary>
    public int MaxLevel { get; set; } = -1;

    /// <summary>
    /// Gets or sets the maximum number of bidirectional neighbors per node and level.
    /// </summary>
    public int M { get; set; } = DefaultM;

    /// <summary>
    /// Gets or sets candidate breadth used during insertion.
    /// </summary>
    public int EfConstruction { get; set; } = DefaultEfConstruction;

    /// <summary>
    /// Gets or sets a value indicating whether insertion-time efConstruction is adaptively scaled.
    /// </summary>
    public bool EnableAdaptiveEfConstruction { get; set; } = true;

    /// <summary>
    /// Gets or sets multiplier used for adaptive efConstruction calculation.
    /// </summary>
    public double AdaptiveEfConstructionMultiplier { get; set; } = 1.25d;

    /// <summary>
    /// Gets or sets candidate breadth used during querying.
    /// </summary>
    public int EfSearch { get; set; } = DefaultEfSearch;

    /// <summary>
    /// Gets or sets a value indicating whether query-time efSearch is adaptively scaled by graph size and requested topK.
    /// </summary>
    public bool EnableAdaptiveEfSearch { get; set; } = true;

    /// <summary>
    /// Gets or sets multiplier used for adaptive efSearch calculation.
    /// </summary>
    public double AdaptiveEfSearchMultiplier { get; set; } = 1.5d;

    /// <summary>
    /// Gets or sets a value indicating whether heuristic diversified neighbor selection is enabled.
    /// </summary>
    public bool EnableDiversityHeuristic { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether delete operations should repair local graph neighborhoods.
    /// </summary>
    public bool EnableDeleteGraphRepair { get; set; } = true;

    /// <summary>
    /// Inserts or updates a vector entry.
    /// </summary>
    public void Insert(long rowId, float[] vector)
    {
        if (vector == null || vector.Length == 0)
        {
            throw new ArgumentException("Vector cannot be null or empty", nameof(vector));
        }

        ValidateDimension(vector);

        if (Entries.ContainsKey(rowId))
        {
            RemoveNode(rowId);
        }

        Entries[rowId] = [.. vector];

        int level = SampleLevel();
        NodeLevels[rowId] = level;
        EnsureNodeInLayers(rowId, level);

        if (EntryPointId is null)
        {
            EntryPointId = rowId;
            MaxLevel = level;
            return;
        }

        long entryPoint = EntryPointId.Value;

        for (int currentLevel = MaxLevel; currentLevel > level; currentLevel--)
        {
            entryPoint = SearchLayerGreedy(vector, entryPoint, currentLevel);
        }

        int maxConnectLevel = Math.Min(level, MaxLevel);
        for (int currentLevel = maxConnectLevel; currentLevel >= 0; currentLevel--)
        {
            int efConstruction = ResolveEffectiveEfConstruction(currentLevel);
            List<long> insertionSeeds = BuildInsertionSeeds(entryPoint, efConstruction, currentLevel);
            List<(long RowId, float Distance)> candidates = SearchLayer(vector, insertionSeeds, efConstruction, currentLevel);
            int neighborLimit = ResolveNeighborLimit(currentLevel);
            List<long> neighbors = SelectNeighbors(candidates, rowId, neighborLimit);

            if (neighbors.Count == 0 && entryPoint != rowId && NodeExistsAtLevel(entryPoint, currentLevel))
            {
                neighbors.Add(entryPoint);
            }

            ConnectNode(rowId, neighbors, currentLevel);
            if (neighbors.Count > 0)
            {
                entryPoint = neighbors[0];
            }
        }

        if (level > MaxLevel)
        {
            EntryPointId = rowId;
            MaxLevel = level;
        }
    }

    /// <summary>
    /// Deletes vector entries by row IDs.
    /// </summary>
    public void Delete(List<long> rowIds)
    {
        foreach (long rowId in rowIds)
        {
            RemoveNode(rowId);
        }
    }

    /// <summary>
    /// Searches for the top-k nearest vectors.
    /// </summary>
    public List<long> SearchTopK(float[] queryVector, int topK)
    {
        if (topK <= 0)
        {
            return [];
        }

        if (queryVector == null || queryVector.Length == 0)
        {
            throw new ArgumentException("Query vector cannot be null or empty", nameof(queryVector));
        }

        if (Entries.Count == 0)
        {
            return [];
        }

        int expectedDimension = Entries.First().Value.Length;
        if (queryVector.Length != expectedDimension)
        {
            return [];
        }

        if (topK >= Entries.Count)
        {
            return ExactSearchTopK(queryVector, topK);
        }

        long entryPoint = ResolveEntryPoint(queryVector);

        for (int level = MaxLevel; level > 0; level--)
        {
            entryPoint = SearchLayerGreedy(queryVector, entryPoint, level);
        }

        int ef = ResolveEffectiveEfSearch(topK);
        List<long> seeds = BuildLevelZeroSeeds(entryPoint, ef);
        List<(long RowId, float Distance)> candidates = SearchLayer(queryVector, seeds, ef, 0);

        if (candidates.Count < topK)
        {
            return ExactSearchTopK(queryVector, topK);
        }

        return candidates
            .OrderBy(item => item.Distance)
            .ThenBy(item => item.RowId)
            .Take(topK)
            .Select(item => item.RowId)
            .ToList();
    }

    private List<long> ExactSearchTopK(float[] queryVector, int topK)
    {
        return Entries
            .Where(entry => entry.Value.Length == queryVector.Length)
            .Select(entry => (entry.Key, Distance(queryVector, entry.Value)))
            .OrderBy(item => item.Item2)
            .ThenBy(item => item.Key)
            .Take(Math.Min(topK, Entries.Count))
            .Select(item => item.Key)
            .ToList();
    }

    /// <summary>
    /// Rebuilds graph layers from current entries.
    /// Useful for loading legacy snapshots that only contain vector payloads.
    /// </summary>
    public void RebuildGraphFromEntries()
    {
        var snapshot = Entries
            .Select(pair => (pair.Key, pair.Value))
            .ToList();

        ResetGraphState();

        foreach (var (rowId, vector) in snapshot)
        {
            Insert(rowId, vector);
        }
    }

    /// <summary>
    /// Gets the count of vectors in this index.
    /// </summary>
    public int Count => Entries.Count;

    /// <summary>
    /// Clears all entries from this index.
    /// </summary>
    public void Clear()
    {
        Entries.Clear();
        ResetGraphState();
    }

    private void ValidateDimension(float[] vector)
    {
        if (Entries.Count == 0)
        {
            return;
        }

        int expected = Entries.First().Value.Length;
        if (vector.Length != expected)
        {
            throw new ArgumentException($"Vector dimension mismatch. Expected {expected}, got {vector.Length}.", nameof(vector));
        }
    }

    private int SampleLevel()
    {
        int level = 0;
        while (_random.NextDouble() < 1.0 / Math.E && level < 32)
        {
            level++;
        }

        return level;
    }

    private void EnsureNodeInLayers(long rowId, int level)
    {
        for (int currentLevel = 0; currentLevel <= level; currentLevel++)
        {
            if (!Layers.TryGetValue(currentLevel, out var levelGraph))
            {
                levelGraph = [];
                Layers[currentLevel] = levelGraph;
            }

            if (!levelGraph.ContainsKey(rowId))
            {
                levelGraph[rowId] = [];
            }
        }
    }

    private long ResolveEntryPoint(float[] queryVector)
    {
        if (EntryPointId.HasValue && Entries.ContainsKey(EntryPointId.Value))
        {
            return EntryPointId.Value;
        }

        long fallback = Entries
            .Select(pair => (pair.Key, Distance(queryVector, pair.Value)))
            .OrderBy(item => item.Item2)
            .ThenBy(item => item.Key)
            .First()
            .Key;

        EntryPointId = fallback;
        MaxLevel = NodeLevels.TryGetValue(fallback, out int level) ? level : 0;
        return fallback;
    }

    private long SearchLayerGreedy(float[] query, long entryPoint, int level)
    {
        long current = entryPoint;
        float currentDistance = Distance(query, Entries[current]);

        bool changed;
        do
        {
            changed = false;

            foreach (long neighbor in GetNeighbors(level, current))
            {
                if (!Entries.TryGetValue(neighbor, out var neighborVector))
                {
                    continue;
                }

                float candidateDistance = Distance(query, neighborVector);
                if (candidateDistance < currentDistance)
                {
                    current = neighbor;
                    currentDistance = candidateDistance;
                    changed = true;
                }
            }
        } while (changed);

        return current;
    }

    private List<long> BuildLevelZeroSeeds(long entryPoint, int ef)
    {
        int maxSeeds = Math.Clamp(Math.Max(2, ef / 8), 2, 16);
        var seeds = new List<long> { entryPoint };

        foreach (long neighbor in GetNeighbors(0, entryPoint))
        {
            if (seeds.Count >= maxSeeds)
            {
                break;
            }

            if (Entries.ContainsKey(neighbor) && !seeds.Contains(neighbor))
            {
                seeds.Add(neighbor);
            }
        }

        if (seeds.Count >= maxSeeds)
        {
            return seeds;
        }

        foreach (long candidate in NodeLevels.Keys.OrderBy(key => key))
        {
            if (seeds.Count >= maxSeeds)
            {
                break;
            }

            if (NodeExistsAtLevel(candidate, 0) && !seeds.Contains(candidate))
            {
                seeds.Add(candidate);
            }
        }

        return seeds;
    }

    private List<long> BuildInsertionSeeds(long entryPoint, int ef, int level)
    {
        int seedBudget = level == 0
            ? Math.Clamp(Math.Max(4, ef / 8), 4, 24)
            : Math.Clamp(Math.Max(2, ef / 16), 2, 12);

        var seeds = new List<long> { entryPoint };

        foreach (long neighbor in GetNeighbors(level, entryPoint))
        {
            if (seeds.Count >= seedBudget)
            {
                break;
            }

            if (Entries.ContainsKey(neighbor) && NodeExistsAtLevel(neighbor, level) && !seeds.Contains(neighbor))
            {
                seeds.Add(neighbor);
            }
        }

        if (seeds.Count >= seedBudget)
        {
            return seeds;
        }

        IEnumerable<long> fallbackPool = NodeLevels
            .Where(pair => pair.Value >= level)
            .OrderByDescending(pair => pair.Value)
            .ThenBy(pair => pair.Key)
            .Select(pair => pair.Key);

        foreach (long candidate in fallbackPool)
        {
            if (seeds.Count >= seedBudget)
            {
                break;
            }

            if (Entries.ContainsKey(candidate) && !seeds.Contains(candidate))
            {
                seeds.Add(candidate);
            }
        }

        return seeds;
    }

    private List<(long RowId, float Distance)> SearchLayer(float[] query, long entryPoint, int ef, int level)
    {
        return SearchLayer(query, [entryPoint], ef, level);
    }

    private List<(long RowId, float Distance)> SearchLayer(float[] query, IReadOnlyCollection<long> entryPoints, int ef, int level)
    {
        ef = Math.Max(1, ef);

        var visited = new HashSet<long>();
        var candidateQueue = new PriorityQueue<(long RowId, float Distance), float>();
        var resultQueue = new PriorityQueue<(long RowId, float Distance), float>();

        void AddResult(long rowId, float distance)
        {
            if (resultQueue.Count < ef)
            {
                resultQueue.Enqueue((rowId, distance), -distance);
                return;
            }

            if (!resultQueue.TryPeek(out _, out float worstPriority))
            {
                return;
            }

            float worstDistance = -worstPriority;
            if (distance >= worstDistance)
            {
                return;
            }

            resultQueue.Dequeue();
            resultQueue.Enqueue((rowId, distance), -distance);
        }

        foreach (long entryPoint in entryPoints)
        {
            if (!Entries.TryGetValue(entryPoint, out var vector))
            {
                continue;
            }

            if (!visited.Add(entryPoint))
            {
                continue;
            }

            float startDistance = Distance(query, vector);
            candidateQueue.Enqueue((entryPoint, startDistance), startDistance);
            AddResult(entryPoint, startDistance);
        }

        if (candidateQueue.Count == 0)
        {
            return [];
        }

        while (candidateQueue.TryDequeue(out var current, out float currentDistance))
        {
            if (resultQueue.Count >= ef
                && resultQueue.TryPeek(out _, out float worstPriority)
                && currentDistance > -worstPriority)
            {
                break;
            }

            foreach (long neighbor in GetNeighbors(level, current.RowId))
            {
                if (!visited.Add(neighbor))
                {
                    continue;
                }

                if (!Entries.TryGetValue(neighbor, out var neighborVector))
                {
                    continue;
                }

                float distance = Distance(query, neighborVector);
                candidateQueue.Enqueue((neighbor, distance), distance);
                AddResult(neighbor, distance);
            }
        }

        return resultQueue.UnorderedItems
            .Select(item => item.Element)
            .OrderBy(item => item.Distance)
            .ThenBy(item => item.RowId)
            .ToList();
    }

    private List<long> SelectNeighbors(List<(long RowId, float Distance)> candidates, long targetNode, int neighborLimit)
    {
        var ordered = candidates
            .Where(candidate => candidate.RowId != targetNode)
            .OrderBy(candidate => candidate.Distance)
            .ThenBy(candidate => candidate.RowId)
            .ToList();

        int limit = Math.Max(1, neighborLimit);
        if (!EnableDiversityHeuristic || ordered.Count <= 1)
        {
            return ordered
                .Take(limit)
                .Select(candidate => candidate.RowId)
                .ToList();
        }

        var selected = new List<(long RowId, float Distance)>(limit);
        foreach (var candidate in ordered)
        {
            if (!Entries.TryGetValue(candidate.RowId, out var candidateVector)
                || !Entries.TryGetValue(targetNode, out var targetVector))
            {
                continue;
            }

            bool occluded = false;
            foreach (var existing in selected)
            {
                if (!Entries.TryGetValue(existing.RowId, out var existingVector))
                {
                    continue;
                }

                float candidateToExisting = Distance(candidateVector, existingVector);
                float candidateToTarget = Distance(candidateVector, targetVector);
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

            selected.Add(candidate);
            if (selected.Count >= limit)
            {
                break;
            }
        }

        if (selected.Count < limit)
        {
            foreach (var candidate in ordered)
            {
                if (selected.Any(item => item.RowId == candidate.RowId))
                {
                    continue;
                }

                selected.Add(candidate);
                if (selected.Count >= limit)
                {
                    break;
                }
            }
        }

        return selected.Select(item => item.RowId).ToList();
    }

    private void ConnectNode(long nodeId, List<long> neighbors, int level)
    {
        List<long> uniqueNeighbors = neighbors
            .Where(neighbor => neighbor != nodeId && Entries.ContainsKey(neighbor) && NodeExistsAtLevel(neighbor, level))
            .Distinct()
            .ToList();

        foreach (long neighbor in uniqueNeighbors)
        {
            AddNeighbor(level, nodeId, neighbor);
            AddNeighbor(level, neighbor, nodeId);
        }
    }

    private void AddNeighbor(int level, long source, long target)
    {
        if (!Layers.TryGetValue(level, out var levelGraph))
        {
            levelGraph = [];
            Layers[level] = levelGraph;
        }

        if (!levelGraph.TryGetValue(source, out var sourceNeighbors))
        {
            sourceNeighbors = [];
            levelGraph[source] = sourceNeighbors;
        }

        if (source == target || sourceNeighbors.Contains(target))
        {
            return;
        }

        sourceNeighbors.Add(target);

        int maxNeighbors = ResolveNeighborLimit(level);
        if (sourceNeighbors.Count <= maxNeighbors)
        {
            return;
        }

        if (!Entries.TryGetValue(source, out var sourceVector))
        {
            return;
        }

        List<(long RowId, float Distance)> rankedCandidates = sourceNeighbors
            .Where(neighbor => Entries.ContainsKey(neighbor) && NodeExistsAtLevel(neighbor, level))
            .Select(neighbor => (RowId: neighbor, Distance: Distance(sourceVector, Entries[neighbor])))
            .OrderBy(candidate => candidate.Distance)
            .ThenBy(candidate => candidate.RowId)
            .ToList();

        List<long> pruned = SelectNeighbors(rankedCandidates, source, maxNeighbors)
            .Take(maxNeighbors)
            .ToList();

        levelGraph[source] = pruned;
    }

    private IReadOnlyList<long> GetNeighbors(int level, long nodeId)
    {
        if (Layers.TryGetValue(level, out var levelGraph)
            && levelGraph.TryGetValue(nodeId, out var neighbors))
        {
            return neighbors;
        }

        return [];
    }

    private bool NodeExistsAtLevel(long nodeId, int level)
    {
        return NodeLevels.TryGetValue(nodeId, out int nodeLevel) && nodeLevel >= level;
    }

    private void RemoveNode(long rowId)
    {
        if (!Entries.ContainsKey(rowId))
        {
            return;
        }

        if (NodeLevels.TryGetValue(rowId, out int level))
        {
            for (int currentLevel = 0; currentLevel <= level; currentLevel++)
            {
                if (!Layers.TryGetValue(currentLevel, out var levelGraph))
                {
                    continue;
                }

                if (levelGraph.TryGetValue(rowId, out var neighbors))
                {
                    List<long> removedNeighbors = [.. neighbors];

                    foreach (long neighbor in neighbors)
                    {
                        if (levelGraph.TryGetValue(neighbor, out var neighborList))
                        {
                            neighborList.Remove(rowId);
                        }
                    }

                    levelGraph.Remove(rowId);

                    if (EnableDeleteGraphRepair)
                    {
                        RepairNeighborhoodAfterDelete(currentLevel, removedNeighbors, rowId);
                    }
                }

                if (levelGraph.Count == 0)
                {
                    Layers.Remove(currentLevel);
                }
            }
        }

        Entries.Remove(rowId);
        NodeLevels.Remove(rowId);

        if (EntryPointId == rowId)
        {
            if (Entries.Count == 0)
            {
                EntryPointId = null;
                MaxLevel = -1;
                return;
            }

            var nextEntry = NodeLevels
                .OrderByDescending(pair => pair.Value)
                .ThenBy(pair => pair.Key)
                .First();

            EntryPointId = nextEntry.Key;
            MaxLevel = nextEntry.Value;
        }
    }

    private void ResetGraphState()
    {
        NodeLevels.Clear();
        Layers.Clear();
        EntryPointId = null;
        MaxLevel = -1;
    }

    private int ResolveEffectiveEfSearch(int topK)
    {
        int maxAllowedEf = Math.Max(1, Entries.Count);
        int baseEf = Math.Min(maxAllowedEf, Math.Max(topK, Math.Max(1, EfSearch)));
        if (!EnableAdaptiveEfSearch || Entries.Count <= 1)
        {
            return baseEf;
        }

        double graphFactor = Math.Log2(Entries.Count + 1);
        double topKFactor = Math.Max(1d, topK * AdaptiveEfSearchMultiplier);
        int adaptiveEf = (int)Math.Ceiling(Math.Max(baseEf, graphFactor * topKFactor));

        return Math.Clamp(adaptiveEf, baseEf, maxAllowedEf);
    }

    private int ResolveEffectiveEfConstruction(int level)
    {
        int maxAllowedEf = Math.Max(1, Entries.Count);
        int baseEf = Math.Min(maxAllowedEf, Math.Max(ResolveNeighborLimit(level), Math.Max(1, EfConstruction)));
        if (!EnableAdaptiveEfConstruction || Entries.Count <= 1)
        {
            return baseEf;
        }

        double graphFactor = Math.Log2(Entries.Count + 1);
        double levelFactor = level == 0 ? 1d : 0.75d;
        double breadth = graphFactor * ResolveNeighborLimit(level) * AdaptiveEfConstructionMultiplier * levelFactor;
        int adaptiveEf = (int)Math.Ceiling(Math.Max(baseEf, breadth));

        return Math.Clamp(adaptiveEf, baseEf, maxAllowedEf);
    }

    private int ResolveNeighborLimit(int level)
    {
        int m = Math.Max(1, M);
        return level == 0 ? Math.Max(2, m * 2) : m;
    }

    private void RepairNeighborhoodAfterDelete(int level, IReadOnlyCollection<long> removedNeighbors, long removedNodeId)
    {
        if (removedNeighbors.Count < 2)
        {
            return;
        }

        if (!Layers.TryGetValue(level, out var levelGraph))
        {
            return;
        }

        List<long> candidates = removedNeighbors
            .Where(nodeId => nodeId != removedNodeId && Entries.ContainsKey(nodeId) && NodeExistsAtLevel(nodeId, level))
            .Distinct()
            .ToList();

        if (candidates.Count < 2)
        {
            return;
        }

        foreach (long source in candidates)
        {
            if (!levelGraph.TryGetValue(source, out var sourceNeighbors))
            {
                sourceNeighbors = [];
                levelGraph[source] = sourceNeighbors;
            }

            List<long> mergeCandidates = sourceNeighbors
                .Where(nodeId => nodeId != removedNodeId && Entries.ContainsKey(nodeId) && NodeExistsAtLevel(nodeId, level))
                .Concat(candidates.Where(nodeId => nodeId != source))
                .Distinct()
                .ToList();

            List<(long RowId, float Distance)> rankedCandidates = mergeCandidates
                .Select(nodeId => (RowId: nodeId, Distance: Distance(Entries[source], Entries[nodeId])))
                .OrderBy(item => item.Distance)
                .ThenBy(item => item.RowId)
                .ToList();

            List<long> selected = SelectNeighbors(rankedCandidates, source, M);
            levelGraph[source] = selected;
        }

        // Ensure bidirectional consistency after local rewiring.
        foreach (long source in candidates)
        {
            if (!levelGraph.TryGetValue(source, out var sourceNeighbors))
            {
                continue;
            }

            foreach (long neighbor in sourceNeighbors)
            {
                if (neighbor != removedNodeId && Entries.ContainsKey(neighbor) && NodeExistsAtLevel(neighbor, level))
                {
                    AddNeighbor(level, neighbor, source);
                }
            }
        }
    }

    private float Distance(float[] a, float[] b)
    {
        return Metric == "cosine"
            ? ComputeCosineDistance(a, b)
            : ComputeEuclideanDistance(a, b);
    }

    // Distance metrics (local implementations to avoid external dependencies)

    private static float ComputeCosineDistance(float[] a, float[] b)
    {
        float dotProduct = 0f;
        float magnitudeA = 0f;
        float magnitudeB = 0f;

        for (int i = 0; i < a.Length; i++)
        {
            dotProduct += a[i] * b[i];
            magnitudeA += a[i] * a[i];
            magnitudeB += b[i] * b[i];
        }

        magnitudeA = MathF.Sqrt(magnitudeA);
        magnitudeB = MathF.Sqrt(magnitudeB);

        if (magnitudeA == 0 || magnitudeB == 0)
        {
            return 1f; // Maximum distance (no similarity)
        }

        float similarity = dotProduct / (magnitudeA * magnitudeB);
        return 1f - similarity; // Convert to distance (0 = identical, 2 = opposite)
    }

    private static float ComputeEuclideanDistance(float[] a, float[] b)
    {
        float sum = 0f;
        for (int i = 0; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }

        return MathF.Sqrt(sum);
    }
}
