using System.Buffers;

namespace DataVo.Core.Indexing.HNSW;

public partial class HNSWIndex
{
    /// <summary>
    /// Makes every node reachable from the entry point on every level it lives on.
    /// </summary>
    /// <remarks>
    /// Inserts that run at the same time cannot see each other. With the diversity heuristic on, two
    /// close nodes inserted together can each have their reverse edges rejected at every neighbor,
    /// because the other one already covers that direction - and then no search can reach them.
    /// A sequential build never does this: a node's nearest existing neighbor cannot be occluded by
    /// anything closer. After a parallel batch, this pass walks each level from the entry point and
    /// links every node it did not reach to the nearest node it did. When that node's list is full
    /// it gives up an edge only to a node that another reached node also points at, so no node that
    /// was reachable stops being reachable. It runs on one thread and is linear in the edge count.
    /// </remarks>
    /// <returns>The number of edges added to reconnect unreachable nodes.</returns>
    internal int RepairConnectivity()
    {
        lock (_levelPromotionGate)
        {
            int entry = TryGetEntryOrdinal();
            int nodeCount = _nextOrdinal;
            if (entry < 0 || nodeCount <= 1)
            {
                return 0;
            }

            int[] reached = ArrayPool<int>.Shared.Rent(nodeCount);
            int[] inDegree = ArrayPool<int>.Shared.Rent(nodeCount);
            int[] queue = ArrayPool<int>.Shared.Rent(nodeCount);
            int repaired = 0;
            try
            {
                for (int level = Math.Min(MaxLevel, _nodeLevels[entry]); level >= 0; level--)
                {
                    Array.Clear(reached, 0, nodeCount);
                    Array.Clear(inDegree, 0, nodeCount);
                    MarkReachable(entry, level, reached, inDegree, queue);

                    for (int ordinal = 0; ordinal < nodeCount; ordinal++)
                    {
                        if (reached[ordinal] != 0 || !_isActive[ordinal] || _nodeLevels[ordinal] < level)
                        {
                            continue;
                        }

                        if (TryLinkFromReachable(ordinal, entry, level, reached, inDegree))
                        {
                            repaired++;
                            inDegree[ordinal]++;
                            MarkReachable(ordinal, level, reached, inDegree, queue);
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(reached);
                ArrayPool<int>.Shared.Return(inDegree);
                ArrayPool<int>.Shared.Return(queue);
            }

            return repaired;
        }
    }

    /// <summary>
    /// Counts, over every level, the nodes on that level that cannot be reached from the entry
    /// point by following edges. A healthy graph returns zero.
    /// </summary>
    internal int CountUnreachableNodes()
    {
        lock (_levelPromotionGate)
        {
            int entry = TryGetEntryOrdinal();
            int nodeCount = _nextOrdinal;
            if (entry < 0)
            {
                return 0;
            }

            int[] reached = new int[nodeCount];
            int[] inDegree = new int[nodeCount];
            int[] queue = new int[nodeCount];
            int unreachable = 0;
            for (int level = MaxLevel; level >= 0; level--)
            {
                Array.Clear(reached, 0, nodeCount);
                MarkReachable(entry, level, reached, inDegree, queue);
                for (int ordinal = 0; ordinal < nodeCount; ordinal++)
                {
                    if (reached[ordinal] == 0 && _isActive[ordinal] && _nodeLevels[ordinal] >= level)
                    {
                        unreachable++;
                    }
                }
            }

            return unreachable;
        }
    }

    // Breadth-first walk of one level from start. Marks what it reaches and counts, for every node,
    // how many reached nodes point at it.
    private void MarkReachable(int start, int level, int[] reached, int[] inDegree, int[] queue)
    {
        if (reached[start] != 0)
        {
            return;
        }

        reached[start] = 1;
        int head = 0;
        int tail = 0;
        queue[tail++] = start;

        while (head < tail)
        {
            int source = queue[head++];
            Span<int> neighbors = GetNeighborSpan(source, level);
            for (int i = 0; i < neighbors.Length; i++)
            {
                int neighbor = neighbors[i];
                if (neighbor < 0)
                {
                    break;
                }

                if ((uint)neighbor >= (uint)reached.Length || !_isActive[neighbor] || _nodeLevels[neighbor] < level)
                {
                    continue;
                }

                inDegree[neighbor]++;
                if (reached[neighbor] == 0)
                {
                    reached[neighbor] = 1;
                    queue[tail++] = neighbor;
                }
            }
        }
    }

    // Adds an edge to orphan from the nearest reached node that can take one.
    private bool TryLinkFromReachable(int orphan, int entry, int level, int[] reached, int[] inDegree)
    {
        ReadOnlySpan<float> vector = GetVector(orphan);
        float inverseNorm = _inverseNormByOrdinal[orphan];

        // Both searches start at the entry point and only follow edges, so everything they return
        // is reached on this level.
        int start = SearchGreedy(vector, inverseNorm, entry, level);
        var workspace = GetSearchWorkspace();
        int candidateCount = SearchLayer(vector, inverseNorm, start, ResolveEffectiveEfConstruction(level), level, workspace);

        for (int i = 0; i < candidateCount; i++)
        {
            int candidate = workspace.ResultOrdinals[i];
            if (candidate != orphan && reached[candidate] != 0 && TryAddRepairEdge(candidate, orphan, level, inDegree))
            {
                return true;
            }
        }

        // The nearest candidates could not give up an edge (very unlikely); try every reached node.
        for (int candidate = 0; candidate < _nextOrdinal; candidate++)
        {
            if (candidate != orphan && reached[candidate] != 0 && TryAddRepairEdge(candidate, orphan, level, inDegree))
            {
                return true;
            }
        }

        return false;
    }

    private bool TryAddRepairEdge(int source, int target, int level, int[] inDegree)
    {
        object? nodeLock = TryGetNodeLock(source);
        if (nodeLock == null)
        {
            return false;
        }

        lock (nodeLock)
        {
            Span<int> neighbors = GetNeighborSpan(source, level);
            int evictAt = -1;
            float evictDistance = float.NegativeInfinity;
            for (int i = 0; i < neighbors.Length; i++)
            {
                int neighbor = neighbors[i];
                if (neighbor < 0)
                {
                    Volatile.Write(ref neighbors[i], target);
                    return true;
                }

                // Dropping this edge keeps the neighbor reachable only if another reached node
                // still points at it. Of those, give up the farthest.
                if (inDegree[neighbor] >= 2)
                {
                    float distance = DistanceBetweenOrdinals(source, neighbor);
                    if (distance > evictDistance)
                    {
                        evictDistance = distance;
                        evictAt = i;
                    }
                }
            }

            if (evictAt < 0)
            {
                return false;
            }

            inDegree[neighbors[evictAt]]--;
            Volatile.Write(ref neighbors[evictAt], target);
            ClearDiversityPruned(source, level);
            return true;
        }
    }
}
