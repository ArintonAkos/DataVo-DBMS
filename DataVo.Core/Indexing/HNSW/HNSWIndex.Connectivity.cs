using System.Buffers;

namespace DataVo.Core.Indexing.HNSW;

public partial class HNSWIndex
{
    // Spanning-tree markers in the repair walk: a node not reached yet, and the walk's root.
    private const int Unreached = -2;
    private const int TreeRoot = -1;

    /// <summary>
    /// Makes every node reachable from the entry point on every level it lives on.
    /// </summary>
    /// <remarks>
    /// Inserts that run at the same time cannot see each other. With the diversity heuristic on, two
    /// close nodes inserted together can each have their reverse edges rejected at every neighbor,
    /// because the other one already covers that direction - and then no search can reach them.
    /// A sequential build never does this: a node's nearest existing neighbor cannot be occluded by
    /// anything closer. After a parallel batch, this pass walks each level from the entry point and
    /// links every node it did not reach to the nearest node it did. The walk records the edge it
    /// first reached each node through; together those edges form a tree that spans every reached
    /// node. When the linking node's list is full it gives up an edge outside that tree, so no node
    /// that was reachable stops being reachable. (Giving up an edge to any node with two incoming
    /// edges is not enough: in a tight cluster the nodes point at each other, and the one edge into
    /// the cluster can go.) It runs on one thread and is linear in the edge count.
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

            int[] parent = ArrayPool<int>.Shared.Rent(nodeCount);
            int[] queue = ArrayPool<int>.Shared.Rent(nodeCount);
            int repaired = 0;
            try
            {
                for (int level = Math.Min(MaxLevel, _nodeLevels[entry]); level >= 0; level--)
                {
                    Array.Fill(parent, Unreached, 0, nodeCount);
                    MarkReachable(entry, TreeRoot, level, parent, queue);

                    for (int ordinal = 0; ordinal < nodeCount; ordinal++)
                    {
                        if (parent[ordinal] != Unreached || !_isActive[ordinal] || _nodeLevels[ordinal] < level)
                        {
                            continue;
                        }

                        int linkedFrom = TryLinkFromReachable(ordinal, entry, level, parent);
                        if (linkedFrom >= 0)
                        {
                            repaired++;
                            MarkReachable(ordinal, linkedFrom, level, parent, queue);
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(parent);
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

            int[] parent = new int[nodeCount];
            int[] queue = new int[nodeCount];
            int unreachable = 0;
            for (int level = MaxLevel; level >= 0; level--)
            {
                Array.Fill(parent, Unreached);
                MarkReachable(entry, TreeRoot, level, parent, queue);
                for (int ordinal = 0; ordinal < nodeCount; ordinal++)
                {
                    if (parent[ordinal] == Unreached && _isActive[ordinal] && _nodeLevels[ordinal] >= level)
                    {
                        unreachable++;
                    }
                }
            }

            return unreachable;
        }
    }

    // Breadth-first walk of one level from start, reached through the edge from startParent. Records,
    // for every node it reaches, the node whose edge reached it first: those edges form a spanning tree.
    private void MarkReachable(int start, int startParent, int level, int[] parent, int[] queue)
    {
        if (parent[start] != Unreached)
        {
            return;
        }

        parent[start] = startParent;
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

                if ((uint)neighbor >= (uint)parent.Length || !_isActive[neighbor] || _nodeLevels[neighbor] < level)
                {
                    continue;
                }

                if (parent[neighbor] == Unreached)
                {
                    parent[neighbor] = source;
                    queue[tail++] = neighbor;
                }
            }
        }
    }

    // Adds an edge to orphan from the nearest reached node that can take one; returns that node, or -1.
    private int TryLinkFromReachable(int orphan, int entry, int level, int[] parent)
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
            if (candidate != orphan && parent[candidate] != Unreached && TryAddRepairEdge(candidate, orphan, level, parent))
            {
                return candidate;
            }
        }

        // The nearest candidates could not give up an edge (very unlikely); try every reached node.
        for (int candidate = 0; candidate < _nextOrdinal; candidate++)
        {
            if (candidate != orphan && parent[candidate] != Unreached && TryAddRepairEdge(candidate, orphan, level, parent))
            {
                return candidate;
            }
        }

        return -1;
    }

    private bool TryAddRepairEdge(int source, int target, int level, int[] parent)
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

                // A tree edge is the neighbor's guaranteed way in; any other edge can go. Of
                // those, give up the farthest.
                if ((uint)neighbor >= (uint)parent.Length || parent[neighbor] != source)
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

            Volatile.Write(ref neighbors[evictAt], target);
            ClearDiversityPruned(source, level);
            return true;
        }
    }
}
