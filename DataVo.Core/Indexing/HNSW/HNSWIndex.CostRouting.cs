namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Chooses between the graph search and an exact scan of the stored vectors by estimated cost.
/// </summary>
/// <remarks>
/// A filtered query asks for many more candidates than it returns (about <c>LIMIT / selectivity</c>), and the
/// graph search widens its beam with the request: 286 candidates over 50,000 vectors search with a beam of
/// about 6,700. Measured on 50,000 x 384 and 20,000 x 1,536 clustered vectors, the graph search costs a fixed
/// amount per beam slot (queue work and cache misses, about 550 ns) plus about 2.4 distance computations, and
/// an exact scan costs one distance computation and a little loop work per stored vector. That puts the
/// break-even beam at roughly 9% of the vectors for 384 dimensions and 20% for 1,536. The scan is also exact,
/// so taking it never lowers recall.
/// </remarks>
public partial class HNSWIndex
{
    /// <summary>The fixed cost of one beam slot of a graph search, expressed in vector dimensions of distance work.</summary>
    private const double GraphSearchFixedCostDimensions = 4300d;

    /// <summary>Distance computations a graph search makes per beam slot.</summary>
    private const double GraphSearchDistancesPerBeamSlot = 2.4d;

    /// <summary>The loop work of an exact scan per stored vector, in vector dimensions of distance work.</summary>
    private const double ExactScanFixedCostDimensions = 100d;

    /// <summary>
    /// Gets or sets whether a search is answered by an exact scan when that is estimated to be cheaper than
    /// the graph search. On by default; turn it off to always search the graph.
    /// </summary>
    public bool EnableExactScanCostRouting { get; set; } = true;

    /// <summary>
    /// Whether a search for <paramref name="topK"/> neighbors is answered by an exact scan of the stored
    /// vectors instead of the graph: when the request covers the whole index, or the beam it needs makes the
    /// graph search the more expensive of the two.
    /// </summary>
    /// <example>
    /// With 50,000 vectors of 384 dimensions, <c>PrefersExactScan(10)</c> is false (a beam of 234) and
    /// <c>PrefersExactScan(286)</c> is true (a beam of about 6,700).
    /// </example>
    internal bool PrefersExactScan(int topK)
    {
        if (topK <= 0 || _count == 0)
        {
            return false;
        }

        if (topK >= _count)
        {
            return true;
        }

        return EnableExactScanCostRouting && IsExactScanCheaper(ResolveEffectiveEfSearch(topK));
    }

    /// <summary>Compares the estimated cost of a graph search with a beam of <paramref name="ef"/> to an exact scan.</summary>
    private bool IsExactScanCheaper(int ef)
    {
        double dimensions = Math.Max(1, _vectorDimension);
        double graphCost = ef * (GraphSearchFixedCostDimensions + GraphSearchDistancesPerBeamSlot * dimensions);
        double exactCost = _count * (ExactScanFixedCostDimensions + dimensions);
        return graphCost >= exactCost;
    }
}
