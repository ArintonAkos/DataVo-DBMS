namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Browser runtime flags used to toggle HNSW behavior in constrained environments.
/// </summary>
public static class BrowserRuntimeFlags
{
    /// <summary>
    /// Forces vector operations to use fallback execution paths.
    /// </summary>
    public static bool ForceVectorFallback { get; set; }
}