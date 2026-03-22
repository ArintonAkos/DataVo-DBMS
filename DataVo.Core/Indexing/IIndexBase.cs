namespace DataVo.Core.Indexing;

/// <summary>
/// Marker interface for all index implementations.
/// </summary>
/// <remarks>
/// <para>
/// All index types (BTree, HNSW, B25, etc.) should implement this interface
/// to be recognizable by <see cref="IndexManager"/>.
/// </para>
/// <para>
/// While the actual operations (Insert, Search, etc.) differ by index type,
/// the factory pattern and metadata handling is uniform.
/// </para>
/// </remarks>
public interface IIndexBase
{
    /// <summary>
    /// Gets the index type identifier.
    /// </summary>
    string IndexType { get; }
}
