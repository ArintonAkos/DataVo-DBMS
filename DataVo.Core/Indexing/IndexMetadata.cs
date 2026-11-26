namespace DataVo.Core.Indexing;

/// <summary>
/// Unified metadata for all index types (BTREE, HNSW, FLAT).
/// </summary>
/// <remarks>
/// <para>
/// This metadata is stored separately from the index data structure itself
/// and allows IndexManager to understand index properties without loading
/// the entire index into memory.
/// </para>
/// </remarks>
public class IndexMetadata
{
    /// <summary>
    /// Gets or sets the logical index name (e.g., "idx_user_email").
    /// </summary>
    public string IndexName { get; set; } = "";

    /// <summary>
    /// Gets or sets the database name this index belongs to.
    /// </summary>
    public string DatabaseName { get; set; } = "";

    /// <summary>
    /// Gets or sets the table name this index covers.
    /// </summary>
    public string TableName { get; set; } = "";

    /// <summary>
    /// Gets or sets the column(s) this index covers.
    /// </summary>
    public string ColumnName { get; set; } = "";

    /// <summary>
    /// Gets or sets the index type: "BTREE", "HNSW", or "FLAT"
    /// </summary>
    public string IndexType { get; set; } = "";

    /// <summary>
    /// Gets or sets type-specific parameters (e.g., vector dimension for HNSW, degree for BTree).
    /// </summary>
    public Dictionary<string, object> Parameters { get; set; } = [];
}
