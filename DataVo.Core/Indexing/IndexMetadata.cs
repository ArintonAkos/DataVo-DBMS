namespace DataVo.Core.Indexing;

/// <summary>
/// Unified metadata for all index types (BTree, HNSW, B25, etc.).
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
    /// Gets or sets the index type: "BTREE", "HNSW", "B25", etc.
    /// </summary>
    public string IndexType { get; set; } = "";

    /// <summary>
    /// Gets or sets the persistence format: "json", "binary", etc.
    /// </summary>
    public string PersistenceFormat { get; set; } = "json";

    /// <summary>
    /// Gets or sets type-specific parameters (e.g., vector dimension for HNSW, degree for BTree).
    /// </summary>
    public Dictionary<string, object> Parameters { get; set; } = [];

    /// <summary>
    /// Gets or sets the creation timestamp (UTC).
    /// </summary>
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Gets or sets the last modification timestamp (UTC).
    /// </summary>
    public DateTime ModifiedAtUtc { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Gets or sets the number of entries in the index.
    /// </summary>
    public long EntryCount { get; set; }

    /// <summary>
    /// Gets or sets whether this is a unique constraint index.
    /// </summary>
    public bool IsUnique { get; set; }

    /// <summary>
    /// Gets or sets whether this is a primary key index.
    /// </summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// Gets a cache key for this index (format: "database/table_index").
    /// </summary>
    public string CacheKey => $"{DatabaseName}/{TableName}_{IndexName}";
}
