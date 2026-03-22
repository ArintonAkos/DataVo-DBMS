namespace DataVo.Core.Indexing;

/// <summary>
/// Factory interface for creating index instances of a specific type and implementation.
/// </summary>
/// <remarks>
/// <para>
/// Each index implementation (BTree, HNSW, B25, etc.) has a corresponding factory
/// responsible for creating new instances with appropriate configuration.
/// </para>
/// <para>
/// Factories abstract away the construction logic and allow <see cref="IndexManager"/>
/// to create indices polymorphically based on type routing.
/// </para>
/// </remarks>
public interface IIndexFactory
{
    /// <summary>
    /// Gets the index type identifier (e.g., "BTREE", "HNSW", "B25").
    /// </summary>
    string IndexType { get; }

    /// <summary>
    /// Creates a new index instance with the given metadata and initial data.
    /// </summary>
    /// <param name="indexName">The logical name of the index.</param>
    /// <param name="columnName">The column(s) this index covers.</param>
    /// <param name="params">Implementation-specific parameters (e.g., vector dimension, M for HNSW).</param>
    /// <returns>A new index instance, ready for use.</returns>
    object CreateIndex(string indexName, string columnName, Dictionary<string, object> @params);

    /// <summary>
    /// Loads an index from disk using the specified persistence handler.
    /// </summary>
    /// <param name="filePath">Path to the serialized index file.</param>
    /// <param name="persistence">The persistence handler for this index type.</param>
    /// <returns>The deserialized index instance.</returns>
    object LoadIndex(string filePath, IIndexPersistence persistence);
}
