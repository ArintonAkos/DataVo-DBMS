using DataVo.Core.BTree;

namespace DataVo.Core.Indexing.BTree;

/// <summary>
/// Factory for creating BTree index instances.
/// </summary>
public class BTreeIndexFactory : IIndexFactory
{
    /// <summary>
    /// Gets the canonical index type handled by this factory.
    /// </summary>
    public string IndexType => "BTREE";

    /// <summary>
    /// Creates a new B-Tree index instance.
    /// </summary>
    /// <param name="indexName">The logical index name.</param>
    /// <param name="columnName">The indexed column name.</param>
    /// <param name="params">Factory parameters such as minDegree.</param>
    /// <returns>A newly created index instance.</returns>
    public object CreateIndex(string indexName, string columnName, Dictionary<string, object> @params)
    {
        // Default minimum degree for B-Tree
        int minDegree = 3;
        
        if (@params.TryGetValue("minDegree", out var val) && val is int degree)
            minDegree = degree;

        // Create a JsonBTreeIndex (in-memory with JSON serialization support)
        return new JsonBTreeIndex(minDegree);
    }

    /// <summary>
    /// Loads an index instance from persisted storage.
    /// </summary>
    /// <param name="filePath">The index file path.</param>
    /// <param name="persistence">Persistence adapter used for deserialization.</param>
    /// <returns>The loaded index instance.</returns>
    public object LoadIndex(string filePath, IIndexPersistence persistence)
    {
        // Delegate to persistence handler which knows how to deserialize
        return persistence.LoadIndex(filePath);
    }
}
