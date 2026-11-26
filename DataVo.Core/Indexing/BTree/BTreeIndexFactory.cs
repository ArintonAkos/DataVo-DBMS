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

        return new InMemoryBTreeIndex(minDegree);
    }
}
