using DataVo.Core.BTree;

namespace DataVo.Core.Indexing.BTree;

/// <summary>
/// Factory for creating BTree index instances.
/// </summary>
public class BTreeIndexFactory : IIndexFactory
{
    public string IndexType => "BTREE";

    public object CreateIndex(string indexName, string columnName, Dictionary<string, object> @params)
    {
        // Default minimum degree for B-Tree
        int minDegree = 3;
        
        if (@params.TryGetValue("minDegree", out var val) && val is int degree)
            minDegree = degree;

        // Create a JsonBTreeIndex (in-memory with JSON serialization support)
        return new JsonBTreeIndex(minDegree);
    }

    public object LoadIndex(string filePath, IIndexPersistence persistence)
    {
        // Delegate to persistence handler which knows how to deserialize
        return persistence.LoadIndex(filePath);
    }
}
