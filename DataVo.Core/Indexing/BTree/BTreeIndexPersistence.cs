using DataVo.Core.BTree;

namespace DataVo.Core.Indexing.BTree;

/// <summary>
/// Persistence handler for BTree indices (JSON serialization format).
/// </summary>
public class BTreeIndexPersistence : IIndexPersistence
{
    public string FileExtension => ".json";

    public void SaveIndex(object index, string filePath)
    {
        if (index is not JsonBTreeIndex btree)
            throw new ArgumentException($"Expected JsonBTreeIndex but got {index?.GetType().Name}", nameof(index));

        btree.Save(filePath);
    }

    public object LoadIndex(string filePath)
    {
        return JsonBTreeIndex.Load(filePath);
    }

    public void Flush(object index)
    {
        // JSON BTree has no additional flushing needed beyond Save
        // (unlike buffered implementations)
    }

    public bool FileExists(string filePath)
    {
        return File.Exists(filePath);
    }
}
