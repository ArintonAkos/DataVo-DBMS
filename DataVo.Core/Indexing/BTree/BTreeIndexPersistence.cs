using DataVo.Core.BTree;

namespace DataVo.Core.Indexing.BTree;

/// <summary>
/// Persistence handler for BTree indices (JSON serialization format).
/// </summary>
public class BTreeIndexPersistence : IIndexPersistence
{
    /// <summary>
    /// Gets the serialized file extension for B-Tree index files.
    /// </summary>
    public string FileExtension => ".json";

    /// <summary>
    /// Persists a B-Tree index instance.
    /// </summary>
    public void SaveIndex(object index, string filePath)
    {
        if (index is not JsonBTreeIndex btree)
            throw new ArgumentException($"Expected JsonBTreeIndex but got {index?.GetType().Name}", nameof(index));

        btree.Save(filePath);
    }

    /// <summary>
    /// Loads a B-Tree index instance from storage.
    /// </summary>
    public object LoadIndex(string filePath)
    {
        return JsonBTreeIndex.Load(filePath);
    }

    /// <summary>
    /// Flushes any pending state for B-Tree persistence.
    /// </summary>
    public void Flush(object index)
    {
        // JSON BTree has no additional flushing needed beyond Save
        // (unlike buffered implementations)
    }

    /// <summary>
    /// Returns whether the backing file exists.
    /// </summary>
    public bool FileExists(string filePath)
    {
        return File.Exists(filePath);
    }

    /// <summary>
    /// Ensures the specified directory exists.
    /// </summary>
    public void EnsureDirectory(string directoryPath)
    {
        if (!string.IsNullOrWhiteSpace(directoryPath))
        {
            Directory.CreateDirectory(directoryPath);
        }
    }

    /// <summary>
    /// Attempts to delete the backing index file.
    /// </summary>
    public bool TryDeleteFile(string filePath)
    {
        try
        {
            if (!File.Exists(filePath))
            {
                return true;
            }

            File.Delete(filePath);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Attempts to delete a directory recursively.
    /// </summary>
    public bool TryDeleteDirectory(string directoryPath)
    {
        try
        {
            if (!Directory.Exists(directoryPath))
            {
                return true;
            }

            Directory.Delete(directoryPath, recursive: true);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
