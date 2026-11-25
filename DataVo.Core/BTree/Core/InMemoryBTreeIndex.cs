using System.Text.Json;
using System.Text.Json.Serialization;
using DataVo.Core.BTree.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.Serialization;

namespace DataVo.Core.BTree;

/// <summary>
/// Implements an <see cref="IIndex"/> using an in-memory generic B-Tree that is serialized to JSON on disk.
/// </summary>
/// <remarks>
/// This implementation favors simplicity and debuggability over write efficiency.
/// Mutations occur in memory and the full structure is serialized when <see cref="Save(string)"/> is called.
/// </remarks>
public class JsonBTreeIndex(int minDegree) : IIndex
{
    private const int DefaultMinDegree = 50;
    private readonly object _treeLock = new();

    /// <summary>
    /// Gets or sets the root node of the B-Tree.
    /// </summary>
    [JsonPropertyName("root")]
    public BTreeNode<string, RowKey> Root { get; set; } = new BTreeNode<string, RowKey>(minDegree, isLeaf: true);

    /// <summary>
    /// Gets or sets the minimum degree used by the tree.
    /// </summary>
    [JsonPropertyName("minDegree")]
    public int MinDegree { get; set; } = minDegree;

    /// <summary>
    /// Initializes a new instance of the <see cref="JsonBTreeIndex"/> class using the default minimum degree.
    /// </summary>
    public JsonBTreeIndex() : this(DefaultMinDegree) { }

    /// <summary>
    /// Inserts a logical key-to-row mapping into the index.
    /// </summary>
    /// <param name="key">The logical index key.</param>
    /// <param name="value">The row ID to associate with the key.</param>
    /// <remarks>
    /// If the root node is full, a new root is created and the old root is split before the insertion continues.
    /// Duplicate keys are supported and append the row ID to the key's value list.
    /// </remarks>
    public void Insert(string key, RowKey value)
    {
        lock (_treeLock)
        {
            InsertCore(key, value);
        }
    }

    private void InsertCore(string key, RowKey value)
    {
        if (Root.IsFull)
        {
            var newRoot = new BTreeNode<string, RowKey>(MinDegree, isLeaf: false);
            newRoot.Children!.Add(Root);
            newRoot.SplitChild(0, Root);

            // Decide which child gets the new key
            int i = 0;
            if (key.CompareTo(newRoot.Keys[0]) == 0)
            {
                // Copy-on-write append: the promoted median's value list may already be published to a reader.
                newRoot.Values[0] = [.. newRoot.Values[0], value];
                Root = newRoot;
                return;
            }

            if (key.CompareTo(newRoot.Keys[0]) > 0)
            {
                i = 1;
            }

            newRoot.Children[i].InsertNonFull(key, value);
            Root = newRoot;
        }
        else
        {
            Root.InsertNonFull(key, value);
        }
    }

    /// <summary>
    /// Returns all row IDs associated with the specified key.
    /// </summary>
    /// <param name="key">The logical key to search for.</param>
    /// <returns>A list of matching row IDs, or an empty list if the key is not present.</returns>
    public List<RowKey> Search(string key)
    {
        lock (_treeLock)
        {
            return Root.Search(key);
        }
    }

    /// <summary>
    /// Point lookup that returns the matching row IDs without copying the value list.
    /// The traversal runs under the tree lock; the returned snapshot stays valid because published
    /// value lists are never mutated in place (duplicate-key appends replace the reference instead).
    /// </summary>
    /// <param name="key">The logical key to search for.</param>
    /// <returns>The matching row IDs as a read-only list, or an empty list if the key is not present.</returns>
    public IReadOnlyList<RowKey> SearchReadOnly(string key)
    {
        lock (_treeLock)
        {
            return Root.SearchReadOnly(key);
        }
    }

    /// <summary>
    /// Determines whether the specified logical key exists without copying the associated row IDs.
    /// </summary>
    /// <param name="key">The logical key to search for.</param>
    /// <returns><see langword="true"/> if the key exists; otherwise, <see langword="false"/>.</returns>
    public bool ContainsKey(string key)
    {
        lock (_treeLock)
        {
            // A key whose rows were all deleted remains as an empty entry until compaction.
            return Root.SearchReadOnly(key).Count > 0;
        }
    }

    /// <summary>
    /// Determines whether the specified row ID exists anywhere in the index.
    /// </summary>
    /// <param name="rowId">The row ID to search for.</param>
    /// <returns><see langword="true"/> if the row ID is present; otherwise, <see langword="false"/>.</returns>
    public bool ContainsValue(RowKey rowId)
    {
        lock (_treeLock)
        {
            return Root.ContainsValue(rowId);
        }
    }

    /// <summary>
    /// Removes a specific key/value mapping from the index by rebuilding the tree from the remaining entries.
    /// </summary>
    /// <param name="key">The key whose mapping should be removed.</param>
    /// <param name="value">The specific row ID to remove from the key.</param>
    /// <remarks>
    /// This helper is not part of <see cref="IIndex"/>, but is available for scenarios that require targeted removal.
    /// The implementation performs an in-order traversal, filters the matching mapping, and rebuilds the tree.
    /// </remarks>
    public void Delete(string key, RowKey value)
    {
        lock (_treeLock)
        {
            var allEntries = new List<KeyValuePair<string, List<RowKey>>>();
            Root.CollectAll(allEntries);

            // Remove the specific value from the matching key. Copy-on-write: filter into a fresh list
            // rather than mutating the collected list in place, which may already be published to a reader.
            bool modified = false;
            for (int idx = 0; idx < allEntries.Count; idx++)
            {
                if (allEntries[idx].Key == key)
                {
                    var filtered = new List<RowKey>(allEntries[idx].Value);
                    filtered.Remove(value);
                    allEntries[idx] = new KeyValuePair<string, List<RowKey>>(key, filtered);
                    modified = true;
                    break;
                }
            }

            if (!modified)
            {
                return;
            }

            // Rebuild the tree from the remaining entries
            Root = new BTreeNode<string, RowKey>(MinDegree, isLeaf: true);
            foreach (var entry in allEntries)
            {
                foreach (RowKey val in entry.Value)
                {
                    InsertCore(entry.Key, val);
                }
            }
        }
    }

    /// <summary>
    /// Removes all occurrences of the specified row IDs, regardless of which keys reference them.
    /// </summary>
    /// <param name="valuesToDelete">The row IDs to remove from every key bucket.</param>
    /// <remarks>
    /// The tree is rebuilt from the filtered entries after the removals are applied.
    /// </remarks>
    public void DeleteValues(List<RowKey> valuesToDelete)
    {
        if (valuesToDelete.Count == 0)
        {
            return;
        }

        lock (_treeLock)
        {
            // Remove the row ids in place instead of rebuilding the whole tree on every delete (that
            // rebuild made each UPDATE cost O(n log n) time and megabytes of garbage). A key whose last
            // row id goes away stays as an empty entry until the next compaction.
            HashSet<RowKey>? many = valuesToDelete.Count > 1 ? [.. valuesToDelete] : null;
            _emptyKeys += RemoveValues(Root, valuesToDelete[0], many);
            if (_emptyKeys >= CompactionThreshold)
            {
                Compact();
            }
        }
    }

    /// <summary>
    /// Removes one (key, row id) mapping by descending to the key: O(log n), unlike
    /// <see cref="DeleteValues"/>, which has to visit every key because it is not given one.
    /// </summary>
    /// <returns><see langword="true"/> when the mapping existed.</returns>
    public bool RemoveValue(string key, RowKey rowId)
    {
        lock (_treeLock)
        {
            BTreeNode<string, RowKey>? node = Root;
            while (node is not null)
            {
                int i = 0;
                while (i < node.Keys.Count && key.CompareTo(node.Keys[i]) > 0)
                {
                    i++;
                }

                if (i < node.Keys.Count && key.CompareTo(node.Keys[i]) == 0)
                {
                    List<RowKey> rowIds = node.Values[i];
                    if (!rowIds.Contains(rowId))
                    {
                        return false;
                    }

                    // Copy-on-write: the old list may already be published to a lock-free reader.
                    var remaining = new List<RowKey>(rowIds.Count);
                    foreach (RowKey existing in rowIds)
                    {
                        if (existing != rowId)
                        {
                            remaining.Add(existing);
                        }
                    }

                    node.Values[i] = remaining;
                    if (remaining.Count == 0 && ++_emptyKeys >= CompactionThreshold)
                    {
                        Compact();
                    }

                    return true;
                }

                node = node.IsLeaf ? null : node.Children![i];
            }

            return false;
        }
    }

    private const int CompactionThreshold = 4096;
    private int _emptyKeys;

    private static int RemoveValues(BTreeNode<string, RowKey> node, RowKey single, HashSet<RowKey>? many)
    {
        int emptied = 0;
        for (int i = 0; i < node.Values.Count; i++)
        {
            List<RowKey> rowIds = node.Values[i];
            bool hit = false;
            foreach (RowKey rowId in rowIds)
            {
                if (many is null ? rowId == single : many.Contains(rowId))
                {
                    hit = true;
                    break;
                }
            }

            if (!hit)
            {
                continue;
            }

            // Copy-on-write: the old list may already be published to a lock-free reader.
            var remaining = new List<RowKey>(rowIds.Count);
            foreach (RowKey rowId in rowIds)
            {
                if (!(many is null ? rowId == single : many.Contains(rowId)))
                {
                    remaining.Add(rowId);
                }
            }

            node.Values[i] = remaining;
            if (remaining.Count == 0)
            {
                emptied++;
            }
        }

        if (node.Children is { } children)
        {
            foreach (BTreeNode<string, RowKey> child in children)
            {
                emptied += RemoveValues(child, single, many);
            }
        }

        return emptied;
    }

    /// <summary>Rebuilds the tree without keys whose row-id lists became empty.</summary>
    private void Compact()
    {
        var allEntries = new List<KeyValuePair<string, List<RowKey>>>();
        Root.CollectAll(allEntries);
        Root = new BTreeNode<string, RowKey>(MinDegree, isLeaf: true);
        foreach (KeyValuePair<string, List<RowKey>> entry in allEntries)
        {
            foreach (RowKey rowId in entry.Value)
            {
                InsertCore(entry.Key, rowId);
            }
        }

        _emptyKeys = 0;
    }

    /// <summary>
    /// Serializes the full tree to a JSON file.
    /// </summary>
    /// <param name="filePath">The destination file path.</param>
    public void Save(string filePath)
    {
        lock (_treeLock)
        {
            string directory = Path.GetDirectoryName(filePath)!;
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            string json = JsonSerializer.Serialize(this, DataVoJsonContext.Default.JsonBTreeIndex);
            File.WriteAllText(filePath, json);
        }
    }

    /// <summary>
    /// Loads a <see cref="JsonBTreeIndex"/> from a JSON file.
    /// </summary>
    /// <param name="filePath">The JSON file to read.</param>
    /// <returns>The deserialized index instance.</returns>
    /// <exception cref="FileNotFoundException">Thrown when <paramref name="filePath"/> does not exist.</exception>
    /// <exception cref="Exception">Thrown when deserialization returns <see langword="null"/>.</exception>
    public static JsonBTreeIndex Load(string filePath)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"B-Tree index file not found: {filePath}");
        }

        string json = File.ReadAllText(filePath);
        return JsonSerializer.Deserialize(json, DataVoJsonContext.Default.JsonBTreeIndex)
             ?? throw new IndexException($"Failed to deserialize B-Tree index from: {filePath}");
    }
}
