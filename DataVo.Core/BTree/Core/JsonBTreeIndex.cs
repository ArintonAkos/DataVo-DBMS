using Newtonsoft.Json;
using DataVo.Core.BTree.Core;

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

    /// <summary>
    /// Gets or sets the root node of the B-Tree.
    /// </summary>
    [JsonProperty("root")]
    public BTreeNode<string, long> Root { get; set; } = new BTreeNode<string, long>(minDegree, isLeaf: true);

    /// <summary>
    /// Gets or sets the minimum degree used by the tree.
    /// </summary>
    [JsonProperty("minDegree")]
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
    public void Insert(string key, long value)
    {
        if (Root.IsFull)
        {
            var newRoot = new BTreeNode<string, long>(MinDegree, isLeaf: false);
            newRoot.Children!.Add(Root);
            newRoot.SplitChild(0, Root);

            // Decide which child gets the new key
            int i = 0;
            if (key.CompareTo(newRoot.Keys[0]) == 0)
            {
                newRoot.Values[0].Add(value);
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
    public List<long> Search(string key)
    {
        return Root.Search(key);
    }

    /// <summary>
    /// Determines whether the specified row ID exists anywhere in the index.
    /// </summary>
    /// <param name="rowId">The row ID to search for.</param>
    /// <returns><see langword="true"/> if the row ID is present; otherwise, <see langword="false"/>.</returns>
    public bool ContainsValue(long rowId)
    {
        return Root.ContainsValue(rowId);
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
    public void Delete(string key, long value)
    {
        var allEntries = new List<KeyValuePair<string, List<long>>>();
        Root.CollectAll(allEntries);

        // Remove the specific value from the matching key
        bool modified = false;
        foreach (var entry in allEntries)
        {
            if (entry.Key == key)
            {
                entry.Value.Remove(value);
                modified = true;
                break;
            }
        }

        if (!modified)
        {
            return;
        }

        // Rebuild the tree from the remaining entries
        Root = new BTreeNode<string, long>(MinDegree, isLeaf: true);
        foreach (var entry in allEntries)
        {
            foreach (long val in entry.Value)
            {
                Insert(entry.Key, val);
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
    public void DeleteValues(List<long> valuesToDelete)
    {
        var allEntries = new List<KeyValuePair<string, List<long>>>();
        Root.CollectAll(allEntries);

        var toDeleteSet = new HashSet<long>(valuesToDelete);

        // Filter out the values to delete
        var filtered = new List<KeyValuePair<string, List<long>>>();
        foreach (var entry in allEntries)
        {
            var remaining = entry.Value.Where(v => !toDeleteSet.Contains(v)).ToList();
            if (remaining.Count > 0)
            {
                filtered.Add(new KeyValuePair<string, List<long>>(entry.Key, remaining));
            }
        }

        // Rebuild
        Root = new BTreeNode<string, long>(MinDegree, isLeaf: true);
        foreach (var entry in filtered)
        {
            foreach (long val in entry.Value)
            {
                Insert(entry.Key, val);
            }
        }
    }

    /// <summary>
    /// Serializes the full tree to a JSON file.
    /// </summary>
    /// <param name="filePath">The destination file path.</param>
    public void Save(string filePath)
    {
        string directory = Path.GetDirectoryName(filePath)!;
        if (!Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        string json = JsonConvert.SerializeObject(this, Formatting.Indented, new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.None
        });
        File.WriteAllText(filePath, json);
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
        return JsonConvert.DeserializeObject<JsonBTreeIndex>(json)
               ?? throw new Exception($"Failed to deserialize B-Tree index from: {filePath}");
    }
}
