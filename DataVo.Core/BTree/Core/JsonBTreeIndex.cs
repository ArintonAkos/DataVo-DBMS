using Newtonsoft.Json;
using DataVo.Core.BTree.Core;

namespace DataVo.Core.BTree;

/// <summary>
/// B-Tree index wrapping a BTreeNode{string, string}.
/// Keys are index column values, values are row IDs.
/// Supports JSON persistence to .btree files.
/// </summary>
public class JsonBTreeIndex : IIndex
{
    private const int DefaultMinDegree = 50;

    [JsonProperty("root")]
    public BTreeNode<string, string> Root { get; set; }

    [JsonProperty("minDegree")]
    public int MinDegree { get; set; }

    public JsonBTreeIndex() : this(DefaultMinDegree) { }

    public JsonBTreeIndex(int minDegree)
    {
        MinDegree = minDegree;
        Root = new BTreeNode<string, string>(minDegree, isLeaf: true);
    }

    /// <summary>
    /// Insert a key-value pair into the index.
    /// Handles root split when the root is full.
    /// </summary>
    public void Insert(string key, string value)
    {
        if (Root.IsFull)
        {
            var newRoot = new BTreeNode<string, string>(MinDegree, isLeaf: false);
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
    /// Search for all row IDs associated with the given key.
    /// </summary>
    public List<string> Search(string key)
    {
        return Root.Search(key);
    }

    /// <summary>
    /// Check if any value exists for the given key.
    /// </summary>
    public bool ContainsValue(string key)
    {
        return Root.ContainsKey(key);
    }

    /// <summary>
    /// Delete a specific (key, value) pair from the index.
    /// Uses a lazy rebuild approach: collects all entries, removes matching, and rebuilds.
    /// </summary>
    public void Delete(string key, string value)
    {
        var allEntries = new List<KeyValuePair<string, List<string>>>();
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
        Root = new BTreeNode<string, string>(MinDegree, isLeaf: true);
        foreach (var entry in allEntries)
        {
            foreach (string val in entry.Value)
            {
                Insert(entry.Key, val);
            }
        }
    }

    /// <summary>
    /// Delete all entries matching any of the given values, regardless of key.
    /// Used when deleting rows from a table — removes those row IDs from all index entries.
    /// </summary>
    public void DeleteValues(List<string> valuesToDelete)
    {
        var allEntries = new List<KeyValuePair<string, List<string>>>();
        Root.CollectAll(allEntries);

        var toDeleteSet = new HashSet<string>(valuesToDelete);

        // Filter out the values to delete
        var filtered = new List<KeyValuePair<string, List<string>>>();
        foreach (var entry in allEntries)
        {
            var remaining = entry.Value.Where(v => !toDeleteSet.Contains(v)).ToList();
            if (remaining.Count > 0)
            {
                filtered.Add(new KeyValuePair<string, List<string>>(entry.Key, remaining));
            }
        }

        // Rebuild
        Root = new BTreeNode<string, string>(MinDegree, isLeaf: true);
        foreach (var entry in filtered)
        {
            foreach (string val in entry.Value)
            {
                Insert(entry.Key, val);
            }
        }
    }

    /// <summary>
    /// Serialize the index to a JSON file on disk.
    /// </summary>
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
    /// Deserialize an index from a JSON file on disk.
    /// </summary>
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
