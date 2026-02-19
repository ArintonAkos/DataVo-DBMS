using Newtonsoft.Json;

namespace Server.Server.BTree;

/// <summary>
/// Generic B-Tree node with configurable minimum degree (t).
/// Supports duplicate keys by storing multiple values per key.
/// </summary>
[JsonObject(MemberSerialization.OptIn)]
public class BTreeNode<TKey, TValue> where TKey : IComparable<TKey>
{
    /// <summary>
    /// Minimum degree of the B-Tree. A node can hold at most 2t-1 keys.
    /// </summary>
    [JsonProperty("t")]
    public int MinDegree { get; set; }

    [JsonProperty("keys")]
    public List<TKey> Keys { get; set; }

    /// <summary>
    /// Each entry in Values corresponds to a key at the same index.
    /// A single key can map to multiple values (duplicate key support).
    /// </summary>
    [JsonProperty("values")]
    public List<List<TValue>> Values { get; set; }

    [JsonProperty("children")]
    public List<BTreeNode<TKey, TValue>>? Children { get; set; }

    [JsonProperty("isLeaf")]
    public bool IsLeaf { get; set; }

    /// <summary>
    /// Parameterless constructor for JSON deserialization.
    /// </summary>
    public BTreeNode()
    {
        MinDegree = 50;
        Keys = new List<TKey>();
        Values = new List<List<TValue>>();
        Children = null;
        IsLeaf = true;
    }

    public BTreeNode(int minDegree, bool isLeaf)
    {
        MinDegree = minDegree;
        IsLeaf = isLeaf;
        Keys = new List<TKey>();
        Values = new List<List<TValue>>();
        Children = isLeaf ? null : new List<BTreeNode<TKey, TValue>>();
    }

    /// <summary>
    /// Whether this node is full (has 2t-1 keys).
    /// </summary>
    public bool IsFull => Keys.Count == 2 * MinDegree - 1;

    /// <summary>
    /// Search for all values associated with the given key.
    /// </summary>
    public List<TValue> Search(TKey key)
    {
        int i = 0;
        while (i < Keys.Count && key.CompareTo(Keys[i]) > 0)
        {
            i++;
        }

        if (i < Keys.Count && key.CompareTo(Keys[i]) == 0)
        {
            return new List<TValue>(Values[i]);
        }

        if (IsLeaf)
        {
            return new List<TValue>();
        }

        return Children![i].Search(key);
    }

    /// <summary>
    /// Check if any value exists for the given key.
    /// </summary>
    public bool ContainsKey(TKey key)
    {
        int i = 0;
        while (i < Keys.Count && key.CompareTo(Keys[i]) > 0)
        {
            i++;
        }

        if (i < Keys.Count && key.CompareTo(Keys[i]) == 0)
        {
            return true;
        }

        if (IsLeaf)
        {
            return false;
        }

        return Children![i].ContainsKey(key);
    }

    /// <summary>
    /// Insert a key-value pair into a non-full node.
    /// If a child is full, split it before descending.
    /// </summary>
    public void InsertNonFull(TKey key, TValue value)
    {
        int i = Keys.Count - 1;

        if (IsLeaf)
        {
            // Find position and check for duplicate key
            int pos = 0;
            while (pos < Keys.Count && key.CompareTo(Keys[pos]) > 0)
            {
                pos++;
            }

            if (pos < Keys.Count && key.CompareTo(Keys[pos]) == 0)
            {
                // Duplicate key — append value to existing list
                Values[pos].Add(value);
                return;
            }

            // Insert new key at correct position
            Keys.Insert(pos, key);
            Values.Insert(pos, new List<TValue> { value });
        }
        else
        {
            // Find the child to descend into
            while (i >= 0 && key.CompareTo(Keys[i]) < 0)
            {
                i--;
            }

            // Check for duplicate key at this internal node
            if (i >= 0 && key.CompareTo(Keys[i]) == 0)
            {
                Values[i].Add(value);
                return;
            }

            i++;

            if (Children![i].IsFull)
            {
                SplitChild(i, Children[i]);

                // After split, determine which of the two children gets the new key
                if (key.CompareTo(Keys[i]) == 0)
                {
                    Values[i].Add(value);
                    return;
                }

                if (key.CompareTo(Keys[i]) > 0)
                {
                    i++;
                }
            }

            Children[i].InsertNonFull(key, value);
        }
    }

    /// <summary>
    /// Split the full child at index i. Promotes the median key to this node.
    /// </summary>
    public void SplitChild(int i, BTreeNode<TKey, TValue> fullChild)
    {
        int t = fullChild.MinDegree;
        var newNode = new BTreeNode<TKey, TValue>(t, fullChild.IsLeaf);

        // Copy the last t-1 keys and values from fullChild to newNode
        for (int j = 0; j < t - 1; j++)
        {
            newNode.Keys.Add(fullChild.Keys[t + j]);
            newNode.Values.Add(fullChild.Values[t + j]);
        }

        // If not leaf, copy the last t children
        if (!fullChild.IsLeaf)
        {
            for (int j = 0; j < t; j++)
            {
                newNode.Children!.Add(fullChild.Children![t + j]);
            }
        }

        // Promote the median key
        TKey medianKey = fullChild.Keys[t - 1];
        List<TValue> medianValues = fullChild.Values[t - 1];

        // Trim the full child
        fullChild.Keys.RemoveRange(t - 1, t);
        fullChild.Values.RemoveRange(t - 1, t);

        if (!fullChild.IsLeaf)
        {
            fullChild.Children!.RemoveRange(t, t);
        }

        // Insert the new node as a child of this node
        Children!.Insert(i + 1, newNode);

        // Insert the median key into this node
        Keys.Insert(i, medianKey);
        Values.Insert(i, medianValues);
    }

    /// <summary>
    /// Collect all key-value pairs in the subtree rooted at this node.
    /// </summary>
    public void CollectAll(List<KeyValuePair<TKey, List<TValue>>> results)
    {
        for (int i = 0; i < Keys.Count; i++)
        {
            if (!IsLeaf)
            {
                Children![i].CollectAll(results);
            }

            results.Add(new KeyValuePair<TKey, List<TValue>>(Keys[i], Values[i]));
        }

        if (!IsLeaf)
        {
            Children![Keys.Count].CollectAll(results);
        }
    }
}
