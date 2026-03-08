using Newtonsoft.Json;

namespace DataVo.Core.BTree;

/// <summary>
/// Represents a generic in-memory B-Tree node with configurable minimum degree.
/// </summary>
/// <typeparam name="TKey">The key type stored in the node. Keys must be comparable.</typeparam>
/// <typeparam name="TValue">The value type associated with each key.</typeparam>
/// <remarks>
/// <para>
/// This implementation supports duplicate keys by storing a <see cref="List{T}"/> of values for each key slot.
/// The same structure is used by the JSON-backed index implementation.
/// </para>
/// <para>
/// For a minimum degree <c>t</c>, each node can contain at most <c>2t - 1</c> keys.
/// Non-leaf nodes can contain up to <c>2t</c> children.
/// </para>
/// </remarks>
[JsonObject(MemberSerialization.OptIn)]
public class BTreeNode<TKey, TValue> where TKey : IComparable<TKey>
{
    /// <summary>
    /// Gets or sets the minimum degree of the B-Tree.
    /// A node can store at most <c>2 * MinDegree - 1</c> keys.
    /// </summary>
    [JsonProperty("t")]
    public int MinDegree { get; set; }

    /// <summary>
    /// Gets or sets the ordered keys stored in this node.
    /// </summary>
    [JsonProperty("keys")]
    public List<TKey> Keys { get; set; }

    /// <summary>
    /// Gets or sets the values associated with the keys in <see cref="Keys"/>.
    /// Each position corresponds to the key at the same index, and each key can map to multiple values.
    /// </summary>
    [JsonProperty("values")]
    public List<List<TValue>> Values { get; set; }

    /// <summary>
    /// Gets or sets the child nodes.
    /// This value is <see langword="null"/> for leaf nodes.
    /// </summary>
    [JsonProperty("children")]
    public List<BTreeNode<TKey, TValue>>? Children { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this node is a leaf node.
    /// </summary>
    [JsonProperty("isLeaf")]
    public bool IsLeaf { get; set; }

    /// <summary>
    /// Initializes a new leaf node using the default minimum degree.
    /// This constructor exists primarily for JSON deserialization.
    /// </summary>
    public BTreeNode()
    {
        MinDegree = 50;
        Keys = new List<TKey>();
        Values = new List<List<TValue>>();
        Children = null;
        IsLeaf = true;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="BTreeNode{TKey, TValue}"/> class.
    /// </summary>
    /// <param name="minDegree">The minimum degree of the B-Tree that owns this node.</param>
    /// <param name="isLeaf"><see langword="true"/> to create a leaf node; otherwise, <see langword="false"/>.</param>
    public BTreeNode(int minDegree, bool isLeaf)
    {
        MinDegree = minDegree;
        IsLeaf = isLeaf;
        Keys = new List<TKey>();
        Values = new List<List<TValue>>();
        Children = isLeaf ? null : new List<BTreeNode<TKey, TValue>>();
    }

    /// <summary>
    /// Gets a value indicating whether this node has reached its maximum key capacity.
    /// </summary>
    public bool IsFull => Keys.Count == 2 * MinDegree - 1;

    /// <summary>
    /// Searches this node and its subtree for the specified key.
    /// </summary>
    /// <param name="key">The key to search for.</param>
    /// <returns>
    /// A copy of the values associated with <paramref name="key"/>, or an empty list if the key is not present.
    /// </returns>
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
    /// Determines whether the specified key exists in this node or any descendant node.
    /// </summary>
    /// <param name="key">The key to locate.</param>
    /// <returns><see langword="true"/> if the key exists; otherwise, <see langword="false"/>.</returns>
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
    /// Determines whether any key in this subtree maps to the specified value.
    /// </summary>
    /// <param name="value">The value to search for.</param>
    /// <returns><see langword="true"/> if the value appears anywhere in the subtree; otherwise, <see langword="false"/>.</returns>
    /// <remarks>
    /// This operation requires a full subtree traversal because values are not ordered independently of keys.
    /// </remarks>
    public bool ContainsValue(TValue value)
    {
        foreach (var valueList in Values)
        {
            if (valueList.Contains(value)) return true;
        }

        if (IsLeaf)
        {
            return false;
        }

        for (int i = 0; i <= Keys.Count; i++)
        {
            if (Children![i].ContainsValue(value)) return true;
        }

        return false;
    }

    /// <summary>
    /// Inserts a key-value pair into a node that is known not to be full.
    /// </summary>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The value to associate with the key.</param>
    /// <remarks>
    /// If the key already exists in the target node, the value is appended to the existing value list.
    /// If the insertion path encounters a full child, that child is split before descending.
    /// </remarks>
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
    /// Splits a full child node and promotes its median key into the current node.
    /// </summary>
    /// <param name="i">The child index in <see cref="Children"/> to split.</param>
    /// <param name="fullChild">The child node that is currently full.</param>
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
    /// Collects all key/value mappings in the subtree rooted at this node.
    /// </summary>
    /// <param name="results">The destination list that receives each key and its associated value list.</param>
    /// <remarks>
    /// Entries are collected in in-order traversal order.
    /// </remarks>
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
