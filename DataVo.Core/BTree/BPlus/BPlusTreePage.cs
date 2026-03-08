namespace DataVo.Core.BTree.BPlus;

/// <summary>
/// Represents a fixed-size 4 KB page used by the binary B+Tree implementation.
/// </summary>
/// <remarks>
/// <para>
/// Leaf pages store encoded keys plus row IDs and are linked together through <see cref="NextPageId"/>
/// to support efficient sequential scans.
/// </para>
/// <para>
/// Internal pages store encoded keys plus child pointers used for routing.
/// Key encoding is delegated to <see cref="IndexKeyEncoder"/>.
/// </para>
/// </remarks>
public class BPlusTreePage
{
    /// <summary>
    /// The size, in bytes, of a serialized page.
    /// </summary>
    public const int PageSize = 4096;

    /// <summary>
    /// The maximum number of keys that can be stored in a page.
    /// </summary>
    public const int MaxKeys = 96;

    /// <summary>
    /// The minimum number of keys a non-root page should contain after a split.
    /// </summary>
    public const int MinKeys = 47; // T-1

    /// <summary>
    /// The minimum degree implied by the page layout.
    /// </summary>
    public const int T = 48;       // MaxKeys / 2

    /// <summary>
    /// Gets or sets the page identifier.
    /// </summary>
    public int PageId { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this page is a leaf page.
    /// </summary>
    public bool IsLeaf { get; set; }

    /// <summary>
    /// Gets or sets the number of populated key slots.
    /// </summary>
    public int NumKeys { get; set; }

    /// <summary>
    /// Gets or sets the next leaf page ID, or <c>-1</c> when there is no next leaf.
    /// </summary>
    public int NextPageId { get; set; } = -1; // Linked list for leaf sequential scans

    /// <summary>
    /// Gets or sets the fixed-size encoded key slots.
    /// </summary>
    public byte[][] Keys { get; set; }

    /// <summary>
    /// Gets or sets the row ID values stored in leaf pages.
    /// A value of <c>0</c> represents an empty or tombstoned slot.
    /// </summary>
    public long[] Values { get; set; }

    /// <summary>
    /// Gets or sets the child page IDs stored in internal pages.
    /// </summary>
    public int[] Children { get; set; }

    /// <summary>
    /// Initializes a new empty page instance.
    /// </summary>
    public BPlusTreePage()
    {
        Keys = new byte[MaxKeys][];
        for (int i = 0; i < MaxKeys; i++)
            Keys[i] = new byte[IndexKeyEncoder.KeySize];

        Values = new long[MaxKeys];
        Children = new int[MaxKeys + 1];
    }

    /// <summary>
    /// Returns the row ID stored at the specified slot.
    /// </summary>
    /// <param name="index">The value slot index.</param>
    /// <returns>The stored row ID.</returns>
    public long GetValue(int index) => Values[index];

    /// <summary>
    /// Stores a row ID at the specified value slot.
    /// </summary>
    /// <param name="index">The value slot index.</param>
    /// <param name="value">The row ID to store.</param>
    public void SetValue(int index, long value) => Values[index] = value;

    /// <summary>
    /// Finds the first slot whose key is greater than or equal to the specified encoded target key.
    /// </summary>
    /// <param name="targetKey">The encoded key to locate.</param>
    /// <returns>The insertion or routing index for the target key.</returns>
    public int FindIndex(byte[] targetKey)
    {
        int i = 0;
        while (i < NumKeys && IndexKeyEncoder.CompareKeys(targetKey, Keys[i]) > 0)
        {
            i++;
        }
        return i;
    }

    /// <summary>
    /// Serializes the page to its fixed-size binary representation.
    /// </summary>
    /// <returns>The serialized page buffer.</returns>
    public byte[] Serialize()
    {
        byte[] buffer = new byte[PageSize];
        using var ms = new MemoryStream(buffer);
        using var writer = new BinaryWriter(ms);

        // Header (16 bytes)
        writer.Write(PageId);
        writer.Write(IsLeaf);
        writer.Write(NumKeys);
        writer.Write(NextPageId);
        writer.Write((byte)0); writer.Write((byte)0); writer.Write((byte)0); // padding

        // Keys: MaxKeys × KeySize bytes
        for (int i = 0; i < MaxKeys; i++)
        {
            writer.Write(Keys[i]);
        }

        if (IsLeaf)
        {
            // Values: MaxKeys × 8 bytes
            for (int i = 0; i < MaxKeys; i++)
            {
                writer.Write(Values[i]);
            }
        }
        else
        {
            // Children: (MaxKeys + 1) × 4 bytes
            for (int i = 0; i < MaxKeys + 1; i++)
            {
                writer.Write(Children[i]);
            }
        }

        return buffer;
    }

    /// <summary>
    /// Deserializes a page from its fixed-size binary representation.
    /// </summary>
    /// <param name="data">The serialized page bytes.</param>
    /// <returns>The deserialized <see cref="BPlusTreePage"/>.</returns>
    public static BPlusTreePage Deserialize(byte[] data)
    {
        var page = new BPlusTreePage();
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms);

        // Header
        page.PageId = reader.ReadInt32();
        page.IsLeaf = reader.ReadBoolean();
        page.NumKeys = reader.ReadInt32();
        page.NextPageId = reader.ReadInt32();
        reader.ReadBytes(3); // Skip padding

        // Keys
        for (int i = 0; i < MaxKeys; i++)
        {
            page.Keys[i] = reader.ReadBytes(IndexKeyEncoder.KeySize);
        }

        if (page.IsLeaf)
        {
            for (int i = 0; i < MaxKeys; i++)
            {
                page.Values[i] = reader.ReadInt64();
            }
        }
        else
        {
            for (int i = 0; i < MaxKeys + 1; i++)
            {
                page.Children[i] = reader.ReadInt32();
            }
        }

        return page;
    }
}
