namespace DataVo.Core.BTree.BPlus;

/// <summary>
/// B+Tree Page. Represents a 4KB Disk Block.
///
/// Page layout (4096 bytes):
///   Header:   16 bytes (PageId, IsLeaf, NumKeys, NextPageId, padding)
///   Keys:     96 × 32 bytes = 3072 bytes (fixed-size byte[32] per slot)
///   Leaf:     96 × 8 bytes = 768 bytes (long row IDs) → total = 3856 bytes
///   Internal: 97 × 4 bytes = 388 bytes (int child page IDs) → total = 3476 bytes
///
/// Key encoding is handled by <see cref="IndexKeyEncoder"/>.
/// </summary>
public class BPlusTreePage
{
    public const int PageSize = 4096;
    public const int MaxKeys = 96;
    public const int MinKeys = 47; // T-1
    public const int T = 48;       // MaxKeys / 2

    public int PageId { get; set; }
    public bool IsLeaf { get; set; }
    public int NumKeys { get; set; }
    public int NextPageId { get; set; } = -1; // Linked list for leaf sequential scans

    // Fixed-size byte[32] key slots — all types, correct ordering via IndexKeyEncoder
    public byte[][] Keys { get; set; }

    // Leaf: row IDs that each key points to. Value 0 = empty/tombstone sentinel.
    public long[] Values { get; set; }

    // Internal: child page IDs. Children[i] points to subtree with keys < Keys[i].
    public int[] Children { get; set; }

    public BPlusTreePage()
    {
        Keys = new byte[MaxKeys][];
        for (int i = 0; i < MaxKeys; i++)
            Keys[i] = new byte[IndexKeyEncoder.KeySize];

        Values = new long[MaxKeys];
        Children = new int[MaxKeys + 1];
    }

    public long GetValue(int index) => Values[index];

    public void SetValue(int index, long value) => Values[index] = value;

    /// <summary>
    /// Finds the first index where Keys[i] >= targetKey using byte comparison.
    /// </summary>
    public int FindIndex(byte[] targetKey)
    {
        int i = 0;
        while (i < NumKeys && IndexKeyEncoder.CompareKeys(targetKey, Keys[i]) > 0)
        {
            i++;
        }
        return i;
    }

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
