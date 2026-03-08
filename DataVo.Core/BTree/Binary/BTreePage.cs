using System.Text;

namespace DataVo.Core.BTree.Binary;

/// <summary>
/// Represents a fixed-size on-disk page used by the binary B-Tree implementation.
/// </summary>
/// <remarks>
/// Each page stores header data, a fixed number of string keys, their row ID values,
/// and child page pointers. The page layout is designed to fit within 4 KB.
/// </remarks>
public class BTreePage
{
    /// <summary>
    /// The size, in bytes, of a serialized page.
    /// </summary>
    public const int PageSize = 4096;

    /// <summary>
    /// The maximum number of keys that can be stored in one page.
    /// </summary>
    public const int MaxKeys = 59; // Calculated based on 4KB page and 64-byte entries. T = 30.

    /// <summary>
    /// The minimum number of keys an internal non-root page may contain after a split.
    /// </summary>
    public const int MinKeys = 29; // T - 1

    /// <summary>
    /// The minimum degree of the binary B-Tree page layout.
    /// </summary>
    public const int T = 30;

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
    /// Gets or sets the fixed-size key array.
    /// Only the first <see cref="NumKeys"/> entries are considered populated.
    /// </summary>
    public string[] Keys { get; set; } = new string[MaxKeys];

    /// <summary>
    /// Gets or sets the row ID values aligned with <see cref="Keys"/>.
    /// </summary>
    public long[] Values { get; set; } = new long[MaxKeys];

    /// <summary>
    /// Gets or sets the child page pointers.
    /// </summary>
    public int[] Children { get; set; } = new int[MaxKeys + 1];

    /// <summary>
    /// Serializes the page into its fixed 4 KB binary representation.
    /// </summary>
    /// <returns>The serialized page buffer.</returns>
    public byte[] Serialize()
    {
        byte[] buffer = new byte[PageSize];
        using var ms = new MemoryStream(buffer);
        using var writer = new BinaryWriter(ms);

        writer.Write(PageId);
        writer.Write(IsLeaf);
        writer.Write(NumKeys);

        // Pad header to exactly 16 bytes. (4 + 1 + 4 = 9 bytes). We write 7 extra zeros.
        for (int i = 0; i < 7; i++) writer.Write((byte)0);

        for (int i = 0; i < MaxKeys; i++)
        {
            writer.Write(GetFixedStringBytes(Keys[i], 32));
            writer.Write(Values[i]);
        }

        for (int i = 0; i < MaxKeys + 1; i++)
        {
            writer.Write(Children[i]);
        }

        return buffer;
    }

    /// <summary>
    /// Deserializes a page from its fixed-size binary representation.
    /// </summary>
    /// <param name="data">The serialized page bytes.</param>
    /// <returns>The deserialized <see cref="BTreePage"/> instance.</returns>
    public static BTreePage Deserialize(byte[] data)
    {
        var page = new BTreePage();
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms);

        page.PageId = reader.ReadInt32();
        page.IsLeaf = reader.ReadBoolean();
        page.NumKeys = reader.ReadInt32();

        reader.ReadBytes(7); // Skip header padding

        for (int i = 0; i < MaxKeys; i++)
        {
            page.Keys[i] = GetStringFromFixedBytes(reader.ReadBytes(32));
            page.Values[i] = reader.ReadInt64();
        }

        for (int i = 0; i < MaxKeys + 1; i++)
        {
            page.Children[i] = reader.ReadInt32();
        }

        return page;
    }

    /// <summary>
    /// Inserts a key/value pair into a page that is known not to be full.
    /// </summary>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The row ID associated with the key.</param>
    /// <param name="pager">The pager used to read and write child pages.</param>
    public void InsertNonFull(string key, long value, DiskPager pager)
    {
        int i = NumKeys - 1;

        if (IsLeaf)
        {
            // Find location and shift right
            while (i >= 0 && string.Compare(key, Keys[i], StringComparison.Ordinal) < 0)
            {
                Keys[i + 1] = Keys[i];
                Values[i + 1] = Values[i];
                i--;
            }

            Keys[i + 1] = key;
            Values[i + 1] = value;
            NumKeys++;
            pager.WritePage(this);
        }
        else
        {
            // Find child
            while (i >= 0 && string.Compare(key, Keys[i], StringComparison.Ordinal) < 0)
            {
                i--;
            }
            i++;

            BTreePage child = pager.ReadPage(Children[i]);

            if (child.NumKeys == MaxKeys)
            {
                SplitChild(i, child, pager);
                if (string.Compare(key, Keys[i], StringComparison.Ordinal) > 0)
                {
                    i++;
                }
            }

            // Re-read child if it was split and we need the new correct target
            child = pager.ReadPage(Children[i]);
            child.InsertNonFull(key, value, pager);
        }
    }

    /// <summary>
    /// Splits a full child page and promotes its median key into the current page.
    /// </summary>
    /// <param name="i">The child slot to split.</param>
    /// <param name="child">The full child page.</param>
    /// <param name="pager">The pager used for page allocation and persistence.</param>
    public void SplitChild(int i, BTreePage child, DiskPager pager)
    {
        BTreePage z = pager.AllocatePage();
        z.IsLeaf = child.IsLeaf;
        z.NumKeys = MinKeys;

        // Copy upper half of keys and values from child (y) to z
        for (int j = 0; j < MinKeys; j++)
        {
            z.Keys[j] = child.Keys[j + T];
            z.Values[j] = child.Values[j + T];
        }

        // Copy upper half of children from y to z if not leaf
        if (!child.IsLeaf)
        {
            for (int j = 0; j < T; j++)
            {
                z.Children[j] = child.Children[j + T];
            }
        }

        child.NumKeys = MinKeys; // Now it only has lower half

        // Shift children of current node to make room for new child z
        for (int j = NumKeys; j >= i + 1; j--)
        {
            Children[j + 1] = Children[j];
        }
        Children[i + 1] = z.PageId;

        // Shift keys of current node to make room for median key from child
        for (int j = NumKeys - 1; j >= i; j--)
        {
            Keys[j + 1] = Keys[j];
            Values[j + 1] = Values[j];
        }

        Keys[i] = child.Keys[T - 1];
        Values[i] = child.Values[T - 1];
        NumKeys++;

        pager.WritePage(child);
        pager.WritePage(z);
        pager.WritePage(this);
    }

    /// <summary>
    /// Encodes a string into a fixed-length UTF-8 byte array.
    /// </summary>
    /// <param name="str">The source string.</param>
    /// <param name="length">The fixed byte length to produce.</param>
    /// <returns>A byte array of exactly <paramref name="length"/> bytes.</returns>
    private static byte[] GetFixedStringBytes(string? str, int length)
    {
        byte[] result = new byte[length];
        if (!string.IsNullOrEmpty(str))
        {
            byte[] strBytes = Encoding.UTF8.GetBytes(str);
            Array.Copy(strBytes, 0, result, 0, Math.Min(strBytes.Length, length));
        }
        return result;
    }

    /// <summary>
    /// Decodes a fixed-length UTF-8 byte array back into a managed string.
    /// </summary>
    /// <param name="bytes">The fixed-length string bytes.</param>
    /// <returns>The decoded string with trailing null padding removed.</returns>
    private static string GetStringFromFixedBytes(byte[] bytes)
    {
        string str = Encoding.UTF8.GetString(bytes);
        int nullIdx = str.IndexOf('\0');
        if (nullIdx >= 0) return str.Substring(0, nullIdx);
        return str.TrimEnd('\0'); // Fallback if no explicit null terminator but padded with zeros
    }
}
