using System;
using System.IO;
using System.Text;

namespace DataVo.Core.BTree.Binary;

public class BTreePage
{
    public const int PageSize = 4096;
    public const int MaxKeys = 59; // Calculated based on 4KB page and 64-byte entries. T = 30.
    public const int MinKeys = 29; // T - 1
    public const int T = 30;

    public int PageId { get; set; }
    public bool IsLeaf { get; set; }
    public int NumKeys { get; set; }
    
    public string[] Keys { get; set; } = new string[MaxKeys];
    public string[] Values { get; set; } = new string[MaxKeys];
    public int[] Children { get; set; } = new int[MaxKeys + 1];

    public byte[] Serialize()
    {
        byte[] buffer = new byte[PageSize];
        using var ms = new MemoryStream(buffer);
        using var writer = new BinaryWriter(ms);

        writer.Write(PageId);
        writer.Write(IsLeaf);
        writer.Write(NumKeys);
        
        // Pad header to exactly 16 bytes. (4 + 1 + 4 = 9 bytes). We write 7 extra zeros.
        for(int i=0; i<7; i++) writer.Write((byte)0);

        for (int i = 0; i < MaxKeys; i++)
        {
            writer.Write(GetFixedStringBytes(Keys[i], 32));
            writer.Write(GetFixedStringBytes(Values[i], 32));
        }

        for (int i = 0; i < MaxKeys + 1; i++)
        {
            writer.Write(Children[i]);
        }

        return buffer;
    }

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
            page.Values[i] = GetStringFromFixedBytes(reader.ReadBytes(32));
        }

        for (int i = 0; i < MaxKeys + 1; i++)
        {
            page.Children[i] = reader.ReadInt32();
        }

        return page;
    }

    public void InsertNonFull(string key, string value, DiskPager pager)
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

    private static string GetStringFromFixedBytes(byte[] bytes)
    {
        string str = Encoding.UTF8.GetString(bytes);
        int nullIdx = str.IndexOf('\0');
        if (nullIdx >= 0) return str.Substring(0, nullIdx);
        return str.TrimEnd('\0'); // Fallback if no explicit null terminator but padded with zeros
    }
}
