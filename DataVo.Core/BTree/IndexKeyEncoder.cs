using System.Text;

namespace DataVo.Core.BTree;

/// <summary>
/// Encodes index keys from their string representation into fixed-size byte[32] arrays
/// suitable for the B+Tree. The encoding preserves sort order for all supported types:
///
/// <list type="bullet">
///   <item>INT: sign-flip + big-endian 4 bytes → correct numeric ordering</item>
///   <item>VARCHAR: raw UTF-8 bytes → correct lexicographic ordering</item>
///   <item>Composite (e.g. "1##101"): each part encoded sequentially into the byte array</item>
/// </list>
///
/// The sign-flip trick: XOR the sign bit so that negative ints sort before positive.
/// Example: -5 → 0x7FFFFFFB, 0 → 0x80000000, 1 → 0x80000001
/// Byte comparison: 0x7F... &lt; 0x80... → -5 &lt; 1 ✅
/// </summary>
public static class IndexKeyEncoder
{
    public const int KeySize = 32;
    public const string CompositeKeySeparator = "##";

    /// <summary>
    /// Empty key (all zeros) — used as the sentinel/default in unoccupied page slots.
    /// </summary>
    public static byte[] EmptyKey => new byte[KeySize];

    /// <summary>
    /// Encodes a string key (as passed through the IIndex interface) into a fixed-size byte[32].
    /// Handles single values and composite keys separated by "##".
    /// </summary>
    public static byte[] Encode(string key)
    {
        var result = new byte[KeySize];

        if (string.IsNullOrEmpty(key))
            return result;

        // Check for composite key
        if (key.Contains(CompositeKeySeparator))
        {
            EncodeComposite(key, result);
        }
        else
        {
            EncodeSingleValue(key, result, 0);
        }

        return result;
    }

    /// <summary>
    /// Builds a composite key string from a row's column values.
    /// Replaces the hardcoded "##" concatenation scattered across InsertInto.cs.
    /// </summary>
    public static string BuildKeyString(Dictionary<string, dynamic> row, IEnumerable<string> attributes)
    {
        return string.Join(CompositeKeySeparator, attributes.Select(attr => row[attr]?.ToString() ?? ""));
    }

    /// <summary>
    /// Compares two byte[32] keys. Returns negative if a &lt; b, 0 if equal, positive if a &gt; b.
    /// </summary>
    public static int CompareKeys(byte[] a, byte[] b)
    {
        return new ReadOnlySpan<byte>(a).SequenceCompareTo(b);
    }

    /// <summary>
    /// Checks if a key is the empty (all-zero) sentinel.
    /// </summary>
    public static bool IsEmptyKey(byte[] key)
    {
        for (int i = 0; i < key.Length; i++)
        {
            if (key[i] != 0) return false;
        }
        return true;
    }

    // --- Internal encoding methods ---

    private static void EncodeComposite(string compositeKey, byte[] result)
    {
        var parts = compositeKey.Split(CompositeKeySeparator);
        int offset = 0;

        foreach (var part in parts)
        {
            if (offset >= KeySize) break;
            int bytesWritten = EncodeSingleValue(part, result, offset);
            offset += bytesWritten;
        }
    }

    /// <summary>
    /// Encodes a single value (string or int) into the result buffer at the given offset.
    /// Returns the number of bytes written.
    /// </summary>
    private static int EncodeSingleValue(string value, byte[] dest, int offset)
    {
        if (offset >= KeySize) return 0;

        // Try integer encoding first (preserves numeric sort order)
        if (int.TryParse(value, out int intVal))
        {
            if (offset + 4 > KeySize) return 0;
            EncodeInt(intVal, dest, offset);
            return 4;
        }

        // Fall back to UTF-8 string encoding (preserves lexicographic order)
        var bytes = Encoding.UTF8.GetBytes(value);
        int len = Math.Min(bytes.Length, KeySize - offset);
        Buffer.BlockCopy(bytes, 0, dest, offset, len);
        return len;
    }

    /// <summary>
    /// Encodes an integer using sign-flip + big-endian for correct byte-level sort order.
    /// XORs with int.MinValue to flip the sign bit: negatives sort before positives.
    /// </summary>
    private static void EncodeInt(int value, byte[] dest, int offset)
    {
        // XOR with MinValue flips the sign bit:
        // int.MinValue (-2B) → 0x00000000 (sorts first)
        // -1              → 0x7FFFFFFF
        // 0               → 0x80000000
        // int.MaxValue (2B) → 0xFFFFFFFF (sorts last)
        uint flipped = unchecked((uint)(value ^ int.MinValue));

        dest[offset]     = (byte)(flipped >> 24);
        dest[offset + 1] = (byte)(flipped >> 16);
        dest[offset + 2] = (byte)(flipped >> 8);
        dest[offset + 3] = (byte)flipped;
    }
}
