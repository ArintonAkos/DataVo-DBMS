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
    /// <summary>
    /// The fixed size, in bytes, of every encoded key stored in the binary B+Tree format.
    /// </summary>
    public const int KeySize = 32;

    /// <summary>
    /// The delimiter used when building composite key strings from multiple attribute values.
    /// </summary>
    public const string CompositeKeySeparator = "##";

    /// <summary>
    /// Empty key (all zeros) — used as the sentinel/default in unoccupied page slots.
    /// </summary>
    public static byte[] EmptyKey => new byte[KeySize];

    /// <summary>
    /// Encodes an index key string into a fixed-size byte buffer suitable for storage in a binary B+Tree.
    /// </summary>
    /// <param name="key">The logical key string to encode. May represent a single value or a composite key.</param>
    /// <returns>A <see cref="byte"/> array of length <see cref="KeySize"/>.</returns>
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
    /// Builds a logical key string from a row and a sequence of indexed attributes.
    /// </summary>
    /// <param name="row">The row values keyed by column name.</param>
    /// <param name="attributes">The indexed attributes, in key order.</param>
    /// <returns>A single-column key or a composite key joined with <see cref="CompositeKeySeparator"/>.</returns>
    public static string BuildKeyString(Dictionary<string, dynamic> row, IEnumerable<string> attributes)
    {
        return string.Join(CompositeKeySeparator, attributes.Select(attr => row[attr]?.ToString() ?? ""));
    }

    /// <summary>
    /// Compares two encoded keys using byte-wise ordering.
    /// </summary>
    /// <param name="a">The first encoded key.</param>
    /// <param name="b">The second encoded key.</param>
    /// <returns>A negative value if <paramref name="a"/> is less than <paramref name="b"/>, zero if equal, or a positive value if greater.</returns>
    public static int CompareKeys(byte[] a, byte[] b)
    {
        return new ReadOnlySpan<byte>(a).SequenceCompareTo(b);
    }

    /// <summary>
    /// Determines whether an encoded key is the all-zero sentinel value.
    /// </summary>
    /// <param name="key">The encoded key to inspect.</param>
    /// <returns><see langword="true"/> if the key contains only zero bytes; otherwise, <see langword="false"/>.</returns>
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
    /// Encodes a single logical value into the destination buffer starting at the specified offset.
    /// Integer values are encoded numerically; all other values are encoded as UTF-8.
    /// </summary>
    /// <param name="value">The logical value to encode.</param>
    /// <param name="dest">The destination buffer.</param>
    /// <param name="offset">The starting offset into <paramref name="dest"/>.</param>
    /// <returns>The number of bytes written to the destination buffer.</returns>
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
    /// Encodes a 32-bit integer using sign-flip plus big-endian byte order so byte comparison preserves numeric ordering.
    /// </summary>
    /// <param name="value">The integer value to encode.</param>
    /// <param name="dest">The destination buffer.</param>
    /// <param name="offset">The offset at which to write the 4-byte encoded value.</param>
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
