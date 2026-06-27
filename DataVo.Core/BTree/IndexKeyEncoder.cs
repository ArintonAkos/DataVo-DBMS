using System.Text;
using System.Globalization;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Utils;

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
/// Byte comparison: 0x7F... &lt; 0x80... → -5 &lt; 1 as desired.
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
    public static string BuildKeyString(Dictionary<string, object?> row, IEnumerable<string> attributes)
    {
        return string.Join(CompositeKeySeparator, attributes.Select(attr => NormalizeValue(row[attr])));
    }

    /// <summary>
    /// Builds a logical key string from typed cells and a sequence of indexed attributes, producing a key
    /// identical to the dictionary <see cref="BuildKeyString(Dictionary{string, object?}, IEnumerable{string})"/>
    /// overload. This is the typed-storage path (no dictionary materialization).
    /// </summary>
    /// <param name="schema">The column layout describing <paramref name="cells"/>.</param>
    /// <param name="cells">The row's cells, in schema order.</param>
    /// <param name="attributes">The indexed attributes, in key order.</param>
    /// <returns>A single-column key or a composite key joined with <see cref="CompositeKeySeparator"/>.</returns>
    internal static string BuildKeyString(
        ReactiveRowSchema schema,
        ReadOnlySpan<CellValue> cells,
        IReadOnlyList<string> attributes)
    {
        if (attributes.Count == 1)
        {
            return NormalizeCell(GetCell(schema, cells, attributes[0]));
        }

        var builder = new StringBuilder();
        for (int i = 0; i < attributes.Count; i++)
        {
            if (i > 0)
            {
                builder.Append(CompositeKeySeparator);
            }

            builder.Append(NormalizeCell(GetCell(schema, cells, attributes[i])));
        }

        return builder.ToString();
    }

    private static CellValue GetCell(ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells, string columnName)
    {
        if (!schema.TryGetOrdinal(columnName, out int ordinal))
        {
            throw new BindingException($"Column {columnName} doesn't exist in typed row schema!");
        }

        return cells[ordinal];
    }

    // Preserve dictionary-path key compatibility without boxing typed scalar cells or allocating transient
    // vector arrays for numeric values. The legacy dictionary path treats parseable numeric strings as a
    // single-element vector, so typed numeric keys keep the same "[n]" logical key shape.
    private static string NormalizeCell(CellValue cell) => cell.Type switch
    {
        CellType.Null => string.Empty,
        CellType.Int32 => FormatSingleElementVector(cell.AsInt32()),
        CellType.Int64 => FormatSingleElementVector(cell.AsInt64()),
        CellType.Double => FormatSingleElementVector(cell.AsDouble()),
        CellType.Decimal => FormatSingleElementVector(cell.AsDecimal()),
        CellType.Boolean => cell.AsBoolean().ToString(),
        CellType.String => NormalizeStringCell(cell.AsString()),
        CellType.Date => cell.AsDate().ToString(),
        CellType.Vector => SerializeVector(cell.AsVectorReadOnlySpan()),
        _ => string.Empty
    };

    private static string NormalizeStringCell(string? value)
    {
        if (value == null)
        {
            return string.Empty;
        }

        return VectorParser.TryParseVector(value, out float[] vector)
            ? VectorParser.SerializeVector(vector)
            : value;
    }

    private static string FormatSingleElementVector(int value) =>
        string.Create(CultureInfo.InvariantCulture, $"[{value}]");

    private static string FormatSingleElementVector(long value) =>
        string.Create(CultureInfo.InvariantCulture, $"[{value}]");

    private static string FormatSingleElementVector(double value) =>
        string.Create(CultureInfo.CurrentCulture, $"[{value}]");

    private static string FormatSingleElementVector(decimal value) =>
        string.Create(CultureInfo.CurrentCulture, $"[{value}]");

    private static string SerializeVector(ReadOnlySpan<float> vector)
    {
        if (vector.Length == 0)
        {
            return "[]";
        }

        var builder = new StringBuilder();
        builder.Append('[');
        for (int i = 0; i < vector.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(',');
            }

            builder.Append(vector[i].ToString(CultureInfo.InvariantCulture));
        }

        builder.Append(']');
        return builder.ToString();
    }

    private static string NormalizeValue(object? value)
    {
        if (value == null)
        {
            return string.Empty;
        }

        if (VectorParser.TryCoerceToVector(value, out float[] vector))
        {
            return VectorParser.SerializeVector(vector);
        }

        return value.ToString() ?? string.Empty;
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
    /// Numeric values are encoded as signed 64-bit values; all other values are encoded as UTF-8.
    /// </summary>
    /// <param name="value">The logical value to encode.</param>
    /// <param name="dest">The destination buffer.</param>
    /// <param name="offset">The starting offset into <paramref name="dest"/>.</param>
    /// <returns>The number of bytes written to the destination buffer.</returns>
    private static int EncodeSingleValue(string value, byte[] dest, int offset)
    {
        if (offset >= KeySize) return 0;

        // Encode all parseable integer-like values as Int64 so mixed int/long ranges
        // preserve one consistent byte-wise ordering.
        if (long.TryParse(value, out long longVal))
        {
            if (offset + 8 > KeySize) return 0;
            EncodeLong(longVal, dest, offset);
            return 8;
        }

        // Fall back to UTF-8 string encoding (preserves lexicographic order)
        var bytes = Encoding.UTF8.GetBytes(value);
        int len = Math.Min(bytes.Length, KeySize - offset);
        Buffer.BlockCopy(bytes, 0, dest, offset, len);
        return len;
    }

    /// <summary>
    /// Encodes a signed 64-bit integer into an order-preserving big-endian byte sequence.
    /// </summary>
    /// <param name="value">The numeric value to encode.</param>
    /// <param name="dest">The destination buffer.</param>
    /// <param name="offset">The starting offset into <paramref name="dest"/>.</param>
    private static void EncodeLong(long value, byte[] dest, int offset)
    {
        ulong flipped = unchecked((ulong)(value ^ long.MinValue));

        dest[offset] = (byte)(flipped >> 56);
        dest[offset + 1] = (byte)(flipped >> 48);
        dest[offset + 2] = (byte)(flipped >> 40);
        dest[offset + 3] = (byte)(flipped >> 32);
        dest[offset + 4] = (byte)(flipped >> 24);
        dest[offset + 5] = (byte)(flipped >> 16);
        dest[offset + 6] = (byte)(flipped >> 8);
        dest[offset + 7] = (byte)flipped;
    }
}
