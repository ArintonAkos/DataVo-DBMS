using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Canonical byte encoding of index entries: <c>u16 length(K) | K | row id</c>, where K is the index key
/// in a type-tagged form. Integer and GUID keys are encoded natively, so the typed write paths (long, Guid)
/// and the string paths ("[42]", "3f2504e0-...") produce identical entries without formatting strings.
/// The length prefix keeps keys prefix-free: all entries of one key are contiguous and sorted by row id.
/// </summary>
internal static class LsmIndexKeyCodec
{
    public const byte IntegerTag = 0x01;
    public const byte GuidTag = 0x02;
    public const byte StringTag = 0x03;

    public const int RowIdSize = sizeof(long);
    public const int LengthPrefixSize = sizeof(ushort);

    /// <summary>Largest encoded key (K) an index entry can hold.</summary>
    public const int MaxKeyLength = ushort.MaxValue;

    /// <summary>K for an integer key.</summary>
    public static byte[] EncodeInteger(long key)
    {
        var encoded = new byte[1 + sizeof(long)];
        encoded[0] = IntegerTag;
        BinaryPrimitives.WriteUInt64BigEndian(encoded.AsSpan(1), unchecked((ulong)(key ^ long.MinValue)));
        return encoded;
    }

    /// <summary>K for a GUID key.</summary>
    public static byte[] EncodeGuid(Guid key)
    {
        var encoded = new byte[1 + 16];
        encoded[0] = GuidTag;
        key.TryWriteBytes(encoded.AsSpan(1));
        return encoded;
    }

    /// <summary>
    /// K for a key in the string form used by index writers (<see cref="BTree.IndexKeyEncoder"/>):
    /// integers ("42" or "[42]") and GUIDs map to their native encodings, everything else is UTF-8.
    /// </summary>
    public static byte[] EncodeString(string key)
    {
        if (TryParseInteger(key, out long integer))
        {
            return EncodeInteger(integer);
        }

        if (key.Length == 36 && Guid.TryParseExact(key, "D", out Guid guid))
        {
            return EncodeGuid(guid);
        }

        int length = Encoding.UTF8.GetByteCount(key);
        if (length + 1 > MaxKeyLength)
        {
            throw new ArgumentException($"Index key is too long ({length} bytes; the limit is {MaxKeyLength - 1}).", nameof(key));
        }

        var encoded = new byte[1 + length];
        encoded[0] = StringTag;
        Encoding.UTF8.GetBytes(key, 0, key.Length, encoded, 1);
        return encoded;
    }

    /// <summary>User key of the entry (K, rowId).</summary>
    public static byte[] UserKey(ReadOnlySpan<byte> encodedKey, long rowId)
    {
        var userKey = new byte[LengthPrefixSize + encodedKey.Length + RowIdSize];
        BinaryPrimitives.WriteUInt16BigEndian(userKey, (ushort)encodedKey.Length);
        encodedKey.CopyTo(userKey.AsSpan(LengthPrefixSize));
        BinaryPrimitives.WriteUInt64BigEndian(userKey.AsSpan(LengthPrefixSize + encodedKey.Length), unchecked((ulong)(rowId ^ long.MinValue)));
        return userKey;
    }

    /// <summary>The prefix shared by every entry of K (length prefix and K): the Bloom-filter key.</summary>
    public static byte[] KeyPrefix(ReadOnlySpan<byte> encodedKey)
    {
        var prefix = new byte[LengthPrefixSize + encodedKey.Length];
        BinaryPrimitives.WriteUInt16BigEndian(prefix, (ushort)encodedKey.Length);
        encodedKey.CopyTo(prefix.AsSpan(LengthPrefixSize));
        return prefix;
    }

    public static long RowIdOf(ReadOnlySpan<byte> userKey) =>
        unchecked((long)(BinaryPrimitives.ReadUInt64BigEndian(userKey[^RowIdSize..]) ^ (ulong)long.MinValue));

    /// <summary>The length-prefixed key of an entry (the user key without its row id).</summary>
    public static ReadOnlySpan<byte> PrefixOf(ReadOnlySpan<byte> userKey) => userKey[..^RowIdSize];

    private static bool TryParseInteger(string key, out long value)
    {
        if (long.TryParse(key, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value))
        {
            // Only the canonical form: "007" or "+7" are strings in a VARCHAR column, not the integer 7.
            return string.Equals(value.ToString(CultureInfo.InvariantCulture), key, StringComparison.Ordinal);
        }

        if (key.Length >= 3 && key[0] == '[' && key[^1] == ']'
            && long.TryParse(key.AsSpan(1, key.Length - 2), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value))
        {
            return key.Length - 2 == value.ToString(CultureInfo.InvariantCulture).Length;
        }

        value = 0;
        return false;
    }
}
