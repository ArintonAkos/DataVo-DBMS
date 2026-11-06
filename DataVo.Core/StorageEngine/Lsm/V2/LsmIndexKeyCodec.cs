using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Canonical byte encoding of index entries: <c>u16 length(K) | K | row key</c>, where K is the index key
/// in a type-tagged form. Integer and GUID keys are encoded natively, so the typed write paths (long, Guid)
/// and the string paths ("[42]", "3f2504e0-...") produce identical entries without formatting strings.
/// The length prefix keeps keys prefix-free: all entries of one key are contiguous and sorted by row key.
/// The row key is stored order-preserving in 9 or 17 bytes: <c>0x00 | low half</c> for an integer key
/// (high half zero) and <c>0x01 | all 16 bytes</c> otherwise, big-endian, so entries of one key sort in
/// <see cref="RowKey"/> order and integer keys cost one byte more than the 8-byte row ids of old index
/// tables (which are rebuilt from the rows on open). Entries therefore vary in length; files store them
/// with <see cref="SsTableKeyedFormat.LengthPrefixedBloomKey"/>.
/// </summary>
internal static class LsmIndexKeyCodec
{
    public const byte IntegerTag = 0x01;
    public const byte GuidTag = 0x02;
    public const byte StringTag = 0x03;

    /// <summary>Bloom setting of index files: the Bloom key is the length-prefixed index key.</summary>
    public const int BloomSuffix = SsTableKeyedFormat.LengthPrefixedBloomKey;

    private const byte IntegerRowKey = 0x00;
    private const byte WideRowKey = 0x01;
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

    /// <summary>User key of the entry (K, row key).</summary>
    public static byte[] UserKey(ReadOnlySpan<byte> encodedKey, RowKey rowKey)
    {
        int suffix = rowKey.IsInt64 ? 1 + sizeof(ulong) : 1 + 16;
        var userKey = new byte[LengthPrefixSize + encodedKey.Length + suffix];
        BinaryPrimitives.WriteUInt16BigEndian(userKey, (ushort)encodedKey.Length);
        encodedKey.CopyTo(userKey.AsSpan(LengthPrefixSize));
        Span<byte> tail = userKey.AsSpan(LengthPrefixSize + encodedKey.Length);
        if (rowKey.IsInt64)
        {
            tail[0] = IntegerRowKey;
            BinaryPrimitives.WriteUInt64BigEndian(tail[1..], rowKey.Low);
        }
        else
        {
            tail[0] = WideRowKey;
            rowKey.WriteTo(tail[1..]);
        }

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

    public static RowKey RowKeyOf(ReadOnlySpan<byte> userKey)
    {
        ReadOnlySpan<byte> tail = userKey[PrefixLength(userKey)..];
        return tail.Length switch
        {
            1 + sizeof(ulong) when tail[0] == IntegerRowKey => new RowKey(0, BinaryPrimitives.ReadUInt64BigEndian(tail[1..])),
            1 + 16 when tail[0] == WideRowKey => RowKey.Read(tail[1..]),
            _ => throw new InvalidDataException("Index entry has a malformed row key."),
        };
    }

    /// <summary>The length-prefixed key of an entry (the user key without its row key).</summary>
    public static ReadOnlySpan<byte> PrefixOf(ReadOnlySpan<byte> userKey) => userKey[..PrefixLength(userKey)];

    private static int PrefixLength(ReadOnlySpan<byte> userKey) =>
        LengthPrefixSize + BinaryPrimitives.ReadUInt16BigEndian(userKey);

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
