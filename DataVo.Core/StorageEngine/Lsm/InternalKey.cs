using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>The kind of a versioned LSM entry. Packed into the low byte of the InternalKey tag.</summary>
public enum LsmValueType : byte
{
    /// <summary>A deletion marker (tombstone) — an entry that carries no value.</summary>
    Deletion = 0,

    /// <summary>A live value: a full row image from an insert or update.</summary>
    Put = 1,
}

/// <summary>
/// Order-preserving encoding and comparison for LSM internal keys. An internal key is
/// <c>userKey ‖ tag</c>, where <c>tag</c> is the fixed 8-byte big-endian trailer
/// <c>(seqno &lt;&lt; 8) | (byte)valueType</c>. Internal keys sort by user key ascending, then by tag
/// descending, so the newest version (highest seqno) of a user key sorts first.
/// </summary>
public static class InternalKey
{
    /// <summary>Size, in bytes, of the packed tag trailer appended to every user key.</summary>
    public const int TagSize = 8;

    /// <summary>Maximum sequence number representable in the 56-bit seqno field of the tag.</summary>
    public const ulong MaxSequenceNumber = (1UL << 56) - 1;

    /// <summary>Bytes an internal key occupies for a user key of the given length.</summary>
    public static int MeasureSize(int userKeyLength) => userKeyLength + TagSize;

    /// <summary>
    /// Writes <paramref name="userKey"/> followed by the packed tag into <paramref name="dest"/> and
    /// returns the number of bytes written (<c>userKey.Length + <see cref="TagSize"/></c>).
    /// </summary>
    public static int Write(Span<byte> dest, ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType)
    {
        if (seqno > MaxSequenceNumber)
        {
            throw new ArgumentOutOfRangeException(nameof(seqno));
        }

        userKey.CopyTo(dest);
        ulong tag = (seqno << 8) | (byte)valueType;
        BinaryPrimitives.WriteUInt64BigEndian(dest.Slice(userKey.Length, TagSize), tag);
        return userKey.Length + TagSize;
    }

    /// <summary>Returns the user-key portion (everything except the trailing tag).</summary>
    public static ReadOnlySpan<byte> UserKey(ReadOnlySpan<byte> internalKey) => internalKey[..^TagSize];

    /// <summary>Returns the sequence number stored in the trailing tag.</summary>
    public static ulong Sequence(ReadOnlySpan<byte> internalKey) =>
        BinaryPrimitives.ReadUInt64BigEndian(internalKey[^TagSize..]) >> 8;

    /// <summary>Returns the value type stored in the trailing tag.</summary>
    public static LsmValueType ValueType(ReadOnlySpan<byte> internalKey) => (LsmValueType)internalKey[^1];

    /// <summary>Compares two internal keys: user key ascending, then tag descending (newest-first).</summary>
    public static int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int byUserKey = a[..^TagSize].SequenceCompareTo(b[..^TagSize]);
        if (byUserKey != 0)
        {
            return byUserKey;
        }

        // Tag descending: comparing b's tag to a's tag inverts the order. Big-endian byte order
        // equals numeric order, so a larger tag (newer) sorts first.
        return b[^TagSize..].SequenceCompareTo(a[^TagSize..]);
    }

    /// <summary>
    /// Encodes a signed 64-bit primary key into an order-preserving 8-byte big-endian user key
    /// (sign-flipped so negatives sort before positives), mirroring
    /// <see cref="DataVo.Core.BTree.IndexKeyEncoder"/>. Returns the number of bytes written (8).
    /// </summary>
    public static int EncodeInt64UserKey(Span<byte> dest, long primaryKey)
    {
        ulong flipped = unchecked((ulong)(primaryKey ^ long.MinValue));
        BinaryPrimitives.WriteUInt64BigEndian(dest[..8], flipped);
        return 8;
    }
}
