using System.Buffers.Binary;
using System.Globalization;

namespace DataVo.Core;

/// <summary>
/// Identifies a row within its table: 16 bytes, ordered as unsigned big-endian bytes.
/// </summary>
/// <remarks>
/// <para>
/// A table clustered on an integer or GUID primary key uses the key itself as the row key; other tables
/// use an engine-assigned integer. Integers are stored order-preserving (sign bit flipped) in the low
/// half, so negative keys sort before positive ones. GUIDs are stored in RFC 4122 byte order, so
/// time-ordered (version 7) GUIDs sort by creation time.
/// </para>
/// <para>
/// A <see cref="long"/> converts implicitly (code that passes integer row ids keeps working); the reverse
/// is explicit and throws for a key that is not an integer.
/// </para>
/// </remarks>
public readonly struct RowKey : IEquatable<RowKey>, IComparable<RowKey>
{
    private const ulong SignBit = 0x8000_0000_0000_0000UL;

    /// <summary>The 16 bytes as two big-endian halves.</summary>
    public ulong High { get; }

    /// <summary>The low half; for an integer key, the integer with its sign bit flipped.</summary>
    public ulong Low { get; }

    /// <summary>Creates a key from its two halves (the inverse of <see cref="High"/> and <see cref="Low"/>).</summary>
    public RowKey(ulong high, ulong low)
    {
        High = high;
        Low = low;
    }

    /// <summary>The key of an integer (an engine-assigned row id or an integer primary key).</summary>
    public static RowKey FromInt64(long value) => new(0, unchecked((ulong)value) ^ SignBit);

    /// <summary>The key of a GUID primary key, in RFC 4122 byte order.</summary>
    public static RowKey FromGuid(Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        WriteGuidBigEndian(value, bytes);
        return new RowKey(BinaryPrimitives.ReadUInt64BigEndian(bytes), BinaryPrimitives.ReadUInt64BigEndian(bytes[8..]));
    }

    /// <summary>Reads a key written by <see cref="WriteTo"/>.</summary>
    public static RowKey Read(ReadOnlySpan<byte> source) =>
        new(BinaryPrimitives.ReadUInt64BigEndian(source), BinaryPrimitives.ReadUInt64BigEndian(source[8..]));

    /// <summary>Whether this key holds an integer (high half zero).</summary>
    public bool IsInt64 => High == 0;

    /// <summary>The integer this key holds; throws for a GUID key.</summary>
    public long AsInt64() => IsInt64
        ? unchecked((long)(Low ^ SignBit))
        : throw new InvalidCastException($"Row key {this} is not an integer.");

    /// <summary>The GUID this key holds (any key reads as a GUID; meaningful for GUID-keyed tables).</summary>
    public Guid AsGuid()
    {
        Span<byte> bytes = stackalloc byte[16];
        WriteTo(bytes);
        return ReadGuidBigEndian(bytes);
    }

    /// <summary>Writes the 16 bytes big-endian (their sort order).</summary>
    public void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt64BigEndian(destination, High);
        BinaryPrimitives.WriteUInt64BigEndian(destination[8..], Low);
    }

    public static implicit operator RowKey(long value) => FromInt64(value);

    public static explicit operator long(RowKey key) => key.AsInt64();

    public bool Equals(RowKey other) => High == other.High && Low == other.Low;

    public override bool Equals(object? obj) => obj is RowKey other && Equals(other);

    public override int GetHashCode()
    {
        // Integer keys differ only in the low half; mix both halves so neither dominates.
        ulong mixed = (High * 0x9E37_79B9_7F4A_7C15UL) ^ Low;
        return unchecked((int)mixed ^ (int)(mixed >> 32));
    }

    public int CompareTo(RowKey other)
    {
        int high = High.CompareTo(other.High);
        return high != 0 ? high : Low.CompareTo(other.Low);
    }

    public static bool operator ==(RowKey left, RowKey right) => left.Equals(right);

    public static bool operator !=(RowKey left, RowKey right) => !left.Equals(right);

    public static bool operator <(RowKey left, RowKey right) => left.CompareTo(right) < 0;

    public static bool operator >(RowKey left, RowKey right) => left.CompareTo(right) > 0;

    public static bool operator <=(RowKey left, RowKey right) => left.CompareTo(right) <= 0;

    public static bool operator >=(RowKey left, RowKey right) => left.CompareTo(right) >= 0;

    /// <summary>An integer key prints as the integer, any other key as a GUID.</summary>
    public override string ToString() =>
        IsInt64 ? AsInt64().ToString(CultureInfo.InvariantCulture) : AsGuid().ToString("D");

    private static void WriteGuidBigEndian(Guid value, Span<byte> destination)
    {
        // Guid's own layout stores its first three fields little-endian; RFC order is big-endian.
#if NET8_0_OR_GREATER
        value.TryWriteBytes(destination, bigEndian: true, out _);
#else
        byte[] bytes = value.ToByteArray();
        destination[0] = bytes[3];
        destination[1] = bytes[2];
        destination[2] = bytes[1];
        destination[3] = bytes[0];
        destination[4] = bytes[5];
        destination[5] = bytes[4];
        destination[6] = bytes[7];
        destination[7] = bytes[6];
        bytes.AsSpan(8, 8).CopyTo(destination[8..]);
#endif
    }

    private static Guid ReadGuidBigEndian(ReadOnlySpan<byte> source)
    {
#if NET8_0_OR_GREATER
        return new Guid(source, bigEndian: true);
#else
        byte[] bytes = new byte[16];
        bytes[0] = source[3];
        bytes[1] = source[2];
        bytes[2] = source[1];
        bytes[3] = source[0];
        bytes[4] = source[5];
        bytes[5] = source[4];
        bytes[6] = source[7];
        bytes[7] = source[6];
        source[8..16].CopyTo(bytes.AsSpan(8));
        return new Guid(bytes);
#endif
    }
}
