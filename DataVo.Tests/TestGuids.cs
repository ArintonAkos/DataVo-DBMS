using System.Security.Cryptography;

namespace DataVo.Tests;

/// <summary>
/// Version 7 (time-ordered) GUIDs for tests on every target framework. .NET 9 added
/// <c>Guid.CreateVersion7</c>; on .NET 8 the same RFC 9562 layout is built here: a 48-bit big-endian Unix
/// millisecond timestamp, the version nibble 7, the variant bits <c>10</c>, and random bits elsewhere.
/// </summary>
/// <example>
/// <code>
/// Guid key = TestGuids.CreateVersion7();
/// </code>
/// </example>
internal static class TestGuids
{
    /// <summary>Creates a version 7 GUID for the current time.</summary>
    public static Guid CreateVersion7() => CreateVersion7(DateTimeOffset.UtcNow);

    /// <summary>Creates a version 7 GUID for the given time.</summary>
    /// <param name="timestamp">The time encoded in the GUID's leading 48 bits.</param>
    public static Guid CreateVersion7(DateTimeOffset timestamp)
    {
#if NET9_0_OR_GREATER
        return Guid.CreateVersion7(timestamp);
#else
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);
        long milliseconds = timestamp.ToUnixTimeMilliseconds();
        for (int i = 0; i < 6; i++)
        {
            bytes[i] = (byte)(milliseconds >> (8 * (5 - i)));
        }

        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x70);
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes, bigEndian: true);
#endif
    }
}
