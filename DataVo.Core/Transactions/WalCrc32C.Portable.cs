#if NETSTANDARD2_1
namespace DataVo.Core.Transactions;

internal static class WalCrc32C
{
    public static uint HashToUInt32(ReadOnlySpan<byte> data) => ~Append(Seed, data);

    /// <summary>Initial running state for <see cref="Append"/>.</summary>
    public const uint Seed = 0xFFFFFFFFu;

    /// <summary>
    /// Continues a running checksum over <paramref name="data"/>; the final CRC is the complement of the
    /// last state. Hashing in pieces gives the same result as hashing the concatenation.
    /// </summary>
    public static uint Append(uint crc, ReadOnlySpan<byte> data)
    {
        for (int i = 0; i < data.Length; i++)
        {
            crc ^= data[i];
            for (int bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1u) != 0
                    ? (crc >> 1) ^ 0x82F63B78u
                    : crc >> 1;
            }
        }

        return crc;
    }
}
#endif
