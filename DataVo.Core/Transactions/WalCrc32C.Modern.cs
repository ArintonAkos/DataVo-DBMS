#if NET10_0_OR_GREATER
using System.Buffers.Binary;
using System.Numerics;

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

        while (data.Length >= sizeof(ulong))
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data[..sizeof(ulong)]));
            data = data[sizeof(ulong)..];
        }

        if (data.Length >= sizeof(uint))
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt32LittleEndian(data[..sizeof(uint)]));
            data = data[sizeof(uint)..];
        }

        if (data.Length >= sizeof(ushort))
        {
            crc = BitOperations.Crc32C(crc, BinaryPrimitives.ReadUInt16LittleEndian(data[..sizeof(ushort)]));
            data = data[sizeof(ushort)..];
        }

        if (data.Length != 0)
        {
            crc = BitOperations.Crc32C(crc, data[0]);
        }

        return crc;
    }
}
#endif
