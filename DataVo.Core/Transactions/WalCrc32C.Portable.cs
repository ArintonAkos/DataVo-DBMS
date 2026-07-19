#if NETSTANDARD2_1
namespace DataVo.Core.Transactions;

internal static class WalCrc32C
{
    public static uint HashToUInt32(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
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

        return ~crc;
    }
}
#endif
