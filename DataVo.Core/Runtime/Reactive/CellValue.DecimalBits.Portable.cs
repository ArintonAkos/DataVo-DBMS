#if NETSTANDARD2_1
namespace DataVo.Core.Runtime.Reactive;

public readonly partial struct CellValue
{
    private static long Int64BitsFromDecimal(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        ulong raw = (uint)bits[0] | ((ulong)(uint)bits[1] << 32);
        return unchecked((long)raw);
    }
}
#endif
