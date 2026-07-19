#if NET10_0_OR_GREATER
namespace DataVo.Core.Runtime.Reactive;

public readonly partial struct CellValue
{
    private static long Int64BitsFromDecimal(decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        ulong raw = (uint)bits[0] | ((ulong)(uint)bits[1] << 32);
        return unchecked((long)raw);
    }
}
#endif
