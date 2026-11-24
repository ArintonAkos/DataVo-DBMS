using System.Runtime.CompilerServices;

namespace DataVo.Core.Internal.Polyfills;

/// <summary>
/// <c>System.Numerics.BitOperations</c> where the runtime has it (net8.0+, a single hardware instruction), and
/// portable bit-twiddling fallbacks on netstandard2.1, which lacks the type.
/// </summary>
internal static class BitOperationsCompat
{
    /// <summary>The number of set bits in <paramref name="value"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int PopCount(ulong value)
    {
#if NET8_0_OR_GREATER
        return System.Numerics.BitOperations.PopCount(value);
#else
        value -= (value >> 1) & 0x5555_5555_5555_5555UL;
        value = (value & 0x3333_3333_3333_3333UL) + ((value >> 2) & 0x3333_3333_3333_3333UL);
        value = (value + (value >> 4)) & 0x0F0F_0F0F_0F0F_0F0FUL;
        return (int)((value * 0x0101_0101_0101_0101UL) >> 56);
#endif
    }

    /// <summary>The number of trailing zero bits; 64 when <paramref name="value"/> is zero.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int TrailingZeroCount(ulong value)
    {
#if NET8_0_OR_GREATER
        return System.Numerics.BitOperations.TrailingZeroCount(value);
#else
        return value == 0 ? 64 : PopCount((value & (~value + 1)) - 1);
#endif
    }

    /// <summary>The number of leading zero bits; 64 when <paramref name="value"/> is zero.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int LeadingZeroCount(ulong value)
    {
#if NET8_0_OR_GREATER
        return System.Numerics.BitOperations.LeadingZeroCount(value);
#else
        if (value == 0)
        {
            return 64;
        }

        int count = 0;
        while ((value & 0x8000_0000_0000_0000UL) == 0)
        {
            value <<= 1;
            count++;
        }

        return count;
#endif
    }
}
