using System.Runtime.InteropServices;
#if NET10_0_OR_GREATER
using System.Runtime.Intrinsics;
#endif

namespace DataVo.Core.Indexing;

internal sealed class GuidSimdEqualityComparer : IEqualityComparer<Guid>
{
    public static readonly GuidSimdEqualityComparer Instance = new();

    private GuidSimdEqualityComparer()
    {
    }

    public bool Equals(Guid x, Guid y)
    {
#if NET10_0_OR_GREATER
        if (!Vector128.IsHardwareAccelerated)
        {
            return x.Equals(y);
        }

        Span<byte> leftBytes = stackalloc byte[16];
        Span<byte> rightBytes = stackalloc byte[16];
        x.TryWriteBytes(leftBytes);
        y.TryWriteBytes(rightBytes);

        Vector128<byte> left = MemoryMarshal.Read<Vector128<byte>>(leftBytes);
        Vector128<byte> right = MemoryMarshal.Read<Vector128<byte>>(rightBytes);
        Vector128<byte> equalBytes = Vector128.Equals(left, right);

        return Vector128.EqualsAll(equalBytes, Vector128<byte>.AllBitsSet);
#else
        return x.Equals(y);
#endif
    }

    public int GetHashCode(Guid obj) => obj.GetHashCode();
}
