using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace DataVo.Core.Indexing;

internal sealed class GuidSimdEqualityComparer : IEqualityComparer<Guid>
{
    public static readonly GuidSimdEqualityComparer Instance = new();

    private GuidSimdEqualityComparer()
    {
    }

    public bool Equals(Guid x, Guid y)
    {
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
    }

    public int GetHashCode(Guid obj) => obj.GetHashCode();
}
