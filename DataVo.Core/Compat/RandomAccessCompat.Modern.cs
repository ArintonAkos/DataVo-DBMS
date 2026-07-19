#if NET10_0_OR_GREATER
using Microsoft.Win32.SafeHandles;

namespace DataVo.Core.Compat;

internal static class RandomAccessCompat
{
    public static long GetLength(SafeFileHandle handle) => RandomAccess.GetLength(handle);

    public static void Write(SafeFileHandle handle, ReadOnlySpan<byte> bytes, long fileOffset) =>
        RandomAccess.Write(handle, bytes, fileOffset);

    public static int Read(SafeFileHandle handle, Span<byte> buffer, long fileOffset) =>
        RandomAccess.Read(handle, buffer, fileOffset);

    public static void SetLength(SafeFileHandle handle, long length) =>
        RandomAccess.SetLength(handle, length);

    public static void FlushToDisk(SafeFileHandle handle) => RandomAccess.FlushToDisk(handle);
}
#endif
