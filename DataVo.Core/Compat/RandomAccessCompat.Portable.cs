#if NETSTANDARD2_1
using Microsoft.Win32.SafeHandles;

namespace DataVo.Core.Compat;

internal static class RandomAccessCompat
{
    private const string UnsupportedMessage =
        "Random-access durable storage is not supported by the experimental netstandard2.1 target. " +
        "Use StorageMode.InMemory.";

    public static long GetLength(SafeFileHandle handle) => throw CreateUnsupportedException();

    public static void Write(SafeFileHandle handle, ReadOnlySpan<byte> bytes, long fileOffset) =>
        throw CreateUnsupportedException();

    public static int Read(SafeFileHandle handle, Span<byte> buffer, long fileOffset) =>
        throw CreateUnsupportedException();

    public static void SetLength(SafeFileHandle handle, long length) =>
        throw CreateUnsupportedException();

    public static void FlushToDisk(SafeFileHandle handle) => throw CreateUnsupportedException();

    private static PlatformNotSupportedException CreateUnsupportedException() => new(UnsupportedMessage);
}
#endif
