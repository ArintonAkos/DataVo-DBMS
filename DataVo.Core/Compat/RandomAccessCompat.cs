using Microsoft.Win32.SafeHandles;
#if NETSTANDARD2_1
using System.Runtime.CompilerServices;
#endif

namespace DataVo.Core.Compat;

internal static class RandomAccessCompat
{
#if NETSTANDARD2_1
    private sealed class StreamBox(FileStream stream)
    {
        public FileStream Stream { get; } = stream;
    }

    private static readonly ConditionalWeakTable<SafeFileHandle, StreamBox> RegisteredStreams = new();

    public static void Register(SafeFileHandle handle, FileStream stream)
    {
        RegisteredStreams.Remove(handle);
        RegisteredStreams.Add(handle, new StreamBox(stream));
    }

    public static void Unregister(SafeFileHandle handle)
    {
        RegisteredStreams.Remove(handle);
    }

    private static FileStream GetRegisteredStream(SafeFileHandle handle)
    {
        if (RegisteredStreams.TryGetValue(handle, out StreamBox? box))
        {
            return box.Stream;
        }

        throw new InvalidOperationException("SafeFileHandle is not registered for netstandard2.1 random access.");
    }
#endif

    public static long GetLength(SafeFileHandle handle)
    {
#if NET6_0_OR_GREATER
        return RandomAccess.GetLength(handle);
#else
        FileStream stream = GetRegisteredStream(handle);
        lock (stream)
        {
            return stream.Length;
        }
#endif
    }

    public static void Write(SafeFileHandle handle, ReadOnlySpan<byte> bytes, long fileOffset)
    {
#if NET6_0_OR_GREATER
        RandomAccess.Write(handle, bytes, fileOffset);
#else
        FileStream stream = GetRegisteredStream(handle);
        lock (stream)
        {
            stream.Seek(fileOffset, SeekOrigin.Begin);
            stream.Write(bytes);
        }
#endif
    }

    public static int Read(SafeFileHandle handle, Span<byte> buffer, long fileOffset)
    {
#if NET6_0_OR_GREATER
        return RandomAccess.Read(handle, buffer, fileOffset);
#else
        FileStream stream = GetRegisteredStream(handle);
        lock (stream)
        {
            stream.Seek(fileOffset, SeekOrigin.Begin);
            return stream.Read(buffer);
        }
#endif
    }

    public static void SetLength(SafeFileHandle handle, long length)
    {
#if NET6_0_OR_GREATER
        RandomAccess.SetLength(handle, length);
#else
        FileStream stream = GetRegisteredStream(handle);
        lock (stream)
        {
            stream.SetLength(length);
        }
#endif
    }

    public static void FlushToDisk(SafeFileHandle handle)
    {
#if NET6_0_OR_GREATER
        RandomAccess.FlushToDisk(handle);
#else
        FileStream stream = GetRegisteredStream(handle);
        lock (stream)
        {
            stream.Flush(flushToDisk: true);
        }
#endif
    }
}
