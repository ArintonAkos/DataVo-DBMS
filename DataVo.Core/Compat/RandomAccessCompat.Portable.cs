#if NETSTANDARD2_1
using System.Collections.Concurrent;
using Microsoft.Win32.SafeHandles;

namespace DataVo.Core.Compat;

/// <summary>
/// Positional file access for runtimes without <c>System.IO.RandomAccess</c>. Each handle is backed by an
/// unbuffered stream, and a per-stream lock makes each seek-then-transfer atomic.
/// </summary>
internal static class RandomAccessCompat
{
    private static readonly ConcurrentDictionary<SafeFileHandle, FileStream> Streams = new();

    public static SafeFileHandle Open(string path)
    {
        var stream = new FileStream(
            path,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.ReadWrite | FileShare.Delete,
            bufferSize: 1,
            FileOptions.RandomAccess);
        SafeFileHandle handle = stream.SafeFileHandle;
        Streams[handle] = stream;
        return handle;
    }

    public static void Close(SafeFileHandle handle)
    {
        if (Streams.TryRemove(handle, out FileStream? stream))
        {
            stream.Dispose();
        }
    }

    public static long GetLength(SafeFileHandle handle)
    {
        FileStream stream = Streams[handle];
        lock (stream)
        {
            return stream.Length;
        }
    }

    public static void Write(SafeFileHandle handle, ReadOnlySpan<byte> bytes, long fileOffset)
    {
        FileStream stream = Streams[handle];
        lock (stream)
        {
            stream.Seek(fileOffset, SeekOrigin.Begin);
            stream.Write(bytes);
            stream.Flush(flushToDisk: false);
        }
    }

    public static int Read(SafeFileHandle handle, Span<byte> buffer, long fileOffset)
    {
        FileStream stream = Streams[handle];
        lock (stream)
        {
            stream.Seek(fileOffset, SeekOrigin.Begin);
            return stream.Read(buffer);
        }
    }

    public static void SetLength(SafeFileHandle handle, long length)
    {
        FileStream stream = Streams[handle];
        lock (stream)
        {
            stream.SetLength(length);
        }
    }

    public static void FlushToDisk(SafeFileHandle handle)
    {
        FileStream stream = Streams[handle];
        lock (stream)
        {
            stream.Flush(flushToDisk: true);
        }
    }
}
#endif
