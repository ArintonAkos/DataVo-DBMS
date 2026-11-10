using System.Buffers;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>How SSTable bytes are read.</summary>
public enum LsmReadMode
{
    /// <summary>Memory-map where supported (64-bit, not browser), otherwise positional reads.</summary>
    Auto = 0,

    /// <summary>Map files read-only: the OS page cache is read directly, no copy and no system call per read.</summary>
    MemoryMapped = 1,

    /// <summary>Positional reads (pread) into pooled buffers; for targets without memory mapping.</summary>
    PositionalRead = 2,
}

/// <summary>Callback receiving a span that is valid only for the duration of the call.</summary>
internal delegate TResult SpanFunc<TState, TResult>(ReadOnlySpan<byte> bytes, TState state);

/// <summary>
/// Read access to one immutable SSTable file. Metadata regions (index, Bloom filter, properties) are
/// exposed as <see cref="ReadOnlyMemory{T}"/> that stays valid while the source is open; data blocks
/// are read through a callback so a positional source can lend a pooled buffer for exactly that long.
/// </summary>
internal abstract class SsTableBlockSource : IDisposable
{
    public abstract long Length { get; }

    /// <summary>True when blocks are served straight from mapped memory (zero copy).</summary>
    public abstract bool IsMapped { get; }

    /// <summary>A region that stays readable until the source is disposed.</summary>
    public abstract ReadOnlyMemory<byte> GetRegion(long offset, int length);

    /// <summary>
    /// The address of [offset, offset + length) when the source is a memory mapping, so a reader can take a span
    /// without wrapping the region in an object. Valid while the source is open.
    /// </summary>
    public virtual bool TryGetRegionAddress(long offset, int length, out IntPtr address)
    {
        address = IntPtr.Zero;
        return false;
    }

    /// <summary>Invokes <paramref name="func"/> with the bytes of [offset, offset + length).</summary>
    public abstract TResult Read<TState, TResult>(long offset, int length, TState state, SpanFunc<TState, TResult> func);

    public abstract void Dispose();

    public static SsTableBlockSource Open(string path, LsmReadMode mode)
    {
        if (ResolveMode(mode) == LsmReadMode.MemoryMapped)
        {
            try
            {
                return new MappedBlockSource(path);
            }
            catch (Exception ex) when (mode == LsmReadMode.Auto && ex is PlatformNotSupportedException or NotSupportedException or IOException)
            {
                // Mapping is unavailable here (or the address space is exhausted): fall back to pread.
            }
        }

        return new PositionalBlockSource(path);
    }

    /// <summary>The mode that <see cref="LsmReadMode.Auto"/> resolves to on this process.</summary>
    public static LsmReadMode ResolveMode(LsmReadMode mode)
    {
        if (mode != LsmReadMode.Auto)
        {
            return mode;
        }

#if NETSTANDARD2_1
        // Memory mapping is not available on every runtime this target serves; plain reads work everywhere.
        return LsmReadMode.PositionalRead;
#else
#if NET5_0_OR_GREATER
        bool browser = OperatingSystem.IsBrowser() || OperatingSystem.IsWasi();
#else
        bool browser = RuntimeInformation.OSDescription.IndexOf("Browser", StringComparison.OrdinalIgnoreCase) >= 0;
#endif
        return Environment.Is64BitProcess && !browser ? LsmReadMode.MemoryMapped : LsmReadMode.PositionalRead;
#endif
    }

    protected static void ThrowIfOutOfRange(long offset, int length, long fileLength)
    {
        if (offset < 0 || length < 0 || offset > fileLength - length)
        {
            throw new InvalidDataException($"SSTable region [{offset}, +{length}) lies outside the {fileLength}-byte file.");
        }
    }
}

/// <summary>
/// Read-only memory mapping of a whole SSTable. The base pointer is acquired once; reads are plain
/// memory accesses into the OS page cache (a cold page costs one page fault, a warm page nothing).
/// SSTables are immutable and never truncated while mapped, so the mapping cannot be invalidated
/// underneath a reader.
/// </summary>
internal sealed unsafe class MappedBlockSource : SsTableBlockSource
{
    private readonly MemoryMappedFile _file;
    private readonly MemoryMappedViewAccessor _view;
    private readonly long _length;
    private byte* _base;
    private int _disposed;

    public MappedBlockSource(string path)
    {
        // FileShare.Delete lets compaction rename/delete around us on Unix; on Windows deletion still
        // waits until the mapping is released, which the handle reference count guarantees.
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
        try
        {
            _length = stream.Length;
            if (_length == 0)
            {
                throw new InvalidDataException($"SSTable '{path}' is empty.");
            }

            _file = MemoryMappedFile.CreateFromFile(stream, mapName: null, capacity: 0, MemoryMappedFileAccess.Read, HandleInheritability.None, leaveOpen: false);
        }
        catch
        {
            stream.Dispose();
            throw;
        }

        try
        {
            _view = _file.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
            byte* pointer = null;
            _view.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
            _base = pointer + _view.PointerOffset;
        }
        catch
        {
            _view?.Dispose();
            _file.Dispose();
            throw;
        }
    }

    public override long Length => _length;

    public override bool IsMapped => true;

    public override ReadOnlyMemory<byte> GetRegion(long offset, int length)
    {
        ThrowIfOutOfRange(offset, length, _length);
        return new UnmanagedRegion(_base + offset, length).Memory;
    }

    public override bool TryGetRegionAddress(long offset, int length, out IntPtr address)
    {
        ThrowIfOutOfRange(offset, length, _length);
        address = (IntPtr)(_base + offset);
        return true;
    }

    public override TResult Read<TState, TResult>(long offset, int length, TState state, SpanFunc<TState, TResult> func)
    {
        ThrowIfOutOfRange(offset, length, _length);
        return func(new ReadOnlySpan<byte>(_base + offset, length), state);
    }

    public override void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _base = null;
        _view.SafeMemoryMappedViewHandle.ReleasePointer();
        _view.Dispose();
        _file.Dispose();
    }

    /// <summary>Exposes mapped bytes as <see cref="Memory{T}"/> without copying.</summary>
    private sealed class UnmanagedRegion(byte* pointer, int length) : MemoryManager<byte>
    {
        public override Span<byte> GetSpan() => new(pointer, length);

        public override MemoryHandle Pin(int elementIndex = 0) => new(pointer + elementIndex);

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}

/// <summary>
/// Positional reads for targets without memory mapping. Metadata regions are read once into arrays;
/// each block read is one pread into a pooled buffer that is returned after the callback.
/// </summary>
internal sealed class PositionalBlockSource : SsTableBlockSource
{
    private readonly FileStream _stream;
    private readonly long _length;
#if !NET6_0_OR_GREATER
    private readonly object _gate = new();
#endif

    public PositionalBlockSource(string path)
    {
        _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, bufferSize: 1, FileOptions.RandomAccess);
        _length = _stream.Length;
    }

    public override long Length => _length;

    public override bool IsMapped => false;

    public override ReadOnlyMemory<byte> GetRegion(long offset, int length)
    {
        ThrowIfOutOfRange(offset, length, _length);
        byte[] bytes = new byte[length];
        ReadExactly(offset, bytes);
        return bytes;
    }

    public override TResult Read<TState, TResult>(long offset, int length, TState state, SpanFunc<TState, TResult> func)
    {
        ThrowIfOutOfRange(offset, length, _length);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            ReadExactly(offset, buffer.AsSpan(0, length));
            return func(buffer.AsSpan(0, length), state);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public override void Dispose() => _stream.Dispose();

    private void ReadExactly(long offset, Span<byte> destination)
    {
#if NET6_0_OR_GREATER
        SafeFileHandle handle = _stream.SafeFileHandle;
        while (!destination.IsEmpty)
        {
            int read = RandomAccess.Read(handle, destination, offset);
            if (read == 0)
            {
                throw new EndOfStreamException("SSTable ended before the requested region.");
            }

            destination = destination[read..];
            offset += read;
        }
#else
        lock (_gate)
        {
            _stream.Position = offset;
            while (!destination.IsEmpty)
            {
                int read = _stream.Read(destination);
                if (read == 0)
                {
                    throw new EndOfStreamException("SSTable ended before the requested region.");
                }

                destination = destination[read..];
            }
        }
#endif
    }
}
