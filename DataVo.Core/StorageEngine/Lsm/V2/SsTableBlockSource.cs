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

/// <summary>Checks one SSTable data block (CRC and structure) and throws <see cref="InvalidDataException"/> when it is bad.</summary>
internal interface ISsTableBlockVerifier
{
    /// <summary>
    /// Verifies block <paramref name="block"/>. <paramref name="physicalRead"/> is true when the bytes were just
    /// read from disk into memory: those are always checked in full. False means the bytes are mapped file
    /// memory, which a reader may check only on first use.
    /// </summary>
    void VerifyBlock(int block, ReadOnlySpan<byte> bytes, bool physicalRead);
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

    /// <summary>
    /// Borrows the verified bytes of data block <paramref name="block"/> at [offset, offset + length). Bytes read
    /// from disk are verified by <paramref name="verifier"/> every time, before anyone sees or caches them.
    /// </summary>
    public abstract SsTableBlockLease ReadBlock(long offset, int length, int block, ISsTableBlockVerifier verifier);

    public abstract void Dispose();

    public static SsTableBlockSource Open(string path, LsmReadMode mode) => Open(path, mode, cache: null);

    /// <summary>Opens <paramref name="path"/>; positional sources serve data blocks through <paramref name="cache"/> when given.</summary>
    public static SsTableBlockSource Open(string path, LsmReadMode mode, LsmBlockCache? cache)
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

        return new PositionalBlockSource(path, cache);
    }

    /// <summary>The mode that <see cref="LsmReadMode.Auto"/> resolves to on this process.</summary>
    public static LsmReadMode ResolveMode(LsmReadMode mode)
    {
        if (mode != LsmReadMode.Auto)
        {
            return mode;
        }

#if !NET8_0_OR_GREATER
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

    public override SsTableBlockLease ReadBlock(long offset, int length, int block, ISsTableBlockVerifier verifier)
    {
        ThrowIfOutOfRange(offset, length, _length);
        var bytes = new ReadOnlySpan<byte>(_base + offset, length);
        verifier.VerifyBlock(block, bytes, physicalRead: false);
        return SsTableBlockLease.Mapped((IntPtr)(_base + offset), length);
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
/// Positional reads for targets without memory mapping (or when mapping is not wanted). Metadata regions are
/// read once into arrays. A data block is one pread into a pooled buffer, CRC-verified on every physical read;
/// with an <see cref="LsmBlockCache"/>, verified blocks are kept and later reads of them touch neither the
/// disk nor the managed heap.
/// </summary>
/// <remarks>
/// Unlike a memory mapping, a truncated or failing file surfaces as an <see cref="InvalidDataException"/> or
/// <see cref="IOException"/> on the reading thread, never as a SIGBUS, and bytes that rot on disk after a block
/// left the cache are caught by the CRC check of the next physical read.
/// </remarks>
internal sealed class PositionalBlockSource : SsTableBlockSource
{
    private readonly FileStream _stream;
    private readonly long _length;
    private readonly LsmBlockCache? _cache;
    private readonly long _sourceId;
    private int _disposed;
#if !NET6_0_OR_GREATER
    private readonly object _gate = new();
#endif

    public PositionalBlockSource(string path, LsmBlockCache? cache = null)
    {
        _stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, bufferSize: 1, FileOptions.RandomAccess);
        _length = _stream.Length;
        _cache = cache;
        _sourceId = cache is null ? 0 : LsmBlockCache.NextSourceId();
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

    public override SsTableBlockLease ReadBlock(long offset, int length, int block, ISsTableBlockVerifier verifier)
    {
        ThrowIfOutOfRange(offset, length, _length);
        LsmBlockCache? cache = _cache;
        if (cache is not null && cache.TryAcquire(_sourceId, offset, out LsmBlockCache.CachedBlock cached))
        {
            return SsTableBlockLease.Cached(cached);
        }

        byte[] buffer = cache is null ? ArrayPool<byte>.Shared.Rent(length) : cache.Rent(length);
        try
        {
            ReadExactly(offset, buffer.AsSpan(0, length));
            verifier.VerifyBlock(block, buffer.AsSpan(0, length), physicalRead: true);
        }
        catch
        {
            ReturnBuffer(cache, buffer);
            throw;
        }

        return cache is null
            ? SsTableBlockLease.Pooled(buffer, length, pool: null)
            : SsTableBlockLease.Cached(cache.InsertAndAcquire(_sourceId, offset, buffer, length));
    }

    public override void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _cache?.RemoveSource(_sourceId);
        _stream.Dispose();
    }

    private static void ReturnBuffer(LsmBlockCache? cache, byte[] buffer)
    {
        if (cache is null)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        else
        {
            cache.Return(buffer);
        }
    }

    private void ReadExactly(long offset, Span<byte> destination)
    {
#if NET6_0_OR_GREATER
        SafeFileHandle handle = _stream.SafeFileHandle;
        while (!destination.IsEmpty)
        {
            int read = RandomAccess.Read(handle, destination, offset);
            if (read == 0)
            {
                throw new InvalidDataException($"SSTable is truncated: it ended before the region at offset {offset}.");
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
                    throw new InvalidDataException($"SSTable is truncated: it ended before the region at offset {offset}.");
                }

                destination = destination[read..];
            }
        }
#endif
    }
}
