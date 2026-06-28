using System.Buffers;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// A single-writer bump allocator backed by pooled byte slabs. Allocations carve forward through the
/// current slab with no per-call heap allocation; crossing a slab boundary rents a fresh slab from
/// <see cref="ArrayPool{T}.Shared"/>. <see cref="Reset"/> returns every slab to the pool, so an arena —
/// and thus a MemTable generation — recycles its memory with ~0 steady-state GC pressure.
/// <para>Not thread-safe for concurrent allocation; the LSM commit pipeline is the sole writer.</para>
/// </summary>
public sealed class Arena : IDisposable
{
    private readonly int _slabSize;
    private readonly List<byte[]> _slabs = [];
    private byte[] _current;
    private int _offset;
    private long _bytesAllocated;
    private bool _disposed;

    /// <summary>Creates an arena whose standard slab is <paramref name="slabSize"/> bytes.</summary>
    public Arena(int slabSize = 1 << 20)
    {
        if (slabSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(slabSize));
        }

        _slabSize = slabSize;
        _current = ArrayPool<byte>.Shared.Rent(slabSize);
        _slabs.Add(_current);
    }

    /// <summary>Total bytes handed out since construction or the last <see cref="Reset"/>.</summary>
    public long BytesAllocated => _bytesAllocated;

    /// <summary>
    /// Carves <paramref name="size"/> bytes from the arena and returns a writable span over them. The
    /// span stays valid until the next <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public Span<byte> Allocate(int size) => Allocate(size, out _);

    /// <summary>
    /// Carves <paramref name="size"/> bytes and returns both the writable span and a stable
    /// <paramref name="handle"/> that <see cref="Resolve"/> maps back to those bytes. The handle packs the
    /// slab index in the high 32 bits and the in-slab offset in the low 32 bits; it stays valid until the
    /// next <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public Span<byte> Allocate(int size, out long handle)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (size < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size));
        }

        if (_offset + size > _current.Length)
        {
            // Oversized requests get a dedicated exact slab; otherwise rent a fresh standard slab.
            int rent = Math.Max(size, _slabSize);
            _current = ArrayPool<byte>.Shared.Rent(rent);
            _slabs.Add(_current);
            _offset = 0;
        }

        int slabIndex = _slabs.Count - 1;
        handle = ((long)slabIndex << 32) | (uint)_offset;

        Span<byte> span = _current.AsSpan(_offset, size);
        _offset += size;
        _bytesAllocated += size;
        return span;
    }

    /// <summary>
    /// Maps a handle returned by <see cref="Allocate(int, out long)"/> back to its bytes. The returned span
    /// is valid until the next <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public Span<byte> Resolve(long handle, int length)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        int slabIndex = (int)(handle >> 32);
        int offset = (int)(handle & 0xFFFFFFFF);
        return _slabs[slabIndex].AsSpan(offset, length);
    }

    /// <summary>Returns every slab to the pool and re-arms the arena with a single fresh slab.</summary>
    public void Reset()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (byte[] slab in _slabs)
        {
            ArrayPool<byte>.Shared.Return(slab);
        }

        _slabs.Clear();
        _current = ArrayPool<byte>.Shared.Rent(_slabSize);
        _slabs.Add(_current);
        _offset = 0;
        _bytesAllocated = 0;
    }

    /// <summary>Returns all slabs to the pool. The arena must not be used after disposal.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        foreach (byte[] slab in _slabs)
        {
            ArrayPool<byte>.Shared.Return(slab);
        }

        _slabs.Clear();
    }
}
