using System.Buffers;
using System.Runtime.CompilerServices;

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
    private const int Alignment = 8;

    private readonly int _slabSize;
    private readonly List<byte[]> _slabs = [];
    private readonly object _leaseSync = new();
    private byte[] _current;
    private int _offset;
    private long _bytesAllocated;
    private int _activeLeases;
    private bool _disposeRequested;
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
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);

        if (size < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size));
        }

        int alignedOffset = AlignUp(_offset);
        if (alignedOffset + size > _current.Length)
        {
            // Oversized requests get a dedicated exact slab; otherwise rent a fresh standard slab.
            int rent = Math.Max(size, _slabSize);
            _current = ArrayPool<byte>.Shared.Rent(rent);
            _slabs.Add(_current);
            alignedOffset = 0;
        }

        int slabIndex = _slabs.Count - 1;
        handle = ((long)slabIndex << 32) | (uint)alignedOffset;

        Span<byte> span = _current.AsSpan(alignedOffset, size);
        _offset = alignedOffset + size;
        _bytesAllocated += size;
        return span;
    }

    /// <summary>
    /// Maps a handle returned by <see cref="Allocate(int, out long)"/> back to its bytes. The returned span
    /// is valid until the next <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public Span<byte> Resolve(long handle, int length)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);

        int slabIndex = (int)(handle >> 32);
        int offset = (int)(handle & 0xFFFFFFFF);
        return _slabs[slabIndex].AsSpan(offset, length);
    }

    internal ref long ResolveInt64Reference(long handle, int offsetInAllocation)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);

        int slabIndex = (int)(handle >> 32);
        int offset = (int)(handle & 0xFFFFFFFF) + offsetInAllocation;
        return ref Unsafe.As<byte, long>(ref _slabs[slabIndex][offset]);
    }

    /// <summary>
    /// Returns every slab to the pool and re-arms the arena with a single fresh slab. Rejected while
    /// read leases are active: a reset would recycle memory a leased reader may still address.
    /// </summary>
    public void Reset()
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
        lock (_leaseSync)
        {
            if (_activeLeases > 0)
            {
                throw new InvalidOperationException("Cannot reset an arena while read leases are active.");
            }
        }

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

    /// <summary>
    /// Registers a read lease that pins the arena's slabs in place. While any lease is active,
    /// <see cref="Dispose"/> defers returning slabs to the pool, so spans handed out by
    /// <see cref="Resolve"/> stay valid for the lease's lifetime even if the owning generation is
    /// flushed and disposed concurrently. Dispose the returned lease to release the pin; the last
    /// release after a deferred dispose performs the actual slab return.
    /// </summary>
    public ArenaLease AcquireLease()
    {
        lock (_leaseSync)
        {
            DataVo.Core.Compat.ThrowHelper.ThrowIfDisposed(_disposed, this);
            _activeLeases++;
            return new ArenaLease(this);
        }
    }

    internal void ReleaseLease()
    {
        lock (_leaseSync)
        {
            if (_activeLeases <= 0)
            {
                throw new InvalidOperationException("Arena lease released more times than acquired.");
            }

            _activeLeases--;
            if (_activeLeases == 0 && _disposeRequested && !_disposed)
            {
                ReturnSlabsNoLock();
            }
        }
    }

    /// <summary>
    /// Returns all slabs to the pool once no read lease is active; with live leases the return is
    /// deferred to the final lease release. The arena accepts no new allocations or leases after
    /// this call.
    /// </summary>
    public void Dispose()
    {
        lock (_leaseSync)
        {
            if (_disposed || _disposeRequested)
            {
                _disposeRequested = true;
                return;
            }

            _disposeRequested = true;
            if (_activeLeases == 0)
            {
                ReturnSlabsNoLock();
            }
        }
    }

    private void ReturnSlabsNoLock()
    {
        _disposed = true;
        foreach (byte[] slab in _slabs)
        {
            ArrayPool<byte>.Shared.Return(slab);
        }

        _slabs.Clear();
    }

    private static int AlignUp(int value) => (value + (Alignment - 1)) & ~(Alignment - 1);
}

/// <summary>
/// A pin on an <see cref="Arena"/>'s slabs (see <see cref="Arena.AcquireLease"/>). Dispose exactly once.
/// </summary>
public struct ArenaLease : IDisposable
{
    private Arena? _arena;

    internal ArenaLease(Arena arena)
    {
        _arena = arena;
    }

    /// <summary>Releases the pin; the last release after a deferred dispose returns the slabs.</summary>
    public void Dispose()
    {
        Arena? arena = _arena;
        _arena = null;
        arena?.ReleaseLease();
    }
}
