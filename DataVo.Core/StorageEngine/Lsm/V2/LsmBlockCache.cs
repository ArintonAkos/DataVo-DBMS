using System.Buffers;
using System.Collections.Concurrent;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// A sharded, thread-safe user-space cache of verified SSTable blocks for <see cref="LsmReadMode.PositionalRead"/>.
/// </summary>
/// <remarks>
/// <para>
/// Blocks are keyed by (source id, file offset) and spread over a power-of-two number of shards. Each shard
/// evicts with SIEVE: a FIFO queue plus one "visited" bit per block and a hand that sweeps from the oldest
/// block, sparing (and clearing) visited blocks and evicting the first unvisited one. A hit only sets the bit,
/// so the hit path takes no lock: one dictionary probe and one compare-and-swap on the block's reference count.
/// </para>
/// <para>
/// Block bytes live in arrays rented from a pool owned by the cache. A cached block holds one reference for
/// the cache and one per outstanding <see cref="SsTableBlockLease"/>; its array goes back to the pool only when
/// the last reference is released, so eviction never recycles bytes a reader is still looking at.
/// Only bytes whose CRC was checked on the physical read are ever inserted.
/// </para>
/// </remarks>
public sealed class LsmBlockCache
{
    /// <summary>The default number of shards.</summary>
    public const int DefaultShardCount = 32;

    private readonly Shard[] _shards;
    private readonly int _shardMask;
    private readonly ArrayPool<byte> _pool;
    private long _hits;
    private long _misses;
    private long _evictions;

    /// <summary>Creates a cache holding up to <paramref name="capacityBytes"/> bytes of block data.</summary>
    /// <param name="capacityBytes">The byte budget across all shards; must be positive.</param>
    /// <param name="shardCount">The number of shards, rounded up to a power of two.</param>
    public LsmBlockCache(long capacityBytes, int shardCount = DefaultShardCount)
    {
        if (capacityBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), "The block cache capacity must be positive.");
        }

        if (shardCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(shardCount));
        }

        int shards = 1;
        while (shards < shardCount)
        {
            shards <<= 1;
        }

        CapacityBytes = capacityBytes;
        _shardMask = shards - 1;
        _shards = new Shard[shards];
        long perShard = Math.Max(1, capacityBytes / shards);
        for (int i = 0; i < shards; i++)
        {
            _shards[i] = new Shard(this, perShard);
        }

        // A private pool: the cache can hold far more arrays than the shared pool keeps per bucket.
        _pool = ArrayPool<byte>.Create(maxArrayLength: 16 * 1024 * 1024, maxArraysPerBucket: 1024);
    }

    /// <summary>The configured byte budget.</summary>
    public long CapacityBytes { get; }

    /// <summary>Lookups served from the cache.</summary>
    public long Hits => Interlocked.Read(ref _hits);

    /// <summary>Lookups that went to disk.</summary>
    public long Misses => Interlocked.Read(ref _misses);

    /// <summary>Blocks evicted to stay within the budget.</summary>
    public long Evictions => Interlocked.Read(ref _evictions);

    /// <summary>The number of blocks currently cached.</summary>
    public int Count
    {
        get
        {
            int count = 0;
            foreach (Shard shard in _shards)
            {
                count += shard.Count;
            }

            return count;
        }
    }

    /// <summary>The bytes of block data currently cached (by rented array size).</summary>
    public long SizeBytes
    {
        get
        {
            long size = 0;
            foreach (Shard shard in _shards)
            {
                size += shard.SizeBytes;
            }

            return size;
        }
    }

    /// <summary>Allocates a new id for an open SSTable source; ids are never reused.</summary>
    internal static long NextSourceId() => Interlocked.Increment(ref s_nextSourceId);

    private static long s_nextSourceId;

    /// <summary>Rents a buffer for a physical read from the cache's pool.</summary>
    internal byte[] Rent(int length) => _pool.Rent(length);

    /// <summary>Returns a buffer rented with <see cref="Rent"/> that was never inserted.</summary>
    internal void Return(byte[] buffer) => _pool.Return(buffer);

    /// <summary>Acquires a reference to a cached block; false on a miss.</summary>
    internal bool TryAcquire(long sourceId, long offset, out CachedBlock block)
    {
        var key = new BlockKey(sourceId, offset);
        if (ShardOf(key).TryAcquire(key, out block))
        {
            Interlocked.Increment(ref _hits);
            return true;
        }

        Interlocked.Increment(ref _misses);
        return false;
    }

    /// <summary>
    /// Inserts verified bytes rented from this cache and returns an acquired reference. When another thread
    /// cached the same block first, <paramref name="buffer"/> goes back to the pool and theirs is returned.
    /// </summary>
    internal CachedBlock InsertAndAcquire(long sourceId, long offset, byte[] buffer, int length)
    {
        var key = new BlockKey(sourceId, offset);
        return ShardOf(key).InsertAndAcquire(key, buffer, length);
    }

    /// <summary>Drops every block of a source that is closing.</summary>
    internal void RemoveSource(long sourceId)
    {
        foreach (Shard shard in _shards)
        {
            shard.RemoveSource(sourceId);
        }
    }

    /// <summary>Drops every cached block.</summary>
    public void Clear()
    {
        foreach (Shard shard in _shards)
        {
            shard.RemoveSource(sourceId: -1);
        }
    }

    private Shard ShardOf(BlockKey key) => _shards[key.GetHashCode() & _shardMask];

    private readonly struct BlockKey(long sourceId, long offset) : IEquatable<BlockKey>
    {
        public readonly long SourceId = sourceId;
        public readonly long Offset = offset;

        public bool Equals(BlockKey other) => SourceId == other.SourceId && Offset == other.Offset;

        public override bool Equals(object? obj) => obj is BlockKey other && Equals(other);

        public override int GetHashCode()
        {
            ulong hash = unchecked(((ulong)SourceId * 0x9E3779B97F4A7C15UL) ^ ((ulong)Offset * 0xC2B2AE3D27D4EB4FUL));
            hash ^= hash >> 29;
            return unchecked((int)hash ^ (int)(hash >> 32));
        }
    }

    /// <summary>One cached block: pooled bytes plus a reference count (the cache holds one reference).</summary>
    internal sealed class CachedBlock
    {
        private readonly LsmBlockCache _owner;
        private int _references = 1;

        internal CachedBlock(LsmBlockCache owner, long sourceId, long offset, byte[] buffer, int length)
        {
            _owner = owner;
            SourceId = sourceId;
            Offset = offset;
            Buffer = buffer;
            Length = length;
        }

        public long SourceId { get; }

        public long Offset { get; }

        public byte[] Buffer { get; private set; }

        public int Length { get; }

        // SIEVE bookkeeping, guarded by the shard lock except Visited (a benign racy flag).
        internal volatile bool Visited;
        internal CachedBlock? Newer;
        internal CachedBlock? Older;

        /// <summary>Adds a reference unless the block was already released by everyone.</summary>
        public bool TryAddRef()
        {
            int current = Volatile.Read(ref _references);
            while (current > 0)
            {
                int seen = Interlocked.CompareExchange(ref _references, current + 1, current);
                if (seen == current)
                {
                    return true;
                }

                current = seen;
            }

            return false;
        }

        /// <summary>Drops one reference; the last one returns the bytes to the pool.</summary>
        public void Release()
        {
            int remaining = Interlocked.Decrement(ref _references);
            if (remaining == 0)
            {
                byte[] buffer = Buffer;
                Buffer = [];
                _owner._pool.Return(buffer);
            }
            else if (remaining < 0)
            {
                throw new InvalidOperationException("A cached SSTable block was released more often than acquired.");
            }
        }
    }

    private sealed class Shard(LsmBlockCache owner, long capacity)
    {
        private readonly object _gate = new();
        private readonly ConcurrentDictionary<BlockKey, CachedBlock> _map = new();

        // FIFO queue: _head is the newest block, _tail the oldest; the hand sweeps from tail toward head.
        private CachedBlock? _head;
        private CachedBlock? _tail;
        private CachedBlock? _hand;
        private long _size;

        public int Count => _map.Count;

        public long SizeBytes => Volatile.Read(ref _size);

        public bool TryAcquire(BlockKey key, out CachedBlock block)
        {
            if (_map.TryGetValue(key, out block!) && block.TryAddRef())
            {
                block.Visited = true;
                return true;
            }

            block = null!;
            return false;
        }

        public CachedBlock InsertAndAcquire(BlockKey key, byte[] buffer, int length)
        {
            List<CachedBlock>? evicted = null;
            CachedBlock result;
            lock (_gate)
            {
                if (_map.TryGetValue(key, out CachedBlock? existing) && existing.TryAddRef())
                {
                    existing.Visited = true;
                    owner._pool.Return(buffer);
                    return existing;
                }

                result = new CachedBlock(owner, key.SourceId, key.Offset, buffer, length);
                result.TryAddRef(); // the caller's reference
                _map[key] = result;
                PushHead(result);
                _size += buffer.Length;
                while (_size > capacity && _tail is not null && _tail != _head)
                {
                    CachedBlock victim = SelectVictim(exclude: result);
                    Unlink(victim);
                    (evicted ??= []).Add(victim);
                }
            }

            ReleaseEvicted(evicted);
            return result;
        }

        public void RemoveSource(long sourceId)
        {
            List<CachedBlock>? removed = null;
            lock (_gate)
            {
                CachedBlock? node = _tail;
                while (node is not null)
                {
                    CachedBlock? next = node.Newer;
                    if (sourceId < 0 || node.SourceId == sourceId)
                    {
                        Unlink(node);
                        (removed ??= []).Add(node);
                    }

                    node = next;
                }
            }

            if (removed is not null)
            {
                foreach (CachedBlock block in removed)
                {
                    block.Release();
                }
            }
        }

        // SIEVE: sweep from the hand (or the tail) toward the head, clearing visited bits and wrapping to the
        // tail. The block just inserted is never chosen; the caller guarantees at least one other block exists.
        private CachedBlock SelectVictim(CachedBlock exclude)
        {
            CachedBlock node = _hand ?? _tail!;
            while (node.Visited || node == exclude)
            {
                node.Visited = false;
                node = node.Newer ?? _tail!;
            }

            _hand = node.Newer;
            return node;
        }

        private void PushHead(CachedBlock block)
        {
            block.Older = _head;
            block.Newer = null;
            if (_head is not null)
            {
                _head.Newer = block;
            }

            _head = block;
            _tail ??= block;
        }

        private void Unlink(CachedBlock block)
        {
            _map.TryRemove(new BlockKey(block.SourceId, block.Offset), out _);
            if (_hand == block)
            {
                _hand = block.Newer;
            }

            if (block.Newer is not null)
            {
                block.Newer.Older = block.Older;
            }
            else
            {
                _head = block.Older;
            }

            if (block.Older is not null)
            {
                block.Older.Newer = block.Newer;
            }
            else
            {
                _tail = block.Newer;
            }

            block.Newer = null;
            block.Older = null;
            _size -= block.Buffer.Length;
        }

        private void ReleaseEvicted(List<CachedBlock>? evicted)
        {
            if (evicted is null)
            {
                return;
            }

            Interlocked.Add(ref owner._evictions, evicted.Count);
            foreach (CachedBlock block in evicted)
            {
                block.Release();
            }
        }
    }
}

/// <summary>
/// A borrowed view of one verified SSTable block: mapped memory, a pooled buffer, or a cached block. Dispose
/// exactly once to give the bytes back; the span is valid until then.
/// </summary>
internal readonly unsafe struct SsTableBlockLease : IDisposable
{
    private readonly IntPtr _address;
    private readonly byte[]? _array;
    private readonly LsmBlockCache.CachedBlock? _cached;
    private readonly LsmBlockCache? _pool;
    private readonly int _length;

    private SsTableBlockLease(IntPtr address, byte[]? array, LsmBlockCache.CachedBlock? cached, LsmBlockCache? pool, int length)
    {
        _address = address;
        _array = array;
        _cached = cached;
        _pool = pool;
        _length = length;
    }

    /// <summary>A lease over mapped memory, which needs no release.</summary>
    public static SsTableBlockLease Mapped(IntPtr address, int length) => new(address, null, null, null, length);

    /// <summary>A lease over an array rented from <see cref="ArrayPool{T}.Shared"/> (or from <paramref name="pool"/>).</summary>
    public static SsTableBlockLease Pooled(byte[] array, int length, LsmBlockCache? pool) => new(IntPtr.Zero, array, null, pool, length);

    /// <summary>A lease holding one reference to a cached block.</summary>
    public static SsTableBlockLease Cached(LsmBlockCache.CachedBlock block) => new(IntPtr.Zero, block.Buffer, block, null, block.Length);

    /// <summary>Whether this lease holds anything.</summary>
    public bool IsEmpty => _address == IntPtr.Zero && _array is null;

    /// <summary>The verified block bytes.</summary>
    public ReadOnlySpan<byte> Span =>
        _address != IntPtr.Zero ? new ReadOnlySpan<byte>((void*)_address, _length) : new ReadOnlySpan<byte>(_array, 0, _length);

    /// <summary>Gives the bytes back.</summary>
    public void Dispose()
    {
        if (_cached is not null)
        {
            _cached.Release();
        }
        else if (_array is not null)
        {
            if (_pool is not null)
            {
                _pool.Return(_array);
            }
            else
            {
                ArrayPool<byte>.Shared.Return(_array);
            }
        }
    }
}
