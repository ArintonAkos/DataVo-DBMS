using System.Buffers.Binary;
using System.Threading;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// A single-writer, multi-reader sorted skiplist keyed by <see cref="InternalKey"/>, backing one LSM
/// MemTable generation. Every node — header, forward pointers, internal-key bytes, and value bytes — is one
/// contiguous record carved from an <see cref="Arena"/>; forward pointers are stable arena handles, so the
/// structure is pointer-linked with no managed object per insert (zero per-<see cref="Put"/> GC).
/// <para>
/// Node record layout (little-endian): <c>[int valueLen][int keyLen][byte height][7 byte padding][long forward[height]]
/// [internalKey bytes][value bytes]</c>. A null forward link is <see cref="Null"/> (-1). The head is a
/// keyless sentinel of maximum height.
/// </para>
/// <para>Single-writer: only one thread calls <see cref="Put"/>/<see cref="Delete"/>. Forward links are
/// 8-byte aligned and published with release writes; readers follow them with acquire reads so live readers
/// cannot observe a node before its key/value bytes and successor links are initialized on weak memory models.</para>
/// </summary>
public sealed class MemTable : IDisposable
{
    private const int MaxHeight = 12;
    private const long Null = -1L;

    // Header field offsets within a node record.
    private const int ValueLenOffset = 0;
    private const int KeyLenOffset = 4;
    private const int HeightOffset = 8;
    private const int ForwardOffset = 16; // forward[0] starts here; each link is an 8-byte-aligned long.

    private readonly Arena _arena;
    private readonly long _head;
    private int _maxHeight = 1;
    private int _count;
    private ulong _rngState = 0x9E3779B97F4A7C15UL; // fixed seed → deterministic structure for tests.
    private bool _frozen;
    private bool _disposed;

    /// <summary>Creates an empty MemTable whose arena uses <paramref name="slabSize"/>-byte slabs.</summary>
    public MemTable(int slabSize = 1 << 20)
    {
        _arena = new Arena(slabSize);
        _head = AllocateHead();
    }

    /// <summary>Number of entries (versions) inserted, including tombstones.</summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>Approximate resident size in bytes (the arena's handed-out total).</summary>
    public long ApproximateBytes => _arena.BytesAllocated;

    /// <summary>Whether the MemTable has been frozen (no further writes accepted).</summary>
    public bool IsFrozen => _frozen;

    internal static int ForwardOffsetForTesting => ForwardOffset;

    /// <summary>
    /// Inserts a versioned entry. Each call inserts a new node; multiple versions of a user key coexist,
    /// newest (highest seqno) first. A tombstone is a <see cref="LsmValueType.Deletion"/> entry.
    /// </summary>
    public void Put(ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType, ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_frozen)
        {
            throw new InvalidOperationException("Cannot Put into a frozen MemTable.");
        }

        int height = RandomHeight();
        long node = AllocateNode(userKey, seqno, valueType, value, height);
        ReadOnlySpan<byte> internalKey = GetKey(node);

        Span<long> update = stackalloc long[MaxHeight];
        long x = _head;
        int currentMaxHeight = Volatile.Read(ref _maxHeight);
        for (int level = currentMaxHeight - 1; level >= 0; level--)
        {
            long next = GetForward(x, level);
            while (next != Null && InternalKey.Compare(GetKey(next), internalKey) < 0)
            {
                x = next;
                next = GetForward(x, level);
            }

            update[level] = x;
        }

        if (height > _maxHeight)
        {
            for (int level = currentMaxHeight; level < height; level++)
            {
                update[level] = _head;
            }

            Volatile.Write(ref _maxHeight, height);
        }

        for (int level = 0; level < height; level++)
        {
            SetForward(node, level, GetForward(update[level], level));
            SetForward(update[level], level, node);
        }

        Volatile.Write(ref _count, _count + 1);
    }

    /// <summary>Marks the MemTable immutable. Subsequent writes throw; reads and iteration remain valid.</summary>
    public void Freeze()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _frozen = true;
    }

    /// <summary>Inserts a tombstone for <paramref name="userKey"/> at <paramref name="seqno"/>.</summary>
    public void Delete(ReadOnlySpan<byte> userKey, ulong seqno)
    {
        Put(userKey, seqno, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty);
    }

    /// <summary>
    /// Returns the newest version of <paramref name="userKey"/> with sequence number ≤
    /// <paramref name="snapshotSeqno"/>. A tombstone sets <paramref name="isTombstone"/> and returns
    /// <see langword="false"/> (the key is logically absent at that snapshot).
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> userKey, ulong snapshotSeqno, out ReadOnlySpan<byte> value, out bool isTombstone)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        value = default;
        isTombstone = false;

        // Lookup key sorts at-or-before any real entry at seqno == snapshot: tag = (snapshot << 8) | 0xFF
        // is >= any real tag (type 0/1) at that seqno, so under descending-tag order it precedes them.
        int ikLen = InternalKey.MeasureSize(userKey.Length);
        Span<byte> lookup = ikLen <= 256 ? stackalloc byte[256] : new byte[ikLen];
        lookup = lookup[..ikLen];
        userKey.CopyTo(lookup);
        ulong tag = (snapshotSeqno << 8) | 0xFF;
        BinaryPrimitives.WriteUInt64BigEndian(lookup.Slice(userKey.Length, InternalKey.TagSize), tag);

        long node = FindGreaterOrEqual(lookup);
        if (node == Null)
        {
            return false;
        }

        ReadOnlySpan<byte> nodeKey = GetKey(node);
        if (!InternalKey.UserKey(nodeKey).SequenceEqual(userKey))
        {
            return false;
        }

        if (InternalKey.ValueType(nodeKey) == LsmValueType.Deletion)
        {
            isTombstone = true;
            return false;
        }

        value = GetValue(node);
        return true;
    }

    private long FindGreaterOrEqual(ReadOnlySpan<byte> internalKey)
    {
        long x = _head;
        int currentMaxHeight = Volatile.Read(ref _maxHeight);
        for (int level = currentMaxHeight - 1; level >= 0; level--)
        {
            long next = GetForward(x, level);
            while (next != Null && InternalKey.Compare(GetKey(next), internalKey) < 0)
            {
                x = next;
                next = GetForward(x, level);
            }
        }

        return GetForward(x, 0);
    }

    private long AllocateHead()
    {
        int total = ForwardOffset + (8 * MaxHeight); // 8 bytes per forward link (a long handle).
        Span<byte> rec = _arena.Allocate(total, out long handle);
        BinaryPrimitives.WriteInt32LittleEndian(rec[ValueLenOffset..], 0);
        BinaryPrimitives.WriteInt32LittleEndian(rec[KeyLenOffset..], 0);
        rec[HeightOffset] = MaxHeight;
        for (int level = 0; level < MaxHeight; level++)
        {
            SetForward(handle, level, Null);
        }

        return handle;
    }

    private long AllocateNode(ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType, ReadOnlySpan<byte> value, int height)
    {
        int keyLen = InternalKey.MeasureSize(userKey.Length);
        int keyStart = ForwardOffset + (8 * height);
        int total = keyStart + keyLen + value.Length;

        Span<byte> rec = _arena.Allocate(total, out long handle);
        BinaryPrimitives.WriteInt32LittleEndian(rec[ValueLenOffset..], value.Length);
        BinaryPrimitives.WriteInt32LittleEndian(rec[KeyLenOffset..], keyLen);
        rec[HeightOffset] = (byte)height;
        for (int level = 0; level < height; level++)
        {
            SetForward(handle, level, Null);
        }

        InternalKey.Write(rec.Slice(keyStart, keyLen), userKey, seqno, valueType);
        value.CopyTo(rec.Slice(keyStart + keyLen, value.Length));
        return handle;
    }

    private long GetForward(long node, int level)
    {
        ref long link = ref _arena.ResolveInt64Reference(node, ForwardOffset + (8 * level));
        return Volatile.Read(ref link);
    }

    private void SetForward(long node, int level, long next)
    {
        ref long link = ref _arena.ResolveInt64Reference(node, ForwardOffset + (8 * level));
        Volatile.Write(ref link, next);
    }

    private ReadOnlySpan<byte> GetKey(long node)
    {
        Span<byte> hdr = _arena.Resolve(node, ForwardOffset);
        int valueLen = BinaryPrimitives.ReadInt32LittleEndian(hdr[ValueLenOffset..]);
        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(hdr[KeyLenOffset..]);
        int height = hdr[HeightOffset];
        int keyStart = ForwardOffset + (8 * height);
        Span<byte> full = _arena.Resolve(node, keyStart + keyLen + valueLen);
        return full.Slice(keyStart, keyLen);
    }

    private ReadOnlySpan<byte> GetValue(long node)
    {
        Span<byte> hdr = _arena.Resolve(node, ForwardOffset);
        int valueLen = BinaryPrimitives.ReadInt32LittleEndian(hdr[ValueLenOffset..]);
        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(hdr[KeyLenOffset..]);
        int height = hdr[HeightOffset];
        int keyStart = ForwardOffset + (8 * height);
        Span<byte> full = _arena.Resolve(node, keyStart + keyLen + valueLen);
        return full.Slice(keyStart + keyLen, valueLen);
    }

    private int RandomHeight()
    {
        int height = 1;
        while (height < MaxHeight && (NextRandom() & 3) == 0) // p = 1/4
        {
            height++;
        }

        return height;
    }

    private ulong NextRandom()
    {
        ulong x = _rngState;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        _rngState = x;
        return x;
    }

    /// <summary>Returns a forward iterator over all entries in <see cref="InternalKey"/> order.</summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>Forward iterator over the skiplist's level-0 chain (fully sorted).</summary>
    public ref struct Enumerator
    {
        private readonly MemTable _table;
        private long _node;

        internal Enumerator(MemTable table)
        {
            _table = table;
            _node = table._head;
        }

        /// <summary>The current entry. Valid only after a successful <see cref="MoveNext"/>.</summary>
        public readonly MemTableEntry Current => new(_table.GetKey(_node), _table.GetValue(_node));

        /// <summary>Advances to the next entry; returns <see langword="false"/> at the end.</summary>
        public bool MoveNext()
        {
            _node = _table.GetForward(_node, 0);
            return _node != Null;
        }
    }

    /// <summary>Releases the backing arena. The MemTable must not be used afterwards.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _arena.Dispose();
    }
}

/// <summary>One entry yielded by the MemTable enumerator: its full internal key and value bytes.</summary>
public readonly ref struct MemTableEntry
{
    /// <summary>The entry's internal key (<c>userKey ‖ tag</c>).</summary>
    public readonly ReadOnlySpan<byte> InternalKey;

    /// <summary>The entry's value bytes (empty for a tombstone).</summary>
    public readonly ReadOnlySpan<byte> Value;

    internal MemTableEntry(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        InternalKey = internalKey;
        Value = value;
    }
}
