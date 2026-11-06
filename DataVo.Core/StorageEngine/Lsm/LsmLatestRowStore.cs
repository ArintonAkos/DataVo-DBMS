using System.Threading;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Tri-state result of an overlay lookup.</summary>
internal enum LatestRowLookup
{
    /// <summary>The overlay has no version of this row key: consult the SSTables.</summary>
    Missing = 0,
    /// <summary>The row key resolves to a live row image.</summary>
    Live = 1,
    /// <summary>The row was deleted (latest version is a tombstone).</summary>
    Tombstone = 2,
}

/// <summary>
/// Lock-free overlay of row versions that are published but not yet flushed to an SSTable, keyed by
/// <see cref="RowKey"/>. Once a MemTable is flushed, its versions are evicted here and served from the
/// memory-mapped SSTables instead, so the overlay holds recent writes only and the GC heap no longer
/// grows with the table.
/// </summary>
/// <remarks>
/// <para>Two places hold versions, chosen by key alone, so a key always lives in the same one. Engine
/// row ids (small non-negative integers) go to a dense array paged by the integer: a read is two array
/// loads and a publish allocates nothing. Every other key (a GUID, a negative or very large integer
/// primary key) goes to an open-addressing hash map of immutable entries, whose reads are lock-free too.</para>
/// <para>Concurrency: mutations (publish and eviction) are serialized by the caller; point reads are
/// lock-free. Published row buffers are immutable (patches are copy-on-write), so a reader sees either
/// the old complete image or the new one. Every version records its sequence so a flush evicts only
/// versions it wrote, never a newer one published meanwhile.</para>
/// <para>Ordering with flushes: a flush publishes its SSTable (manifest edit) before evicting, and readers
/// check the overlay before taking the current file set; a reader that misses an evicted slot therefore
/// always sees the new file.</para>
/// </remarks>
internal sealed class LsmLatestRowStore
{
    private const int PageShift = 12;
    private const int PageSize = 1 << PageShift; // 4K slots: small enough that sparse recent writes stay cheap
    private const int PageMask = PageSize - 1;

    /// <summary>
    /// Integers below this use the dense pages (65,536 pages, a page table of at most 512 KB); larger
    /// ones would make the page table itself huge, so they go to the sparse map.
    /// </summary>
    internal const long DenseLimit = 1L << (PageShift + 16);

    /// <summary>Sentinel published for deleted rows; matched by reference, never by content.</summary>
    private static readonly byte[] TombstoneSentinel = new byte[1];

    private Page?[] _pages = new Page?[16];
    private readonly SparseRowMap _sparse = new();
    private long _count;

    /// <summary>Row versions currently held (live rows and tombstones).</summary>
    public long Count => Volatile.Read(ref _count);

    /// <summary>Looks up the overlay version of <paramref name="key"/>. Lock-free.</summary>
    public LatestRowLookup TryGet(RowKey key, out byte[] rowBytes)
    {
        rowBytes = [];
        byte[]? value;
        if (TryGetDenseIndex(key, out long rowId))
        {
            long pageIndex = rowId >> PageShift;
            Page?[] pages = Volatile.Read(ref _pages);
            if (pageIndex >= pages.Length)
            {
                return LatestRowLookup.Missing;
            }

            Page? page = Volatile.Read(ref pages[pageIndex]);
            if (page is null)
            {
                return LatestRowLookup.Missing;
            }

            value = Volatile.Read(ref page.Slots[rowId & PageMask]);
        }
        else
        {
            value = _sparse.Find(key)?.Value;
        }

        if (value is null)
        {
            return LatestRowLookup.Missing;
        }

        if (ReferenceEquals(value, TombstoneSentinel))
        {
            return LatestRowLookup.Tombstone;
        }

        rowBytes = value;
        return LatestRowLookup.Live;
    }

    /// <summary>Publishes a live version. Serialized by the caller.</summary>
    public void Set(RowKey key, byte[] rowBytes, ulong sequence)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowBytes);
        Publish(key, rowBytes, sequence);
    }

    /// <summary>Publishes a deletion. Serialized by the caller.</summary>
    public void SetTombstone(RowKey key, ulong sequence) => Publish(key, TombstoneSentinel, sequence);

    /// <summary>
    /// Drops the version of <paramref name="key"/> when it is at or below <paramref name="flushedThrough"/>,
    /// i.e. it is now in an SSTable. A newer version (published after the flushed MemTable) stays.
    /// Serialized by the caller with publishes.
    /// </summary>
    public void EvictFlushed(RowKey key, ulong flushedThrough)
    {
        if (!TryGetDenseIndex(key, out long rowId))
        {
            if (_sparse.Find(key) is { } version
                && version.Sequence <= flushedThrough
                && _sparse.Remove(key))
            {
                Interlocked.Decrement(ref _count);
            }

            return;
        }

        long pageIndex = rowId >> PageShift;
        Page?[] pages = _pages;
        if (pageIndex >= pages.Length || pages[pageIndex] is not { } page)
        {
            return;
        }

        int slot = (int)(rowId & PageMask);
        if (page.Slots[slot] is null || page.Sequences[slot] > flushedThrough)
        {
            return;
        }

        Volatile.Write(ref page.Slots[slot], null);
        Interlocked.Decrement(ref _count);
        if (--page.Occupied == 0)
        {
            // Readers still holding this page see only empty slots and fall through to the SSTables.
            Volatile.Write(ref pages[pageIndex], null);
        }
    }

    /// <summary>
    /// Invokes <paramref name="visit"/> for every version in ascending key order, with its sequence and
    /// <see langword="null"/> for a deletion. A dense value is read before its sequence, so a concurrent
    /// publish can only make a sequence look newer than its value, never older; a sparse version is one
    /// immutable object.
    /// </summary>
    public void ForEachVersion<TState>(TState state, Action<TState, RowKey, ulong, byte[]?> visit)
    {
        // Sparse keys sort around the dense ones: negative integers before, large integers and GUIDs after.
        List<KeyValuePair<RowKey, SparseVersion>>? sparse = null;
        if (_sparse.Count > 0)
        {
            sparse = _sparse.Snapshot();
            sparse.Sort(static (x, y) => x.Key.CompareTo(y.Key));
        }

        int next = 0;
        Page?[] pages = Volatile.Read(ref _pages);
        for (int pageIndex = 0; pageIndex < pages.Length; pageIndex++)
        {
            Page? page = Volatile.Read(ref pages[pageIndex]);
            if (page is null)
            {
                continue;
            }

            long pageBase = (long)pageIndex << PageShift;
            for (int slot = 0; slot < PageSize; slot++)
            {
                byte[]? value = Volatile.Read(ref page.Slots[slot]);
                if (value is not null)
                {
                    ulong sequence = Volatile.Read(ref page.Sequences[slot]);
                    RowKey key = pageBase + slot;
                    while (sparse is not null && next < sparse.Count && sparse[next].Key < key)
                    {
                        VisitSparse(state, visit, sparse[next++]);
                    }

                    visit(state, key, sequence, ReferenceEquals(value, TombstoneSentinel) ? null : value);
                }
            }
        }

        while (sparse is not null && next < sparse.Count)
        {
            VisitSparse(state, visit, sparse[next++]);
        }
    }

    private static void VisitSparse<TState>(TState state, Action<TState, RowKey, ulong, byte[]?> visit, KeyValuePair<RowKey, SparseVersion> entry) =>
        visit(state, entry.Key, entry.Value.Sequence, ReferenceEquals(entry.Value.Value, TombstoneSentinel) ? null : entry.Value.Value);

    private void Publish(RowKey key, byte[] value, ulong sequence)
    {
        if (!TryGetDenseIndex(key, out long rowId))
        {
            // Publishes are serialized, so the check and the write cannot race with another publish.
            if (_sparse.Set(new SparseVersion(key, value, sequence)))
            {
                Interlocked.Increment(ref _count);
            }

            return;
        }

        Page page = EnsurePage(rowId);
        int slot = (int)(rowId & PageMask);
        if (page.Slots[slot] is null)
        {
            page.Occupied++;
            Interlocked.Increment(ref _count);
        }

        // Sequence first: an evictor comparing sequences never pairs a new value with an old sequence.
        Volatile.Write(ref page.Sequences[slot], sequence);
        Volatile.Write(ref page.Slots[slot], value);
    }

    /// <summary>Whether <paramref name="key"/> lives in the dense pages, and at which integer.</summary>
    private static bool TryGetDenseIndex(RowKey key, out long rowId)
    {
        rowId = unchecked((long)(key.Low ^ 0x8000_0000_0000_0000UL));
        return key.High == 0 && (ulong)rowId < DenseLimit;
    }

    private Page EnsurePage(long rowId)
    {
        long pageIndex = rowId >> PageShift;
        Page?[] pages = _pages;
        if (pageIndex >= pages.Length)
        {
            long grown = pages.Length;
            while (grown <= pageIndex)
            {
                grown *= 2;
            }

            var next = new Page?[checked((int)grown)];
            Array.Copy(pages, next, pages.Length);
            Volatile.Write(ref _pages, next);
            pages = next;
        }

        Page? page = pages[pageIndex];
        if (page is null)
        {
            page = new Page();
            Volatile.Write(ref pages[pageIndex], page);
        }

        return page;
    }

    private sealed class Page
    {
        public readonly byte[]?[] Slots = new byte[]?[PageSize];
        public readonly ulong[] Sequences = new ulong[PageSize];
        public int Occupied;
    }

    /// <summary>An immutable (key, value, sequence) entry: a reader never sees one part without the others.</summary>
    private sealed class SparseVersion(RowKey key, byte[] value, ulong sequence)
    {
        public RowKey Key { get; } = key;

        public byte[] Value { get; } = value;

        public ulong Sequence { get; } = sequence;
    }

    /// <summary>
    /// Open-addressing hash map from row keys to <see cref="SparseVersion"/>s, for one serialized writer and
    /// lock-free readers. A slot holds an immutable entry, written with one reference store; a removal
    /// leaves a marker so probes continue past it. Growing builds a new table and publishes it with one
    /// reference store, dropping the markers.
    /// </summary>
    /// <remarks>
    /// A reader holding the previous table may miss a version published while it reads, the same overlap
    /// any read racing a publish has; an evicted version it still sees is the one now in an SSTable.
    /// </remarks>
    private sealed class SparseRowMap
    {
        private static readonly SparseVersion Removed = new(default, [], 0);

        private SparseVersion?[] _slots = new SparseVersion?[64];
        private int _count;
        private int _used; // live entries plus removal markers

        public int Count => Volatile.Read(ref _count);

        public SparseVersion? Find(RowKey key)
        {
            SparseVersion?[] slots = Volatile.Read(ref _slots);
            int mask = slots.Length - 1;
            for (int i = Hash(key) & mask; ; i = (i + 1) & mask)
            {
                SparseVersion? entry = Volatile.Read(ref slots[i]);
                if (entry is null)
                {
                    return null;
                }

                if (!ReferenceEquals(entry, Removed) && entry.Key == key)
                {
                    return entry;
                }
            }
        }

        /// <summary>Stores <paramref name="version"/> under its key; true when the key was new.</summary>
        public bool Set(SparseVersion version)
        {
            if ((_used + 1) * 2 > _slots.Length)
            {
                Grow();
            }

            SparseVersion?[] slots = _slots;
            int mask = slots.Length - 1;
            int firstRemoved = -1;
            for (int i = Hash(version.Key) & mask; ; i = (i + 1) & mask)
            {
                SparseVersion? entry = slots[i];
                if (entry is null)
                {
                    // New key: reuse a removal marker passed on the way, else the empty slot.
                    int target = firstRemoved >= 0 ? firstRemoved : i;
                    if (firstRemoved < 0)
                    {
                        _used++;
                    }

                    Volatile.Write(ref slots[target], version);
                    Volatile.Write(ref _count, _count + 1);
                    return true;
                }

                if (ReferenceEquals(entry, Removed))
                {
                    if (firstRemoved < 0)
                    {
                        firstRemoved = i;
                    }
                }
                else if (entry.Key == version.Key)
                {
                    Volatile.Write(ref slots[i], version);
                    return false;
                }
            }
        }

        public bool Remove(RowKey key)
        {
            SparseVersion?[] slots = _slots;
            int mask = slots.Length - 1;
            for (int i = Hash(key) & mask; ; i = (i + 1) & mask)
            {
                SparseVersion? entry = slots[i];
                if (entry is null)
                {
                    return false;
                }

                if (!ReferenceEquals(entry, Removed) && entry.Key == key)
                {
                    Volatile.Write(ref slots[i], Removed);
                    Volatile.Write(ref _count, _count - 1);
                    return true;
                }
            }
        }

        public List<KeyValuePair<RowKey, SparseVersion>> Snapshot()
        {
            SparseVersion?[] slots = Volatile.Read(ref _slots);
            var entries = new List<KeyValuePair<RowKey, SparseVersion>>(Count);
            foreach (SparseVersion? entry in slots)
            {
                if (entry is not null && !ReferenceEquals(entry, Removed))
                {
                    entries.Add(new KeyValuePair<RowKey, SparseVersion>(entry.Key, entry));
                }
            }

            return entries;
        }

        private void Grow()
        {
            int capacity = 64;
            while (capacity < (_count + 1) * 4)
            {
                capacity *= 2;
            }

            var grown = new SparseVersion?[capacity];
            int mask = capacity - 1;
            foreach (SparseVersion? entry in _slots)
            {
                if (entry is null || ReferenceEquals(entry, Removed))
                {
                    continue;
                }

                int i = Hash(entry.Key) & mask;
                while (grown[i] is not null)
                {
                    i = (i + 1) & mask;
                }

                grown[i] = entry;
            }

            _used = _count;
            Volatile.Write(ref _slots, grown);
        }

        // Spreads both halves (GUID keys differ in either; integer keys only in the low half).
        private static int Hash(RowKey key)
        {
            ulong mixed = (key.High ^ (key.Low * 0x9E37_79B9_7F4A_7C15UL)) * 0xBF58_476D_1CE4_E5B9UL;
            return (int)(mixed >> 33);
        }
    }
}
