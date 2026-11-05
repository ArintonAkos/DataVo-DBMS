using System.Threading;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Tri-state result of an overlay lookup.</summary>
internal enum LatestRowLookup
{
    /// <summary>The overlay has no version of this row id: consult the SSTables.</summary>
    Missing = 0,
    /// <summary>The row id resolves to a live row image.</summary>
    Live = 1,
    /// <summary>The row id was deleted (latest version is a tombstone).</summary>
    Tombstone = 2,
}

/// <summary>
/// Lock-free overlay of row versions that are published but not yet flushed to an SSTable, keyed by the
/// engine's dense row ids. Once a MemTable is flushed, its versions are evicted here and served from the
/// memory-mapped SSTables instead, so the overlay holds recent writes only and the GC heap no longer
/// grows with the table.
/// </summary>
/// <remarks>
/// <para>Concurrency: mutations (publish and eviction) are serialized by the caller; point reads are
/// lock-free. Published row buffers are immutable (patches are copy-on-write), so a reader sees either
/// the old complete image or the new one. Every slot records the sequence of its version so a flush
/// evicts only versions it wrote, never a newer one published meanwhile.</para>
/// <para>Ordering with flushes: a flush publishes its SSTable (manifest edit) before evicting, and readers
/// check the overlay before taking the current file set; a reader that misses an evicted slot therefore
/// always sees the new file.</para>
/// </remarks>
internal sealed class LsmLatestRowStore
{
    private const int PageShift = 12;
    private const int PageSize = 1 << PageShift; // 4K slots: small enough that sparse recent writes stay cheap
    private const int PageMask = PageSize - 1;

    /// <summary>Sentinel published for deleted rows; matched by reference, never by content.</summary>
    private static readonly byte[] TombstoneSentinel = new byte[1];

    private Page?[] _pages = new Page?[16];
    private long _count;

    /// <summary>Row versions currently held (live rows and tombstones).</summary>
    public long Count => Volatile.Read(ref _count);

    /// <summary>Looks up the overlay version of <paramref name="rowId"/>. Lock-free.</summary>
    public LatestRowLookup TryGet(long rowId, out byte[] rowBytes)
    {
        rowBytes = [];
        if (rowId < 0)
        {
            return LatestRowLookup.Missing;
        }

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

        byte[]? slot = Volatile.Read(ref page.Slots[rowId & PageMask]);
        if (slot is null)
        {
            return LatestRowLookup.Missing;
        }

        if (ReferenceEquals(slot, TombstoneSentinel))
        {
            return LatestRowLookup.Tombstone;
        }

        rowBytes = slot;
        return LatestRowLookup.Live;
    }

    /// <summary>Publishes a live version. Serialized by the caller.</summary>
    public void Set(long rowId, byte[] rowBytes, ulong sequence)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowBytes);
        Publish(rowId, rowBytes, sequence);
    }

    /// <summary>Publishes a deletion. Serialized by the caller.</summary>
    public void SetTombstone(long rowId, ulong sequence) => Publish(rowId, TombstoneSentinel, sequence);

    /// <summary>
    /// Drops the version of <paramref name="rowId"/> when it is at or below <paramref name="flushedThrough"/>,
    /// i.e. it is now in an SSTable. A newer version (published after the flushed MemTable) stays.
    /// Serialized by the caller with publishes.
    /// </summary>
    public void EvictFlushed(long rowId, ulong flushedThrough)
    {
        if (rowId < 0)
        {
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
    /// Invokes <paramref name="visit"/> for every version in ascending row-id order, with
    /// <see langword="null"/> for a deletion.
    /// </summary>
    public void ForEach<TState>(TState state, Action<TState, long, byte[]?> visit)
    {
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
                    visit(state, pageBase + slot, ReferenceEquals(value, TombstoneSentinel) ? null : value);
                }
            }
        }
    }

    private void Publish(long rowId, byte[] value, ulong sequence)
    {
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

    private Page EnsurePage(long rowId)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNegative(rowId);
        long pageIndex = rowId >> PageShift;
        if (pageIndex > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(rowId), rowId, "Row id exceeds the overlay's addressable range.");
        }

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
}
