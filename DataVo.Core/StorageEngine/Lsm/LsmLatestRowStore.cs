using System.Threading;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Tri-state result of a latest-row lookup.</summary>
internal enum LatestRowLookup
{
    /// <summary>The row id has never been inserted.</summary>
    Missing = 0,
    /// <summary>The row id resolves to a live row image.</summary>
    Live = 1,
    /// <summary>The row id was deleted (latest version is a tombstone).</summary>
    Tombstone = 2,
}

/// <summary>
/// Paged latest-row map keyed by the LSM engine's dense, monotonically-assigned RowIds. Replaces a
/// <c>ConcurrentDictionary&lt;long, LatestRowVersion&gt;</c> whose per-entry node allocations dominated
/// bulk-ingest GC (one node + bucket churn per row). A slot holds the row's latest serialized image,
/// the tombstone sentinel, or <see langword="null"/> (never inserted), so publishing a version is a
/// single volatile reference store.
/// <para>Concurrency contract (same as the map it replaces): mutations run under the owning table's
/// SyncRoot — one writer at a time — while point reads are lock-free. Published row buffers are
/// immutable (patches are copy-on-write), so a reader either sees the old complete image or the new
/// one, never a partial write. Page and root arrays are published with volatile stores for the same
/// reason.</para>
/// </summary>
internal sealed class LsmLatestRowStore
{
    private const int PageShift = 16;
    private const int PageSize = 1 << PageShift; // 64K slots (512KB of references) per page.
    private const int PageMask = PageSize - 1;

    /// <summary>Sentinel published for deleted rows; matched by reference, never by content.</summary>
    private static readonly byte[] TombstoneSentinel = new byte[1];

    private byte[]?[][] _pages = new byte[]?[4][];

    /// <summary>Looks up the latest version of <paramref name="rowId"/>. Lock-free.</summary>
    public LatestRowLookup TryGet(long rowId, out byte[] rowBytes)
    {
        rowBytes = [];
        if (rowId < 0)
        {
            return LatestRowLookup.Missing;
        }

        long pageIndex = rowId >> PageShift;
        byte[]?[][] pages = Volatile.Read(ref _pages);
        if (pageIndex >= pages.Length)
        {
            return LatestRowLookup.Missing;
        }

        byte[]?[]? page = Volatile.Read(ref pages[pageIndex]);
        if (page is null)
        {
            return LatestRowLookup.Missing;
        }

        byte[]? slot = Volatile.Read(ref page[rowId & PageMask]);
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

    /// <summary>Publishes <paramref name="rowBytes"/> as the latest live version of <paramref name="rowId"/>. Writer-locked by the caller.</summary>
    public void Set(long rowId, byte[] rowBytes)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rowBytes);
        byte[]?[] page = EnsurePage(rowId);
        Volatile.Write(ref page[rowId & PageMask], rowBytes);
    }

    /// <summary>Publishes a tombstone as the latest version of <paramref name="rowId"/>. Writer-locked by the caller.</summary>
    public void SetTombstone(long rowId)
    {
        byte[]?[] page = EnsurePage(rowId);
        Volatile.Write(ref page[rowId & PageMask], TombstoneSentinel);
    }

    /// <summary>Whether any live (non-tombstoned) row exists. Early-exits on the first hit.</summary>
    public bool HasAnyLive()
    {
        byte[]?[][] pages = Volatile.Read(ref _pages);
        foreach (byte[]?[]? page in pages)
        {
            if (page is null)
            {
                continue;
            }

            foreach (byte[]? slot in page)
            {
                if (slot is not null && !ReferenceEquals(slot, TombstoneSentinel))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Invokes <paramref name="onLiveRow"/> for every live row in ascending row-id order (slots are
    /// laid out by row id, so page order is key order — no sort needed).
    /// </summary>
    public void ForEachLive<TState>(TState state, Action<TState, long, byte[]> onLiveRow)
    {
        byte[]?[][] pages = Volatile.Read(ref _pages);
        for (int pageIndex = 0; pageIndex < pages.Length; pageIndex++)
        {
            byte[]?[]? page = Volatile.Read(ref pages[pageIndex]);
            if (page is null)
            {
                continue;
            }

            long pageBase = (long)pageIndex << PageShift;
            for (int slot = 0; slot < page.Length; slot++)
            {
                byte[]? value = Volatile.Read(ref page[slot]);
                if (value is not null && !ReferenceEquals(value, TombstoneSentinel))
                {
                    onLiveRow(state, pageBase + slot, value);
                }
            }
        }
    }

    private byte[]?[] EnsurePage(long rowId)
    {
        if (rowId < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(rowId), rowId, "Value cannot be negative.");
        }
        long pageIndex = rowId >> PageShift;
        if (pageIndex > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(rowId), rowId, "Row id exceeds the latest-row store's addressable range.");
        }

        byte[]?[][] pages = _pages;
        if (pageIndex >= pages.Length)
        {
            long grown = pages.Length;
            while (grown <= pageIndex)
            {
                grown *= 2;
            }

            var next = new byte[]?[checked((int)grown)][];
            Array.Copy(pages, next, pages.Length);
            Volatile.Write(ref _pages, next);
            pages = next;
        }

        byte[]?[]? page = pages[pageIndex];
        if (page is null)
        {
            page = new byte[]?[PageSize];
            Volatile.Write(ref pages[pageIndex], page);
        }

        return page;
    }
}
