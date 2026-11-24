using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// Receives one key of an index and how many live rows it holds. <paramref name="encodedKey"/> is the key in
/// <see cref="LsmIndexKeyCodec"/> form (without its length prefix) and is valid only during the call. Returns
/// false to stop.
/// </summary>
internal delegate bool IndexKeyCountVisitor<TState>(ref TState state, ReadOnlySpan<byte> encodedKey, long rows);

/// <summary>
/// Walks a persisted index in key order and counts the live rows of each key, without reading a table row: a
/// <c>GROUP BY</c> on an indexed column that only counts rows is answered from the index alone.
/// </summary>
public sealed partial class LsmStorageEngine
{
    /// <summary>
    /// Visits every key of an index with its live-row count, in index key order. The published overlay and the
    /// index runs are merged entry by entry, the newest version of each (key, row) deciding whether it is live, so
    /// the counts are exactly those of <see cref="IndexLookup"/>. Returns false, visiting nothing, when the index
    /// table does not exist or this flow has index changes staged (an open write unit).
    /// </summary>
    bool ILsmIndexStore.TryVisitIndexKeyCounts<TState>(string databaseName, string tableName, string indexName, ref TState state, IndexKeyCountVisitor<TState> visit) =>
        TryVisitIndexKeyCounts(databaseName, tableName, indexName, ref state, visit);

    /// <inheritdoc cref="ILsmIndexStore.TryVisitIndexKeyCounts{TState}"/>
    internal bool TryVisitIndexKeyCounts<TState>(string databaseName, string tableName, string indexName, ref TState state, IndexKeyCountVisitor<TState> visit)
    {
        if (RowKeyIndex(databaseName, tableName, indexName) is not null
            || !_indexTables.TryGetValue((databaseName, tableName, indexName), out IndexTableState? index)
            || _ambientWrite.Value?.StagedIndexFor(index) is { Count: > 0 })
        {
            return false;
        }

        // Overlay before the file set: a flush publishes its file before evicting (see ResolveRow).
        int overlayCount = SnapshotIndexOverlay(index, out OverlayEntry[] overlay);
        TableVersion version = index.AcquireVersion();
        try
        {
            var merge = new IndexEntryMerge(overlay, overlayCount, version.Files, _readMode, _blockCache);
            CountIndexKeys(ref merge, ref state, visit);
        }
        finally
        {
            version.Release();
            System.Buffers.ArrayPool<OverlayEntry>.Shared.Return(overlay, clearArray: true);
        }

        return true;
    }

    /// <summary>One published, unflushed index entry: its key prefix (shared with the overlay), row and newest version.</summary>
    private readonly record struct OverlayEntry(byte[] Prefix, RowKey Row, ulong Sequence, bool Live);

    /// <summary>
    /// Copies the published, unflushed entries of an index into a pooled array, sorted as the runs are (key prefix,
    /// then row key). Returns how many there are.
    /// </summary>
    private static int SnapshotIndexOverlay(IndexTableState index, out OverlayEntry[] entries)
    {
        lock (index.OverlayGate)
        {
            int count = 0;
            foreach (Dictionary<RowKey, (ulong Sequence, bool Live)> rows in index.Overlay.Values)
            {
                count += rows.Count;
            }

            entries = System.Buffers.ArrayPool<OverlayEntry>.Shared.Rent(Math.Max(count, 1));
            int at = 0;
            foreach (KeyValuePair<byte[], Dictionary<RowKey, (ulong Sequence, bool Live)>> key in index.Overlay)
            {
                foreach (KeyValuePair<RowKey, (ulong Sequence, bool Live)> row in key.Value)
                {
                    entries[at++] = new OverlayEntry(key.Key, row.Key, row.Value.Sequence, row.Value.Live);
                }
            }

            Array.Sort(entries, 0, count, OverlayOrder.Instance);
            return count;
        }
    }

    /// <summary>Orders overlay entries as index runs order their user keys.</summary>
    private sealed class OverlayOrder : IComparer<OverlayEntry>
    {
        public static readonly OverlayOrder Instance = new();

        public int Compare(OverlayEntry x, OverlayEntry y) => IndexEntryMerge.Compare(x.Prefix, x.Row, y.Prefix, y.Row);
    }

    /// <summary>Groups the merged live entries by key and hands each key's count to the visitor.</summary>
    private static void CountIndexKeys<TState>(ref IndexEntryMerge merge, ref TState state, IndexKeyCountVisitor<TState> visit)
    {
        byte[] prefix = new byte[64];
        int prefixLength = -1;
        long rows = 0;
        while (merge.MoveNext(out bool live))
        {
            ReadOnlySpan<byte> entryPrefix = merge.Prefix;
            if (prefixLength < 0 || !entryPrefix.SequenceEqual(prefix.AsSpan(0, prefixLength)))
            {
                if (rows > 0 && !visit(ref state, prefix.AsSpan(LsmIndexKeyCodec.LengthPrefixSize, prefixLength - LsmIndexKeyCodec.LengthPrefixSize), rows))
                {
                    return;
                }

                if (prefix.Length < entryPrefix.Length)
                {
                    prefix = new byte[entryPrefix.Length * 2];
                }

                entryPrefix.CopyTo(prefix);
                prefixLength = entryPrefix.Length;
                rows = 0;
            }

            if (live)
            {
                rows++;
            }
        }

        if (rows > 0)
        {
            visit(ref state, prefix.AsSpan(LsmIndexKeyCodec.LengthPrefixSize, prefixLength - LsmIndexKeyCodec.LengthPrefixSize), rows);
        }
    }

    /// <summary>
    /// The entries of an index in key order, one per (key, row): the overlay and every run merged, the version with
    /// the highest sequence deciding. Entries compare by key prefix, then row key, which is how runs order their user
    /// keys (an integer row key is encoded before a wide one, each big-endian).
    /// </summary>
    private struct IndexEntryMerge
    {
        private readonly OverlayEntry[] _overlay;
        private readonly int _overlayCount;
        private readonly SsTableKeyedCursor[] _cursors;
        private readonly bool[] _hasCurrent;
        private int _overlayIndex;
        private byte[] _prefix;
        private int _prefixLength;
        private RowKey _row;

        public IndexEntryMerge(OverlayEntry[] overlay, int overlayCount, LsmSstableHandle[] files, LsmReadMode readMode, LsmBlockCache? blockCache)
        {
            _overlay = overlay;
            _overlayCount = overlayCount;
            _cursors = new SsTableKeyedCursor[files.Length];
            _hasCurrent = new bool[files.Length];
            for (int i = 0; i < files.Length; i++)
            {
                _cursors[i] = files[i].GetKeyedReader(readMode, blockCache).OpenCursor();
                _hasCurrent[i] = _cursors[i].MoveNext();
            }

            _overlayIndex = 0;
            _prefix = new byte[64];
            _prefixLength = 0;
            _row = default;
        }

        /// <summary>The key prefix (length prefix and key) of the current entry.</summary>
        public readonly ReadOnlySpan<byte> Prefix => _prefix.AsSpan(0, _prefixLength);

        public static int Compare(ReadOnlySpan<byte> prefix, RowKey row, ReadOnlySpan<byte> otherPrefix, RowKey otherRow)
        {
            int order = prefix.SequenceCompareTo(otherPrefix);
            return order != 0 ? order : row.CompareTo(otherRow);
        }

        /// <summary>Moves to the next (key, row) entry; <paramref name="live"/> says whether its newest version is a put.</summary>
        public bool MoveNext(out bool live)
        {
            live = false;
            bool any = false;
            ReadOnlySpan<byte> prefix = default;
            RowKey row = default;
            if (_overlayIndex < _overlayCount)
            {
                prefix = _overlay[_overlayIndex].Prefix;
                row = _overlay[_overlayIndex].Row;
                any = true;
            }

            for (int i = 0; i < _cursors.Length; i++)
            {
                if (!_hasCurrent[i])
                {
                    continue;
                }

                ReadOnlySpan<byte> userKey = _cursors[i].UserKey;
                ReadOnlySpan<byte> cursorPrefix = LsmIndexKeyCodec.PrefixOf(userKey);
                RowKey cursorRow = LsmIndexKeyCodec.RowKeyOf(userKey);
                if (!any || Compare(cursorPrefix, cursorRow, prefix, row) < 0)
                {
                    prefix = cursorPrefix;
                    row = cursorRow;
                    any = true;
                }
            }

            if (!any)
            {
                return false;
            }

            if (_prefix.Length < prefix.Length)
            {
                _prefix = new byte[prefix.Length * 2];
            }

            prefix.CopyTo(_prefix);
            _prefixLength = prefix.Length;
            _row = row;
            live = TakeNewest();
            return true;
        }

        /// <summary>Steps every source past the current entry and returns whether its newest version is live.</summary>
        private bool TakeNewest()
        {
            ReadOnlySpan<byte> prefix = Prefix;
            ulong newest = 0;
            bool live = false;
            bool any = false;
            if (_overlayIndex < _overlayCount && _overlay[_overlayIndex].Row == _row && _overlay[_overlayIndex].Prefix.AsSpan().SequenceEqual(prefix))
            {
                (_, _, newest, live) = _overlay[_overlayIndex++];
                any = true;
            }

            for (int i = 0; i < _cursors.Length; i++)
            {
                // A run lists the versions of an entry newest first; the first one is the run's say.
                if (_hasCurrent[i] && IsCurrent(_cursors[i].UserKey, prefix))
                {
                    if (!any || _cursors[i].Sequence > newest)
                    {
                        newest = _cursors[i].Sequence;
                        live = _cursors[i].Type == LsmValueType.Put;
                        any = true;
                    }

                    do
                    {
                        _hasCurrent[i] = _cursors[i].MoveNext();
                    }
                    while (_hasCurrent[i] && IsCurrent(_cursors[i].UserKey, prefix));
                }
            }

            return live;
        }

        private readonly bool IsCurrent(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> prefix) =>
            LsmIndexKeyCodec.PrefixOf(userKey).SequenceEqual(prefix) && LsmIndexKeyCodec.RowKeyOf(userKey) == _row;
    }
}
