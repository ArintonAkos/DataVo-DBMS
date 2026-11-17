using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// Streams the rows a transaction's snapshot sees, so a typed read inside an open transaction decodes only the
/// columns it needs instead of materializing whole rows through <see cref="GetTableContents(List{RowKey}?, string, string, HashSet{string}?)"/>.
/// </summary>
public partial class StorageContext
{
    /// <summary>
    /// Streams the rows visible to the current MVCC snapshot, with the same effect as the general path's
    /// visibility filter: committed rows the snapshot sees, retained images of rows replaced since it began, and the
    /// transaction's own updates, deletes and inserts. Committed rows arrive in stored form; rows the overlay
    /// supplies arrive serialized exactly as a commit would store them (a clustered key cell is present there).
    /// Without a snapshot this is <see cref="ScanTableRows{TState}"/>.
    /// </summary>
    /// <remarks>
    /// <paramref name="lower"/> and <paramref name="upper"/> bound only the committed rows walked; overlay rows are
    /// always offered, so the visitor must test the predicates the range was derived from.
    /// </remarks>
    /// <returns>False when the scan cannot run here (staged LSM writes), so the caller takes the general path.</returns>
    internal bool ScanVisibleRows<TState>(string tableName, string databaseName, RowKey? lower, RowKey? upper, ref TState state, RawRowVisitor<TState> visit)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot is null)
        {
            return ScanTableRows(tableName, databaseName, lower, upper, ref state, visit);
        }

        if (LsmEngine is { } lsm && lsm.HasStagedWrites(databaseName, tableName))
        {
            return false;
        }

        DataVoEngine engine = DataVoEngine.Current();
        var scan = new SnapshotScan<TState>(state, visit, engine, snapshot, tableName, databaseName, BuildSnapshotOverlay(engine, tableName, databaseName, snapshot));
        ScanCommittedRows(tableName, databaseName, lower, upper, ref scan, VisitCommittedRow);
        if (!scan.Stopped)
        {
            VisitUnseenOverlayRows(ref scan);
        }

        state = scan.State;
        return true;
    }

    /// <summary>What the snapshot overlay says about one committed row key.</summary>
    private sealed class OverlayEntry
    {
        /// <summary>The retained image the snapshot reads in place of the stored row, serialized; null when none.</summary>
        public byte[]? Image;

        /// <summary>The transaction's own latest update of the row, serialized; null when none.</summary>
        public byte[]? Update;

        /// <summary>Whether the transaction deleted the row.</summary>
        public bool Deleted;

        /// <summary>Whether the committed walk reached the key.</summary>
        public bool Seen;
    }

    /// <summary>The overlay of a snapshot scan: per-key entries plus the transaction's buffered inserts, in order.</summary>
    private sealed class SnapshotOverlay(Dictionary<RowKey, OverlayEntry> entries, List<byte[]> inserts)
    {
        public Dictionary<RowKey, OverlayEntry> Entries { get; } = entries;

        public List<byte[]> Inserts { get; } = inserts;
    }

    private struct SnapshotScan<TState>(
        TState state,
        RawRowVisitor<TState> visit,
        DataVoEngine engine,
        TransactionSnapshot snapshot,
        string tableName,
        string databaseName,
        SnapshotOverlay overlay)
    {
        public TState State = state;
        public readonly RawRowVisitor<TState> Visit = visit;
        public readonly DataVoEngine Engine = engine;
        public readonly TransactionSnapshot Snapshot = snapshot;
        public readonly string TableName = tableName;
        public readonly string DatabaseName = databaseName;
        public readonly SnapshotOverlay Overlay = overlay;
        public bool Stopped;
    }

    /// <summary>
    /// Collects the overlay in the order the general path applies it: retained images, then the transaction's
    /// updates (latest wins), deletes and inserts.
    /// </summary>
    private SnapshotOverlay BuildSnapshotOverlay(DataVoEngine engine, string tableName, string databaseName, TransactionSnapshot snapshot)
    {
        var entries = new Dictionary<RowKey, OverlayEntry>();
        var inserts = new List<byte[]>();
        foreach (RetainedRowImage image in engine.VersionStorageManager.GetVisibleImages(databaseName, tableName, snapshot))
        {
            EntryOf(entries, image.RowId).Image = SerializeOverlayRow(image.Row, tableName, databaseName);
        }

        if (MvccExecutionScope.CurrentTransaction is not { } transaction || !transaction.HasBufferedWrites(tableName))
        {
            return new SnapshotOverlay(entries, inserts);
        }

        if (transaction.UpdatedRows.TryGetValue(tableName, out var updates))
        {
            foreach ((RowKey rowId, Dictionary<string, object?> row) in updates)
            {
                EntryOf(entries, rowId).Update = SerializeOverlayRow(row, tableName, databaseName);
            }
        }

        if (transaction.DeletedRowIds.TryGetValue(tableName, out HashSet<RowKey>? deleted))
        {
            foreach (RowKey rowId in deleted)
            {
                EntryOf(entries, rowId).Deleted = true;
            }
        }

        if (transaction.InsertedRows.TryGetValue(tableName, out List<Dictionary<string, object?>>? inserted))
        {
            foreach (Dictionary<string, object?> row in inserted)
            {
                inserts.Add(SerializeOverlayRow(row, tableName, databaseName));
            }
        }

        return new SnapshotOverlay(entries, inserts);
    }

    private static OverlayEntry EntryOf(Dictionary<RowKey, OverlayEntry> entries, RowKey rowId)
    {
        if (!entries.TryGetValue(rowId, out OverlayEntry? entry))
        {
            entry = new OverlayEntry();
            entries[rowId] = entry;
        }

        return entry;
    }

    /// <summary>Serializes an overlay row the way a commit stores it, so it is read with the stored rows' decoder.</summary>
    private byte[] SerializeOverlayRow(IReadOnlyDictionary<string, object?> row, string tableName, string databaseName)
    {
        Dictionary<string, object?> values = row as Dictionary<string, object?> ?? row.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.OrdinalIgnoreCase);
        return RowSerializer.Serialize(databaseName, tableName, values, ResolveCatalog(), ResolveSerializerSchemaScopeKey());
    }

    /// <summary>
    /// Visits one committed row as the snapshot sees it: a deleted row is skipped, a retained image replaces the
    /// stored row (and makes it visible), and the transaction's own update replaces any visible row.
    /// </summary>
    private static bool VisitCommittedRow<TState>(ref SnapshotScan<TState> scan, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        if (scan.Overlay.Entries.TryGetValue(rowId, out OverlayEntry? entry))
        {
            entry.Seen = true;
            if (entry.Deleted)
            {
                return true;
            }

            if (entry.Image is { } image)
            {
                return Offer(ref scan, rowId, entry.Update ?? image, default);
            }

            if (!IsVisibleToSnapshot(ref scan, rowId))
            {
                return true;
            }

            return entry.Update is { } update ? Offer(ref scan, rowId, update, default) : Offer(ref scan, rowId, rowBytes, typedCells);
        }

        return !IsVisibleToSnapshot(ref scan, rowId) || Offer(ref scan, rowId, rowBytes, typedCells);
    }

    /// <summary>
    /// Offers the overlay rows the committed walk did not reach: retained images of rows no longer stored (or
    /// outside the walked key range), then the transaction's buffered inserts under their reserved keys.
    /// </summary>
    private static void VisitUnseenOverlayRows<TState>(ref SnapshotScan<TState> scan)
    {
        foreach ((RowKey rowId, OverlayEntry entry) in scan.Overlay.Entries)
        {
            if (!entry.Seen && !entry.Deleted && entry.Image is { } image && !Offer(ref scan, rowId, entry.Update ?? image, default))
            {
                return;
            }
        }

        List<byte[]> inserts = scan.Overlay.Inserts;
        for (int index = 0; index < inserts.Count; index++)
        {
            if (!Offer(ref scan, new RowKey(BufferedRowKeyHigh, (ulong)index), inserts[index], default))
            {
                return;
            }
        }
    }

    private static bool Offer<TState>(ref SnapshotScan<TState> scan, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        if (scan.Visit(ref scan.State, rowId, rowBytes, typedCells))
        {
            return true;
        }

        scan.Stopped = true;
        return false;
    }

    private static bool IsVisibleToSnapshot<TState>(ref SnapshotScan<TState> scan, RowKey rowId) =>
        SnapshotVisibilityEvaluator.IsVersionVisible(
            MvccCoordinator.EnsureRowVersionExists(scan.Engine, scan.DatabaseName, scan.TableName, rowId),
            scan.Snapshot);
}
