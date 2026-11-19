using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;
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
    /// always offered (with a bound, also the transaction's updates of rows outside it, which may have moved into
    /// it), so the visitor must test the predicates the range was derived from.
    /// </remarks>
    /// <returns>
    /// False when the scan cannot run here (staged LSM writes), so the caller takes the general path.
    /// <paramref name="typedColumns"/> limits the decoded typed columns as for <see cref="ScanTableRows{TState}"/>.
    /// </returns>
    internal bool ScanVisibleRows<TState>(string tableName, string databaseName, RowKey? lower, RowKey? upper, ref TState state, RawRowVisitor<TState> visit, bool[]? typedColumns = null)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot is null)
        {
            return ScanTableRows(tableName, databaseName, lower, upper, ref state, visit, typedColumns);
        }

        if (LsmEngine is { } lsm && lsm.HasStagedWrites(databaseName, tableName))
        {
            return false;
        }

        DataVoEngine engine = DataVoEngine.Current();
        var scan = new SnapshotScan<TState>(state, visit, engine, snapshot, tableName, databaseName, BuildSnapshotOverlay(engine, tableName, databaseName, snapshot));
        ScanCommittedRows(tableName, databaseName, lower, upper, ref scan, VisitCommittedRow, typedColumns);
        if (!scan.Stopped)
        {
            VisitUnseenOverlayRows(ref scan, offerUnseenUpdates: lower is not null || upper is not null);
        }

        state = scan.State;
        return true;
    }

    /// <summary>
    /// Streams the rows of a key range visible to the current MVCC snapshot, as <see cref="ScanVisibleRows{TState}"/>
    /// does for a whole table, but walking only <paramref name="committedRows"/> (the stored rows whose committed key
    /// is in the range, in row order) instead of every stored row. Rows the walk cannot reach are offered from the
    /// overlay: retained images of rows deleted or re-keyed since the snapshot began, the transaction's own updates
    /// of rows whose committed key is outside the range, and its buffered inserts. The visitor must test the
    /// predicates the range was derived from. Without a snapshot this is <see cref="VisitRowsByKey{TState}"/>.
    /// </summary>
    /// <returns>False when the read cannot run here (staged LSM writes), so the caller takes the general path.</returns>
    internal bool VisitVisibleRowsInKeyRange<TState>(string tableName, string databaseName, IReadOnlyList<RowKey> committedRows, ref TState state, RawRowVisitor<TState> visit, bool[]? typedColumns = null)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot is null)
        {
            return VisitRowsByKey(tableName, databaseName, committedRows, ref state, visit, typedColumns);
        }

        if (LsmEngine is { } lsm && lsm.HasStagedWrites(databaseName, tableName))
        {
            return false;
        }

        RuntimeQueryDiagnosticsScope.RecordTableRead(tableName, committedRows.Count);
        DataVoEngine engine = DataVoEngine.Current();
        var scan = new SnapshotScan<TState>(state, visit, engine, snapshot, tableName, databaseName, BuildSnapshotOverlay(engine, tableName, databaseName, snapshot));
        VisitStoredRowsByKey(tableName, databaseName, committedRows, ref scan, VisitCommittedRow, typedColumns);
        if (!scan.Stopped)
        {
            VisitUnseenOverlayRows(ref scan, offerUnseenUpdates: true);
        }

        state = scan.State;
        return true;
    }

    /// <summary>
    /// Visits the specified rows as the current MVCC snapshot sees them, combining committed storage with retained
    /// images and the current transaction's buffered updates, deletes, and inserts without scanning the whole table.
    /// </summary>
    internal bool VisitVisibleRowsByKey<TState>(
        string tableName,
        string databaseName,
        IReadOnlyList<RowKey> rowIds,
        ref TState state,
        RawRowVisitor<TState> visit)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot is null)
        {
            return VisitRowsByKey(tableName, databaseName, rowIds, ref state, visit);
        }

        if (LsmEngine is { } lsm && lsm.HasStagedWrites(databaseName, tableName))
        {
            return false;
        }

        RuntimeQueryDiagnosticsScope.RecordTableRead(tableName, rowIds.Count);
        if (rowIds.Count == 0)
        {
            return true;
        }

        DataVoEngine engine = DataVoEngine.Current();
        TransactionContext? transaction = MvccExecutionScope.CurrentTransaction;
        Dictionary<RowKey, IReadOnlyDictionary<string, object?>>? visibleImages = BuildVisibleImageMap(engine, databaseName, tableName, snapshot);
        List<Dictionary<string, object?>>? bufferedInserts = null;
        transaction?.InsertedRows.TryGetValue(tableName, out bufferedInserts);

        for (int i = 0; i < rowIds.Count; i++)
        {
            RowKey rowId = rowIds[i];
            if (TransactionContext.IsBufferedRowKey(rowId))
            {
                int position = (int)rowId.Low;
                if (bufferedInserts is not null
                    && (uint)position < (uint)bufferedInserts.Count
                    && !visit(ref state, rowId, SerializeOverlayRow(bufferedInserts[position], tableName, databaseName), default))
                {
                    break;
                }

                continue;
            }

            if (transaction?.IsDeleted(tableName, rowId) == true)
            {
                continue;
            }

            IReadOnlyDictionary<string, object?>? imageRow = null;
            bool hasImage = visibleImages is not null && visibleImages.TryGetValue(rowId, out imageRow);
            if (transaction is not null && transaction.TryGetUpdatedRow(tableName, rowId, out Dictionary<string, object?>? updatedRow))
            {
                bool baseVisible = hasImage
                    || SnapshotVisibilityEvaluator.IsVersionVisible(MvccCoordinator.GetVersionOrBase(engine, databaseName, tableName, rowId), snapshot);
                if (baseVisible && !visit(ref state, rowId, SerializeOverlayRow(updatedRow, tableName, databaseName), default))
                {
                    break;
                }

                continue;
            }

            if (hasImage)
            {
                if (!visit(ref state, rowId, SerializeOverlayRow(imageRow!, tableName, databaseName), default))
                {
                    break;
                }

                continue;
            }

            if (!SnapshotVisibilityEvaluator.IsVersionVisible(MvccCoordinator.GetVersionOrBase(engine, databaseName, tableName, rowId), snapshot))
            {
                continue;
            }

            if (!VisitSingleCommittedRowByKey(tableName, databaseName, rowId, ref state, visit))
            {
                break;
            }
        }

        return true;
    }

    private static Dictionary<RowKey, IReadOnlyDictionary<string, object?>>? BuildVisibleImageMap(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        TransactionSnapshot snapshot)
    {
        if (!engine.VersionStorageManager.HasRetainedImages(databaseName, tableName))
        {
            return null;
        }

        List<RetainedRowImage> images = engine.VersionStorageManager.GetVisibleImages(databaseName, tableName, snapshot);
        if (images.Count == 0)
        {
            return null;
        }

        var map = new Dictionary<RowKey, IReadOnlyDictionary<string, object?>>(images.Count);
        for (int i = 0; i < images.Count; i++)
        {
            map[images[i].RowId] = images[i].Row;
        }

        return map;
    }

    private bool VisitSingleCommittedRowByKey<TState>(
        string tableName,
        string databaseName,
        RowKey rowId,
        ref TState state,
        RawRowVisitor<TState> visit)
    {
        if (LsmEngine is { } lsm)
        {
            var box = (State: state, Visit: visit, KeepGoing: true);
            lsm.TryVisitStoredRowByKey(
                databaseName,
                tableName,
                rowId,
                ref box,
                static (ref (TState State, RawRowVisitor<TState> Visit, bool KeepGoing) b, RowKey id, ReadOnlySpan<byte> bytes) =>
                {
                    b.KeepGoing = b.Visit(ref b.State, id, bytes, default);
                    return b.KeepGoing;
                });
            state = box.State;
            return box.KeepGoing;
        }

        if (_storageEngine is ITypedRowStorageEngine typedStorage)
        {
            var keyed = new TypedRangeScan<TState>(state, visit, null, null);
            bool found = typedStorage.TryVisitTypedCells(databaseName, tableName, RowKeyConversions.ToRowId(rowId), ref keyed, VisitTypedRowInRange);
            state = keyed.State;
            if (keyed.Stopped)
            {
                return false;
            }

            if (found)
            {
                return true;
            }
        }

        byte[]? bytes = TryReadRowBytes(tableName, databaseName, rowId);
        return bytes is null || visit(ref state, rowId, bytes, default);
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
    /// outside the walked key range), then the transaction's buffered inserts under their reserved keys. With
    /// <paramref name="offerUnseenUpdates"/> (a walk of only some stored rows), the transaction's own updates of
    /// visible rows the walk skipped are offered too, since the update may have moved a row into the range.
    /// </summary>
    private static void VisitUnseenOverlayRows<TState>(ref SnapshotScan<TState> scan, bool offerUnseenUpdates)
    {
        foreach ((RowKey rowId, OverlayEntry entry) in scan.Overlay.Entries)
        {
            if (entry.Seen || entry.Deleted)
            {
                continue;
            }

            byte[]? row = entry.Image is { } image ? entry.Update ?? image
                : offerUnseenUpdates && entry.Update is { } update && IsVisibleToSnapshot(ref scan, rowId) ? update
                : null;
            if (row is not null && !Offer(ref scan, rowId, row, default))
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
            MvccCoordinator.GetVersionOrBase(scan.Engine, scan.DatabaseName, scan.TableName, rowId),
            scan.Snapshot);
}
