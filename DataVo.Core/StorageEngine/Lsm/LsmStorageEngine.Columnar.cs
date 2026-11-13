using DataVo.Core.Indexing.Columnar;
using DataVo.Core.StorageEngine.Lsm.V2;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.StorageEngine.Lsm;

// The columnar index of an LSM table is in two places. The rows that reached an SSTable are in that file's
// columnar segment, written with the file by the flush or compaction that made it, and are read from disk a
// chunk at a time. The rows published since the last flush are in a small index in memory, the tail, which
// every commit that publishes rows of the table writes in the same step and which starts again, nearly
// empty, after each flush. So memory holds only the unflushed rows, and opening a table builds nothing.
public sealed partial class LsmStorageEngine
{
    /// <summary>The table's columnar tail, or null when none has been built since the table was opened. Takes no lock.</summary>
    internal ColumnarIndex? Columnar(string databaseName, string tableName) =>
        _tables.TryGetValue((databaseName, tableName), out TableState? state) ? state.Columnar : null;

    /// <summary>
    /// Makes the table's SSTables carry the given columns as columnar segments, rewriting the files that do not
    /// yet, and attaches a tail built from the rows not flushed yet, replacing any it had.
    /// </summary>
    /// <remarks>
    /// Rewriting reads every row of a file that lacks its segment, once: after <c>CREATE INDEX ... USING
    /// COLUMNAR</c>, or when the indexed columns changed. Otherwise the cost is that of the unflushed rows. The
    /// table's publication is held throughout, so no commit becomes visible halfway.
    /// </remarks>
    internal ColumnarIndex BuildColumnar(string databaseName, string tableName, StorageKind[] tableKinds, int[] ordinals, int schemaVersion, int keyOrdinal)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);
        var spec = new ColumnarSpec(tableKinds, ordinals, keyOrdinal);
        lock (state.SyncRoot)
        {
            SetColumnarSpec(state, spec);
            state.Table.BackfillColumnarSegments();
            lock (state.OverlayGate)
            {
                var tail = new ColumnarIndex(tableKinds, ordinals, schemaVersion, keyed: true, keyOrdinal, tail: true);
                NoteNaNsOfFiles(state, tail);
                FillTail(state, tail);
                state.Columnar = tail;
                return tail;
            }
        }
    }

    /// <summary>The columns the table's SSTables are written with as a columnar segment, or null.</summary>
    internal ColumnarSpec? ColumnarSpecOf(string databaseName, string tableName) =>
        GetOrCreateTable(databaseName, tableName).Table.ColumnarSpec;

    /// <summary>
    /// Makes every SSTable the table writes from now on carry <paramref name="spec"/>'s columns as a columnar
    /// segment, and remembers that across restarts; null stops it. Files already written are left as they are.
    /// </summary>
    internal void SetColumnarSpec(string databaseName, string tableName, ColumnarSpec? spec) =>
        SetColumnarSpec(GetOrCreateTable(databaseName, tableName), spec);

    private static void SetColumnarSpec(TableState state, ColumnarSpec? spec)
    {
        if (spec is null)
        {
            LsmColumnarSpecFile.Delete(state.TableDirectory);
        }
        else if (state.Table.ColumnarSpec is not { } current || !current.Matches(spec))
        {
            LsmColumnarSpecFile.Save(state.TableDirectory, spec);
        }

        state.Table.ColumnarSpec = spec;
    }

    /// <summary>
    /// Stops keeping a columnar index for the table. Segments already in its files stay until compaction
    /// rewrites those files, and are ignored.
    /// </summary>
    internal void DropColumnar(string databaseName, string tableName)
    {
        if (_tables.TryGetValue((databaseName, tableName), out TableState? state))
        {
            state.Columnar = null;
            SetColumnarSpec(state, null);
        }
    }

    /// <summary>
    /// Starts the table's tail again from the rows still not flushed, once a flush has moved the others into an
    /// SSTable. Runs on the flushing thread after those rows left the overlay.
    /// </summary>
    /// <remarks>
    /// Until the new tail is in place the old one still holds the flushed rows; its keys keep the file's copies
    /// out of a scan, so each row counts once either way. Deletes that are not flushed yet go into the new
    /// tail as well, since they still hide rows of older files.
    /// </remarks>
    private static void ResetColumnarTail(TableState state)
    {
        lock (state.OverlayGate)
        {
            if (state.Columnar is { IsTail: true } old)
            {
                ColumnarIndex fresh = old.NewTail();
                FillTail(state, fresh);
                state.Columnar = fresh;
            }
        }
    }

    // Adds every row version the overlay holds: a put as a row, a deletion as a deleted key.
    private static void FillTail(TableState state, ColumnarIndex tail) =>
        state.LatestRows.ForEachVersion(tail, static (index, key, _, value) =>
        {
            if (value is null)
            {
                index.Delete(key);
            }
            else
            {
                index.Upsert(key, value);
            }
        });

    // A query asks the index whether a column holds a NaN anywhere before it compares against it; the answer
    // must cover the files' rows too.
    private void NoteNaNsOfFiles(TableState state, ColumnarIndex tail)
    {
        TableVersion version = state.AcquireVersion();
        try
        {
            foreach (LsmSstableHandle file in version.Files)
            {
                if (file.GetV2Reader(_readMode).Columnar is not { } segment || segment.Spec.ColumnCount != tail.ColumnCount)
                {
                    continue;
                }

                for (int slot = 0; slot < tail.ColumnCount; slot++)
                {
                    if (segment.AnyNaN(slot))
                    {
                        tail.NoteNaN(slot);
                    }
                }
            }
        }
        finally
        {
            version.Release();
        }
    }
}
