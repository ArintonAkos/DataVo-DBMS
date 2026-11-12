using DataVo.Core.Indexing.Columnar;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.StorageEngine.Lsm;

// The columnar index of an LSM table is derived state held in memory. It mirrors the table's published rows:
// it is built from them by one scan, and from then on every commit that publishes rows of the table writes
// the index in the same step. A flush or a compaction moves bytes without changing any row, so neither
// touches it, and after a restart it is simply built again from the durable rows.
public sealed partial class LsmStorageEngine
{
    /// <summary>The table's columnar index, or null when none has been built since the table was opened. Takes no lock.</summary>
    internal ColumnarIndex? Columnar(string databaseName, string tableName) =>
        _tables.TryGetValue((databaseName, tableName), out TableState? state) ? state.Columnar : null;

    /// <summary>
    /// Builds the table's columnar index from its live rows and attaches it, replacing any it had.
    /// </summary>
    /// <remarks>
    /// The table's publication is held for the scan, so no commit becomes visible halfway through it. A row
    /// whose commit reached an SSTable before it was published is seen by the scan and published again
    /// afterwards; rows are added by key, so the second time changes nothing.
    /// </remarks>
    internal ColumnarIndex BuildColumnar(string databaseName, string tableName, StorageKind[] tableKinds, int[] ordinals, int schemaVersion, int keyOrdinal)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);
        lock (state.SyncRoot)
        lock (state.OverlayGate)
        {
            var build = new ColumnarBuild(new ColumnarIndex(tableKinds, ordinals, schemaVersion, keyed: true, keyOrdinal));
            MergeLiveRows(state, ref build, null, null, int.MaxValue);
            state.Columnar = build.Index;
            return build.Index;
        }
    }

    /// <summary>Stops keeping a columnar index for the table.</summary>
    internal void DropColumnar(string databaseName, string tableName)
    {
        if (_tables.TryGetValue((databaseName, tableName), out TableState? state))
        {
            state.Columnar = null;
        }
    }

    private readonly struct ColumnarBuild(ColumnarIndex index) : ILiveRowVisitor
    {
        public ColumnarIndex Index { get; } = index;

        public bool Visit(RowKey rowId, ulong sequence, ReadOnlySpan<byte> value)
        {
            Index.Upsert(rowId, value);
            return true;
        }
    }
}
