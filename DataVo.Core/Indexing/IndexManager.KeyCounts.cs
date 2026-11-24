using System.Buffers.Binary;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;
using IndexCacheKey = (string Database, string Table, string Index);

namespace DataVo.Core.Indexing;

/// <summary>Receives one non-NULL key of a single-column integer index and how many rows hold it. Returns false to stop.</summary>
internal delegate bool IntegerKeyCountVisitor<TState>(ref TState state, long key, long rows);

/// <summary>Row counts per key of a single-column integer index, read from the index without touching table rows.</summary>
/// <remarks>
/// A persisted (LSM) index holds its NULL-key rows (see <see cref="InsertNullKeyRow"/>) and reports how many there
/// are; an in-memory index does not hold them, so a caller that needs the NULL group counts it from the rows.
/// </remarks>
public partial class IndexManager
{
    /// <summary>
    /// Whether <see cref="TryVisitIntegerKeyCounts{TState}"/> can answer for this index as things stand: a built LSM
    /// index table (not a clustered table's primary key, which the rows themselves are), or a complete in-memory
    /// integer lane.
    /// </summary>
    internal bool CanCountIntegerKeys(string indexName, string tableName, string databaseName) =>
        _lsmStore is { } store
            ? !store.IsRowKeyIndex(databaseName, tableName, indexName) && store.IsIndexBuilt(databaseName, tableName, indexName)
            : HasIntegerIndexFastLane(indexName, tableName, databaseName);

    /// <summary>
    /// Visits every non-NULL key of a single-column integer secondary index with the number of rows that hold it. On
    /// the LSM engine the persisted index is walked in key order; in memory the complete integer lane is read.
    /// Returns false when the index cannot answer exactly (no such index, an incomplete lane, a key that is not an
    /// integer, staged index changes). The visitor may already have been called by then, so a caller discards what it
    /// gathered on false. <paramref name="nullRows"/> is the number of rows whose key is NULL, or null when the index
    /// does not hold them (in memory).
    /// </summary>
    internal bool TryVisitIntegerKeyCounts<TState>(string indexName, string tableName, string databaseName, ref TState state, IntegerKeyCountVisitor<TState> visit, out long? nullRows)
    {
        nullRows = null;
        bool answered = _lsmStore is { } store
            ? VisitLsmIntegerKeyCounts(store, indexName, tableName, databaseName, ref state, visit, out nullRows)
            : VisitLaneIntegerKeyCounts(indexName, tableName, databaseName, ref state, visit);
        if (answered)
        {
            RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
        }

        return answered;
    }

    private static bool VisitLsmIntegerKeyCounts<TState>(StorageEngine.ILsmIndexStore store, string indexName, string tableName, string databaseName, ref TState state, IntegerKeyCountVisitor<TState> visit, out long? nullRows)
    {
        var box = new KeyCountBox<TState>(state, visit);
        bool walked = store.TryVisitIndexKeyCounts(databaseName, tableName, indexName, ref box, static (ref KeyCountBox<TState> b, ReadOnlySpan<byte> encodedKey, long rows) =>
        {
            if (encodedKey.Length == 1 + sizeof(long) && encodedKey[0] == LsmIndexKeyCodec.IntegerTag)
            {
                long key = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(encodedKey[1..]) ^ long.MinValue);
                return b.Visit(ref b.State, key, rows);
            }

            if (encodedKey.Length == 1 && encodedKey[0] == LsmIndexKeyCodec.NullTag)
            {
                b.NullRows += rows;
                return true;
            }

            b.Failed = true;
            return false;
        });
        state = box.State;
        nullRows = box.NullRows;
        return walked && !box.Failed;
    }

    /// <summary>Reads the counts from a complete in-memory integer lane.</summary>
    private bool VisitLaneIntegerKeyCounts<TState>(string indexName, string tableName, string databaseName, ref TState state, IntegerKeyCountVisitor<TState> visit)
    {
        IndexCacheKey cacheKey = GetCacheKey(indexName, tableName, databaseName);
        lock (_lock)
        {
            if (!IsLaneComplete(cacheKey) || !_integerIndexMaps.TryGetValue(cacheKey, out Dictionary<long, List<RowKey>>? lane))
            {
                return false;
            }

            foreach (KeyValuePair<long, List<RowKey>> entry in lane)
            {
                if (entry.Value.Count > 0 && !visit(ref state, entry.Key, entry.Value.Count))
                {
                    break;
                }
            }
        }

        return true;
    }

    private struct KeyCountBox<TState>(TState state, IntegerKeyCountVisitor<TState> visit)
    {
        public TState State = state;
        public readonly IntegerKeyCountVisitor<TState> Visit = visit;
        public bool Failed;
        public long NullRows;
    }
}
