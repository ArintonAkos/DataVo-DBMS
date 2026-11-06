using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Core.Runtime;

/// <summary>
/// HNSW graph snapshots for LSM databases. Without them every open rebuilt each vector index from all
/// rows (about 1.3 s for 20,000 768-dimension vectors, growing with the data). A clean close writes the
/// graph with the commit LSN it reflects; open loads it and re-indexes only rows written after that LSN.
/// </summary>
public sealed partial class DataVoEngine
{
    // Above this share of changed rows a parallel rebuild beats one-by-one catch-up inserts.
    private const double SnapshotCatchUpLimit = 0.5;

    // The graph loaded from each snapshot and its mutation count then: unchanged graphs are not rewritten.
    private readonly Dictionary<(string Database, string Table, string Index), (HNSWIndex Graph, long Mutations)> _vectorSnapshotStamps = [];

    private readonly record struct PendingVectorSnapshot(string Path, HNSWIndex Index, string Column);

    /// <summary>
    /// Restores a vector index from its snapshot and catches it up with the rows. Returns
    /// <see langword="false"/> (the caller rebuilds) when there is no usable snapshot.
    /// </summary>
    private bool TryRestoreVectorIndexFromSnapshot(string databaseName, string tableName, IndexFile index)
    {
        LsmStorageEngine? lsm = StorageContext.LsmEngine;
        if (lsm is null
            || BrowserRuntimeFlags.ForceVectorFallback
            || !string.Equals(index.IndexKind, "HNSW", StringComparison.OrdinalIgnoreCase)
            || index.AttributeNames.Count != 1)
        {
            return false;
        }

        string column = index.AttributeNames[0];
        string path = lsm.GetVectorSnapshotPath(databaseName, tableName, index.IndexFileName);
        if (!HnswSnapshotFile.TryRead(path, column, out HNSWIndex? graph, out long watermark) || graph is null)
        {
            return false;
        }

        (List<RowKey> reindex, List<RowKey> removed) = graph.PlanCatchUp(
            visit => lsm.ForEachLiveRowVersion(databaseName, tableName, visit),
            watermark);
        if (reindex.Count + removed.Count > Math.Max(1_000, graph.Count * SnapshotCatchUpLimit))
        {
            return false;
        }

        graph.Delete(removed);
        if (reindex.Count > 0)
        {
            foreach (KeyValuePair<RowKey, StoredRow> row in StorageContext.GetTypedTableContents(reindex, tableName, databaseName))
            {
                if (row.Value.TryGet(column, out CellValue cell) && !cell.IsNull && cell.Type == CellType.Vector)
                {
                    graph.Insert(row.Key, cell.AsVectorReadOnlySpan());
                }
                else if (graph.ContainsRow(row.Key))
                {
                    graph.Delete([row.Key]);
                }
            }
        }

        IndexManager.InstallVectorIndex(graph, index.IndexFileName, tableName, databaseName, index.IndexKind);
        lock (_vectorSnapshotStamps)
        {
            _vectorSnapshotStamps[(databaseName, tableName, index.IndexFileName)] = (graph, graph.MutationCount);
        }

        return true;
    }

    /// <summary>
    /// Collects the HNSW graphs that changed since they were loaded. Runs at close, after writers have
    /// stopped, so each graph reflects every applied commit.
    /// </summary>
    private List<PendingVectorSnapshot> CaptureVectorSnapshots(LsmStorageEngine lsm)
    {
        var pending = new List<PendingVectorSnapshot>();
        if (!HnswSnapshotFile.IsSupported || BrowserRuntimeFlags.ForceVectorFallback)
        {
            return pending;
        }

        foreach (string databaseName in Catalog.GetDatabases())
        {
            foreach (string tableName in Catalog.GetTables(databaseName))
            {
                foreach (IndexFile index in Catalog.GetTableIndexes(tableName, databaseName))
                {
                    if (!string.Equals(index.IndexKind, "HNSW", StringComparison.OrdinalIgnoreCase)
                        || index.AttributeNames.Count != 1
                        || IndexManager.TryGetLoadedHnswIndex(index.IndexFileName, tableName, databaseName) is not { } graph)
                    {
                        continue;
                    }

                    string path = lsm.GetVectorSnapshotPath(databaseName, tableName, index.IndexFileName);
                    bool unchanged;
                    lock (_vectorSnapshotStamps)
                    {
                        unchanged = _vectorSnapshotStamps.TryGetValue((databaseName, tableName, index.IndexFileName), out var loaded)
                            && ReferenceEquals(loaded.Graph, graph)
                            && loaded.Mutations == graph.MutationCount;
                    }

                    if (!unchanged || !File.Exists(path))
                    {
                        pending.Add(new PendingVectorSnapshot(path, graph, index.AttributeNames[0]));
                    }
                }
            }
        }

        return pending;
    }

    /// <summary>
    /// Writes the captured graphs once the storage engine has closed: its final flush makes every row up
    /// to <paramref name="watermark"/> durable first, so a snapshot never runs ahead of the data. A
    /// failed write only costs a rebuild on the next open, so it never fails the close.
    /// </summary>
    private static void WriteVectorSnapshots(List<PendingVectorSnapshot> pending, long watermark)
    {
        foreach (PendingVectorSnapshot snapshot in pending)
        {
            try
            {
                HnswSnapshotFile.Write(snapshot.Path, snapshot.Index, watermark, snapshot.Column);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // The write is atomic, so any older snapshot is intact and still a valid (older) base.
                Logger.Error($"Could not save the vector index snapshot '{snapshot.Path}': {ex.Message}");
            }
        }
    }
}
