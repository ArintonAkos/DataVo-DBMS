using System.Collections.Concurrent;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.MVCC;

/// <summary>
/// Manages version metadata storage for all rows in the database.
/// Maintains a mapping from row identifiers to RowVersion structures.
/// </summary>
public class VersionStorageManager : IDisposable
{
    /// <summary>
    /// Stores version metadata indexed by row ID.
    /// Format: (databaseName, tableName, rowId) -> RowVersion
    /// </summary>
    private readonly ConcurrentDictionary<(string, string, RowKey), RowVersion> _versionMetadata = new();

    /// <summary>
    /// Lock for synchronizing version metadata access.
    /// </summary>
    private readonly ReaderWriterLockSlim _versionLock = new();

    // Storage keeps one version of each row, so the images an open snapshot may still need live here.
    private readonly ConcurrentDictionary<(string Database, string Table), List<RetainedRowImage>> _retainedImages = new();

    /// <summary>
    /// Allocates a new row version with the given xmin (creator transaction).
    /// Creates the initial version metadata for a newly inserted/updated row.
    /// </summary>
    public RowVersion AllocateVersion(string databaseName, string tableName, RowKey rowId, long xmin)
    {
        var version = new RowVersion(xmin: xmin, xmax: 0, versionChain: 0);
        var key = (databaseName, tableName, rowId);

        _versionLock.EnterWriteLock();
        try
        {
            _versionMetadata[key] = version;
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }

        return version;
    }

    /// <summary>
    /// Allocates version metadata for a contiguous or non-contiguous insert batch under one write lock.
    /// </summary>
    public void AllocateInsertVersions(string databaseName, string tableName, IReadOnlyList<RowKey> rowIds, long xmin)
    {
        if (rowIds.Count == 0)
        {
            return;
        }

        var version = new RowVersion(xmin: xmin, xmax: 0, versionChain: 0);

        _versionLock.EnterWriteLock();
        try
        {
            for (int i = 0; i < rowIds.Count; i++)
            {
                _versionMetadata[(databaseName, tableName, rowIds[i])] = version;
            }
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Forgets the version metadata of rows (absent metadata reads as an always-visible base version).
    /// Free when no metadata is held at all, which is the steady state of auto-commit workloads.
    /// </summary>
    public void RemoveVersions(string databaseName, string tableName, IReadOnlyList<RowKey> rowIds)
    {
        if (_versionMetadata.IsEmpty || rowIds.Count == 0)
        {
            return;
        }

        _versionLock.EnterWriteLock();
        try
        {
            for (int i = 0; i < rowIds.Count; i++)
            {
                _versionMetadata.TryRemove((databaseName, tableName, rowIds[i]), out _);
            }
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>Forgets the version metadata of one row (see <see cref="RemoveVersions"/>).</summary>
    public void RemoveVersion(string databaseName, string tableName, RowKey rowId) =>
        RemoveVersions(databaseName, tableName, [rowId]);

    /// <summary>
    /// Retrieves the current version metadata for a row.
    /// </summary>
    public RowVersion? GetVersion(string databaseName, string tableName, RowKey rowId)
    {
        if (_versionMetadata.IsEmpty)
        {
            return null;
        }

        var key = (databaseName, tableName, rowId);

        _versionLock.EnterReadLock();
        try
        {
            return _versionMetadata.TryGetValue(key, out var version) ? version : null;
        }
        finally
        {
            _versionLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Updates a version's xmax to mark it as superseded/deleted.
    /// Creates a new version chain entry pointing to the next version.
    /// </summary>
    public void MarkVersionObsolete(string databaseName, string tableName, RowKey rowId, long xmax)
    {
        var key = (databaseName, tableName, rowId);

        _versionLock.EnterWriteLock();
        try
        {
            if (_versionMetadata.TryGetValue(key, out var currentVersion))
            {
                currentVersion.Xmax = xmax;
                _versionMetadata[key] = currentVersion;
            }
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Updates version_chain to point to the next version in the chain.
    /// Used when an old version is superseded by a new one.
    /// </summary>
    public void LinkVersionChain(string databaseName, string tableName, RowKey oldRowId, RowKey newRowId)
    {
        var oldKey = (databaseName, tableName, oldRowId);

        _versionLock.EnterWriteLock();
        try
        {
            if (_versionMetadata.TryGetValue(oldKey, out var oldVersion))
            {
                oldVersion.VersionChain = newRowId;
                _versionMetadata[oldKey] = oldVersion;
            }
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Retrieves the full version chain for a row, starting from the given rowId.
    /// Returns versions in order from oldest to newest.
    /// </summary>
    public List<(RowKey RowId, RowVersion Version)> GetVersionChain(string databaseName, string tableName, RowKey startRowId)
    {
        var chain = new List<(RowKey, RowVersion)>();
        RowKey currentRowId = startRowId;

        _versionLock.EnterReadLock();
        try
        {
            while (currentRowId != 0)
            {
                var key = (databaseName, tableName, currentRowId);
                if (!_versionMetadata.TryGetValue(key, out var version))
                {
                    break;
                }

                chain.Add((currentRowId, version));
                currentRowId = version.VersionChain;
            }
        }
        finally
        {
            _versionLock.ExitReadLock();
        }

        return chain;
    }

    /// <summary>
    /// Cleans up all version metadata for a table (used during table drop/truncate).
    /// </summary>
    public void ClearTableVersions(string databaseName, string tableName)
    {
        _retainedImages.TryRemove((databaseName, tableName), out _);
        _versionLock.EnterWriteLock();
        try
        {
            var keysToRemove = _versionMetadata.Keys
                .Where(k => k.Item1 == databaseName && k.Item2 == tableName)
                .ToList();

            foreach (var key in keysToRemove)
            {
                _versionMetadata.TryRemove(key, out _);
            }
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes all tracked version metadata.
    /// </summary>
    public void Clear()
    {
        _retainedImages.Clear();
        _versionLock.EnterWriteLock();
        try
        {
            _versionMetadata.Clear();
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>Keeps a row's previous image for snapshots that began before it was replaced or deleted.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The owning table.</param>
    /// <param name="rowId">The row key the image was stored under.</param>
    /// <param name="image">The complete previous row.</param>
    /// <param name="xmin">The transaction that created this image.</param>
    /// <param name="xmax">The transaction that replaced or deleted it.</param>
    /// <example>
    /// <code>
    /// versions.RetainImage("Shop", "Items", rowId, oldRow, xmin: 0, xmax: updatingTransactionId);
    /// </code>
    /// </example>
    public void RetainImage(string databaseName, string tableName, RowKey rowId, IReadOnlyDictionary<string, object?> image, long xmin, long xmax)
    {
        List<RetainedRowImage> images = _retainedImages.GetOrAdd((databaseName, tableName), static _ => []);
        lock (images)
        {
            images.Add(new RetainedRowImage(rowId, image, new RowVersion(xmin, xmax)));
        }
    }

    /// <summary>Gets whether any row of a table has been replaced while a transaction was open.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The owning table.</param>
    /// <returns><see langword="true"/> when retained images exist for the table.</returns>
    public bool HasRetainedImages(string databaseName, string tableName) =>
        !_retainedImages.IsEmpty
        && _retainedImages.TryGetValue((databaseName, tableName), out List<RetainedRowImage>? images)
        && Volatile.Read(ref images) is { Count: > 0 };

    /// <summary>Returns the retained images a snapshot should read.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The owning table.</param>
    /// <param name="snapshot">The reading snapshot.</param>
    /// <returns>The visible images; empty when there are none.</returns>
    internal List<RetainedRowImage> GetVisibleImages(string databaseName, string tableName, TransactionSnapshot snapshot)
    {
        var visible = new List<RetainedRowImage>();
        if (_retainedImages.TryGetValue((databaseName, tableName), out List<RetainedRowImage>? images))
        {
            lock (images)
            {
                visible.AddRange(images.Where(image => image.Version.IsVisibleTo(snapshot)));
            }
        }

        return visible;
    }

    /// <summary>Drops images no open snapshot can read.</summary>
    /// <param name="oldestSnapshot">The oldest open snapshot, or <see langword="null"/> when no transaction is open.</param>
    public void PruneImages(long? oldestSnapshot)
    {
        foreach (KeyValuePair<(string Database, string Table), List<RetainedRowImage>> table in _retainedImages)
        {
            lock (table.Value)
            {
                table.Value.RemoveAll(image => oldestSnapshot is null || image.Version.Xmax <= oldestSnapshot);
            }
        }
    }

    /// <summary>
    /// Removes version metadata entries whose backing physical rows no longer exist.
    /// </summary>
    public int VacuumTable(string databaseName, StorageContext storageContext)
    {
        if (storageContext == null)
        {
            throw new ArgumentNullException(nameof(storageContext));
        }

        List<(string DatabaseName, string TableName, RowKey RowId)> candidates;
        _versionLock.EnterReadLock();
        try
        {
            candidates = _versionMetadata.Keys
                .Where(k => k.Item1 == databaseName)
                .Select(k => (k.Item1, k.Item2, k.Item3))
                .ToList();
        }
        finally
        {
            _versionLock.ExitReadLock();
        }

        List<(string DatabaseName, string TableName, RowKey RowId)> missingRows = candidates
            .Where(k => !storageContext.TableContainsRow(k.RowId, k.TableName, databaseName))
            .ToList();

        if (missingRows.Count == 0)
        {
            return 0;
        }

        int removed = 0;
        _versionLock.EnterWriteLock();
        try
        {
            foreach (var key in missingRows)
            {
                var metadataKey = (key.DatabaseName, key.TableName, key.RowId);
                if (_versionMetadata.TryRemove(metadataKey, out _))
                {
                    removed++;
                }
            }
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }

        return removed;
    }

    /// <summary>
    /// Disposes the version storage manager and releases all resources.
    /// </summary>
    public void Dispose()
    {
        _versionLock?.Dispose();
    }
}

/// <summary>A row's earlier state, kept for snapshots that started before the row changed.</summary>
/// <param name="RowId">The row key the image was stored under.</param>
/// <param name="Row">The complete earlier row.</param>
/// <param name="Version">The lifetime of this image.</param>
internal readonly record struct RetainedRowImage(RowKey RowId, IReadOnlyDictionary<string, object?> Row, RowVersion Version);
