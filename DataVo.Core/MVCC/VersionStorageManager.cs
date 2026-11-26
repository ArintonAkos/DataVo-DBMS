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

    // Set before the first metadata entry is added and never cleared: until then every "remove this row's
    // metadata" call of a writer that records no versions returns without even enumerating the map (whose
    // enumerator is an allocation).
    private volatile bool _mayHaveVersions;

    /// <summary>
    /// Lock for synchronizing version metadata access.
    /// </summary>
    private readonly ReaderWriterLockSlim _versionLock = new();

    // Every version change in the order it was recorded, with the stamp that made it: VacuumVersions walks this
    // from the front instead of scanning the whole map. Guarded by _versionLock.
    private readonly Queue<(string Database, string Table, RowKey RowId, long Stamp)> _recorded = new();

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
            _mayHaveVersions = true;
            _versionMetadata[key] = version;
            _recorded.Enqueue((databaseName, tableName, rowId, xmin));
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
            _mayHaveVersions = true;
            for (int i = 0; i < rowIds.Count; i++)
            {
                _versionMetadata[(databaseName, tableName, rowIds[i])] = version;
                _recorded.Enqueue((databaseName, tableName, rowIds[i], xmin));
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
        if (rowIds.Count == 0 || HasNoVersionMetadata())
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

    /// <summary>Sets a row's version metadata as it was before a failed commit changed it.</summary>
    public void SetVersion(string databaseName, string tableName, RowKey rowId, RowVersion version)
    {
        _versionLock.EnterWriteLock();
        try
        {
            _mayHaveVersions = true;
            _versionMetadata[(databaseName, tableName, rowId)] = version;
            _recorded.Enqueue((databaseName, tableName, rowId, Math.Max(version.Xmin, version.Xmax)));
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>Drops the images a failed commit retained (the ones it ended).</summary>
    /// <param name="xmax">The failed commit's stamp.</param>
    public void DiscardImages(long xmax)
    {
        foreach (KeyValuePair<(string Database, string Table), List<RetainedRowImage>> table in _retainedImages)
        {
            lock (table.Value)
            {
                table.Value.RemoveAll(image => image.Version.Xmax == xmax);
            }
        }
    }

    /// <summary>Forgets the version metadata of one row (see <see cref="RemoveVersions"/>).</summary>
    public void RemoveVersion(string databaseName, string tableName, RowKey rowId)
    {
        if (HasNoVersionMetadata())
        {
            return;
        }

        _versionLock.EnterWriteLock();
        try
        {
            _versionMetadata.TryRemove((databaseName, tableName, rowId), out _);
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Retrieves the current version metadata for a row.
    /// </summary>
    public RowVersion? GetVersion(string databaseName, string tableName, RowKey rowId)
    {
        var key = (databaseName, tableName, rowId);

        // A lock-free probe answers the common miss. (ConcurrentDictionary.IsEmpty is no shortcut: on an empty
        // map, the steady state between transactions, it takes every bucket lock to confirm.)
        if (!_versionMetadata.ContainsKey(key))
        {
            return null;
        }

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
    /// Updates a version's <see cref="RowVersion.Xmax"/> to mark it as superseded or deleted, atomically
    /// bootstrapping a legacy base version (<c>Xmin = 0</c>) if no metadata entry is currently present.
    /// </summary>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="tableName">The owning table name.</param>
    /// <param name="rowId">The row identifier being obsoleted.</param>
    /// <param name="xmax">The transaction stamp that deleted or superseded the row.</param>
    public void MarkVersionObsolete(string databaseName, string tableName, RowKey rowId, long xmax)
    {
        var key = (databaseName, tableName, rowId);

        _versionLock.EnterWriteLock();
        try
        {
            _mayHaveVersions = true;
            RowVersion currentVersion = _versionMetadata.TryGetValue(key, out RowVersion existing)
                ? existing
                : new RowVersion(xmin: 0, xmax: 0, versionChain: 0);
            currentVersion.Xmax = xmax;
            _versionMetadata[key] = currentVersion;
            _recorded.Enqueue((databaseName, tableName, rowId, xmax));
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
            _recorded.Clear();
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

    /// <summary>
    /// Returns whether a concurrent map holds no entries, without the bucket locks
    /// <see cref="ConcurrentDictionary{TKey, TValue}.IsEmpty"/> takes when the map is empty. Like that check, the
    /// answer is a moment-in-time hint for a fast path, not a guarantee.
    /// </summary>
    private bool HasNoVersionMetadata() => !_mayHaveVersions || IsEmptyWithoutLocks(_versionMetadata);

    private static bool IsEmptyWithoutLocks<TKey, TValue>(ConcurrentDictionary<TKey, TValue> map)
        where TKey : notnull
    {
        foreach (KeyValuePair<TKey, TValue> _ in map)
        {
            return false;
        }

        return true;
    }

    /// <summary>Gets whether any row of a table has been replaced while a transaction was open.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The owning table.</param>
    /// <returns><see langword="true"/> when retained images exist for the table.</returns>
    public bool HasRetainedImages(string databaseName, string tableName) =>
        _retainedImages.TryGetValue((databaseName, tableName), out List<RetainedRowImage>? images)
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

    /// <summary>
    /// Drops retained images whose ending timestamp is strictly below <paramref name="oldestSnapshot"/>,
    /// or all images when <paramref name="oldestSnapshot"/> is <see langword="null"/>.
    /// </summary>
    /// <param name="oldestSnapshot">The exclusive timestamp cutoff, or <see langword="null"/> when no transaction is open.</param>
    public void PruneImages(long? oldestSnapshot)
    {
        foreach (KeyValuePair<(string Database, string Table), List<RetainedRowImage>> table in _retainedImages)
        {
            lock (table.Value)
            {
                table.Value.RemoveAll(image => oldestSnapshot is null || image.Version.Xmax < oldestSnapshot);
            }
        }
    }

    /// <summary>
    /// Drops version metadata no open snapshot can need: a version created and (if ended) ended before the oldest
    /// open snapshot reads the same as having no metadata (an always-visible row, or a row that is gone for
    /// everyone). Walks the versions recorded since the last pass in recording order and stops at the first one
    /// an open snapshot may still need, so the cost follows the commits, not the size of the version map. With no
    /// open transaction everything is dropped.
    /// </summary>
    /// <param name="oldestSnapshot">The oldest open snapshot, or <see langword="null"/> when no transaction is open.</param>
    /// <returns>The number of metadata entries removed.</returns>
    public int VacuumVersions(long? oldestSnapshot)
    {
        if (_recorded.Count == 0 && HasNoVersionMetadata())
        {
            return 0;
        }

        _versionLock.EnterWriteLock();
        try
        {
            if (oldestSnapshot is not { } oldest)
            {
                int count = _versionMetadata.Count;
                _versionMetadata.Clear();
                _recorded.Clear();
                return count;
            }

            int removed = 0;
            while (_recorded.Count > 0 && _recorded.Peek().Stamp < oldest)
            {
                (string database, string table, RowKey rowId, _) = _recorded.Dequeue();
                var key = (database, table, rowId);
                if (_versionMetadata.TryGetValue(key, out RowVersion version)
                    && version.Xmin < oldest
                    && (version.Xmax == 0 || version.Xmax < oldest)
                    && _versionMetadata.TryRemove(key, out _))
                {
                    removed++;
                }
            }

            return removed;
        }
        finally
        {
            _versionLock.ExitWriteLock();
        }
    }

    /// <summary>Gets the number of rows that currently carry version metadata (diagnostics and tests).</summary>
    public int VersionCount => _versionMetadata.Count;

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
