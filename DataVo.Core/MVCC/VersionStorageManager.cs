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
    private readonly ConcurrentDictionary<(string, string, long), RowVersion> _versionMetadata = new();

    /// <summary>
    /// Lock for synchronizing version metadata access.
    /// </summary>
    private readonly ReaderWriterLockSlim _versionLock = new();

    /// <summary>
    /// Allocates a new row version with the given xmin (creator transaction).
    /// Creates the initial version metadata for a newly inserted/updated row.
    /// </summary>
    public RowVersion AllocateVersion(string databaseName, string tableName, long rowId, long xmin)
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
    /// Retrieves the current version metadata for a row.
    /// </summary>
    public RowVersion? GetVersion(string databaseName, string tableName, long rowId)
    {
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
    public void MarkVersionObsolete(string databaseName, string tableName, long rowId, long xmax)
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
    public void LinkVersionChain(string databaseName, string tableName, long oldRowId, long newRowId)
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
    public List<(long RowId, RowVersion Version)> GetVersionChain(string databaseName, string tableName, long startRowId)
    {
        var chain = new List<(long, RowVersion)>();
        long currentRowId = startRowId;

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
    /// Removes version metadata entries whose backing physical rows no longer exist.
    /// </summary>
    public int VacuumTable(string databaseName, StorageContext storageContext)
    {
        if (storageContext == null)
        {
            throw new ArgumentNullException(nameof(storageContext));
        }

        List<(string DatabaseName, string TableName, long RowId)> candidates;
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

        List<(string DatabaseName, string TableName, long RowId)> missingRows = candidates
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
