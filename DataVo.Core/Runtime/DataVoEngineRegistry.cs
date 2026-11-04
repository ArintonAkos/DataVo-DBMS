using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime;

/// <summary>
/// Ensures each persistent data directory is served by exactly one engine. Contexts and connections in
/// this process that open the same directory share a reference-counted engine; an exclusive lock file
/// keeps any other process out while the engine is alive. Independent engines on one directory would
/// each cache their own catalog and indexes and silently overwrite one another.
/// </summary>
internal static class DataVoEngineRegistry
{
    internal const string LockFileName = "datavo.lock";

    private static readonly object Gate = new();
    // Ordinal on purpose: a case-insensitive match could merge distinct directories on a case-sensitive
    // volume. Two spellings of one directory instead fail safely on the exclusive lock file.
    private static readonly Dictionary<string, Entry> Entries = new(StringComparer.Ordinal);

    /// <summary>
    /// Returns the engine for <paramref name="config"/>, creating it with <paramref name="create"/> on
    /// first use. Storage modes without a data directory always get a private engine.
    /// </summary>
    internal static DataVoEngine Acquire(DataVoConfig config, Func<DataVoConfig, DataVoEngine> create)
    {
        string? root = DataVoConfigSharing.ResolvePersistentRoot(config);
        if (root is null)
        {
            return create(config);
        }

        IReadOnlyList<KeyValuePair<string, string>> fingerprint = DataVoConfigSharing.Capture(config);

        // Opening and disposing serialize on one gate so a directory is never served by a closing and a
        // newly opening engine at the same time. Recovery therefore also runs under the gate.
        lock (Gate)
        {
            if (Entries.TryGetValue(root, out Entry? existing))
            {
                string? conflict = DataVoConfigSharing.FindConflict(existing.Fingerprint, fingerprint);
                if (conflict is not null)
                {
                    throw new DatabaseInUseException(
                        root,
                        $"Data directory '{root}' is already open in this process with a different '{conflict}' setting. " +
                        "Contexts and connections that share a directory must use equivalent configuration.");
                }

                existing.References++;
                return existing.Engine;
            }

            FileStream lockFile = AcquireDirectoryLock(root);
            try
            {
                DataVoEngine engine = create(config);
                engine.AttachSharedDirectory(root);
                Entries[root] = new Entry(engine, fingerprint, lockFile);
                return engine;
            }
            catch
            {
                lockFile.Dispose();
                throw;
            }
        }
    }

    /// <summary>
    /// Drops one reference to a shared engine. Returns <see langword="true"/> when that was the last
    /// reference: the engine has been shut down and the directory lock released.
    /// </summary>
    internal static bool Release(DataVoEngine engine, string root, Action shutdown)
    {
        lock (Gate)
        {
            if (!Entries.TryGetValue(root, out Entry? entry) || !ReferenceEquals(entry.Engine, engine))
            {
                return false;
            }

            if (--entry.References > 0)
            {
                return false;
            }

            try
            {
                shutdown();
            }
            finally
            {
                Entries.Remove(root);
                entry.LockFile.Dispose();
            }

            return true;
        }
    }

    private static FileStream AcquireDirectoryLock(string root)
    {
        Directory.CreateDirectory(root);
        string path = Path.Combine(root, LockFileName);
        try
        {
            return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException ex)
        {
            throw new DatabaseInUseException(
                root,
                $"Data directory '{root}' is locked by another process. Close the other DataVo instance before opening it here.",
                ex);
        }
    }

    private sealed class Entry(DataVoEngine engine, IReadOnlyList<KeyValuePair<string, string>> fingerprint, FileStream lockFile)
    {
        public DataVoEngine Engine { get; } = engine;

        public IReadOnlyList<KeyValuePair<string, string>> Fingerprint { get; } = fingerprint;

        public FileStream LockFile { get; } = lockFile;

        public int References { get; set; } = 1;
    }
}
