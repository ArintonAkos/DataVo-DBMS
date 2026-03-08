using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Transactions;

/// <summary>
/// Reads write-ahead log entries from persistent storage.
/// </summary>
/// <remarks>
/// The reader exposes high-level filtering operations while delegating file locking and
/// JSON line parsing to <see cref="WalFileStore"/>.
/// </remarks>
/// <example>
/// <code>
/// var reader = new WalReader(config);
/// List&lt;WalEntry&gt; pendingEntries = reader.ReadUncheckpointed();
/// </code>
/// </example>
public sealed class WalReader(DataVoConfig config)
{
    /// <summary>
    /// Provides synchronized access to the WAL file contents.
    /// </summary>
    private readonly WalFileStore _fileStore = new(config.ResolveWalFilePath());

    /// <summary>
    /// Reads every WAL entry from the underlying log file.
    /// </summary>
    /// <returns>The ordered list of persisted entries.</returns>
    public List<WalEntry> ReadAll()
    {
        return _fileStore.ReadEntries();
    }

    /// <summary>
    /// Reads only entries that have not yet been checkpointed.
    /// </summary>
    /// <returns>The ordered list of uncheckpointed entries.</returns>
    public List<WalEntry> ReadUncheckpointed()
    {
        return [.. ReadAll()
            .Where(IsUncheckpointed)
            .OrderBy(entry => entry.Timestamp)];
    }

    /// <summary>
    /// Determines whether a WAL entry still requires recovery or checkpoint processing.
    /// </summary>
    /// <param name="entry">The entry to inspect.</param>
    /// <returns><c>true</c> when the entry is not yet checkpointed; otherwise, <c>false</c>.</returns>
    private static bool IsUncheckpointed(WalEntry entry)
    {
        return !entry.IsCheckpointed;
    }
}
