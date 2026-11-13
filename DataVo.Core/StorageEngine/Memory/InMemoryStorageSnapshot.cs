namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// A copy of every in-memory table's rows at one moment, to go back to later. The snapshot owns its copy: it
/// is taken from copies of the tables and hands out further copies, so it can be restored more than once and
/// nothing done to the tables afterwards reaches it.
/// </summary>
internal sealed class InMemoryStorageSnapshot
{
    private readonly Dictionary<(string DatabaseName, string TableName), TableRows> _tables;

    /// <summary>Takes ownership of copies of the tables' rows.</summary>
    /// <param name="tables">Each table's rows, already copied from the live table.</param>
    internal InMemoryStorageSnapshot(Dictionary<(string DatabaseName, string TableName), TableRows> tables)
    {
        _tables = tables;
    }

    /// <summary>Each table of the snapshot with a new copy of its rows.</summary>
    internal IEnumerable<KeyValuePair<(string DatabaseName, string TableName), TableRows>> EnumerateTables()
    {
        foreach (var entry in _tables)
        {
            yield return new KeyValuePair<(string DatabaseName, string TableName), TableRows>(entry.Key, entry.Value.Clone());
        }
    }
}
