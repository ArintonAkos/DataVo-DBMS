namespace DataVo.Core.StorageEngine.Memory;

internal sealed class InMemoryStorageSnapshot
{
    private readonly Dictionary<(string DatabaseName, string TableName), List<byte[]?>> _tables;

    internal InMemoryStorageSnapshot(Dictionary<(string DatabaseName, string TableName), List<byte[]?>> tables)
    {
        _tables = new Dictionary<(string DatabaseName, string TableName), List<byte[]?>>(tables.Count);

        foreach (var entry in tables)
        {
            _tables[entry.Key] = CloneRows(entry.Value);
        }
    }

    internal IEnumerable<KeyValuePair<(string DatabaseName, string TableName), List<byte[]?>>> EnumerateTables()
    {
        foreach (var entry in _tables)
        {
            yield return new KeyValuePair<(string DatabaseName, string TableName), List<byte[]?>>(entry.Key, CloneRows(entry.Value));
        }
    }

    private static List<byte[]?> CloneRows(List<byte[]?> rows)
    {
        var clone = new List<byte[]?>(rows.Count);

        foreach (byte[]? row in rows)
        {
            clone.Add(row?.ToArray());
        }

        return clone;
    }
}
