using DataVo.Core.StorageEngine;

namespace DataVo.Core.StorageEngine.Memory;

internal sealed class InMemoryStorageSnapshot
{
    private readonly Dictionary<(string DatabaseName, string TableName), List<object?>> _tables;

    internal InMemoryStorageSnapshot(Dictionary<(string DatabaseName, string TableName), List<object?>> tables)
    {
        _tables = new Dictionary<(string DatabaseName, string TableName), List<object?>>(tables.Count);

        foreach (var entry in tables)
        {
            _tables[entry.Key] = CloneRows(entry.Value);
        }
    }

    internal IEnumerable<KeyValuePair<(string DatabaseName, string TableName), List<object?>>> EnumerateTables()
    {
        foreach (var entry in _tables)
        {
            yield return new KeyValuePair<(string DatabaseName, string TableName), List<object?>>(entry.Key, CloneRows(entry.Value));
        }
    }

    private static List<object?> CloneRows(List<object?> rows)
    {
        var clone = new List<object?>(rows.Count);

        foreach (object? row in rows)
        {
            clone.Add(row switch
            {
                byte[] bytes => bytes.ToArray(),
                StoredRow typed => typed.Clone(),
                _ => null
            });
        }

        return clone;
    }
}
