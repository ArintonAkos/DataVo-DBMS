namespace DataVo.Core.StorageEngine;

/// <summary>Conversions between the built-in engines' integer row ids and <see cref="RowKey"/>.</summary>
internal static class RowKeyConversions
{
    public static List<RowKey> ToRowKeys(List<long> rowIds)
    {
        var keys = new List<RowKey>(rowIds.Count);
        foreach (long rowId in rowIds)
        {
            keys.Add(rowId);
        }

        return keys;
    }

    // Boundaries to layers that still use integer row ids: every call marks a place that changes when
    // that layer moves to RowKey (grep for RowKeyConversions).
    public static long ToRowId(RowKey key) => (long)key;

    public static long[] ToRowIds(RowKey[] keys)
    {
        var rowIds = new long[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            rowIds[i] = (long)keys[i];
        }

        return rowIds;
    }

    public static List<long> ToRowIds(IEnumerable<RowKey> keys)
    {
        var rowIds = keys is ICollection<RowKey> collection ? new List<long>(collection.Count) : [];
        foreach (RowKey key in keys)
        {
            rowIds.Add((long)key);
        }

        return rowIds;
    }

    public static RowKey[] ToRowKeys(long[] rowIds)
    {
        var keys = new RowKey[rowIds.Length];
        for (int i = 0; i < rowIds.Length; i++)
        {
            keys[i] = rowIds[i];
        }

        return keys;
    }

    public static List<RowKey> ToRowKeys(IEnumerable<long> rowIds)
    {
        var keys = rowIds is ICollection<long> collection ? new List<RowKey>(collection.Count) : [];
        foreach (long rowId in rowIds)
        {
            keys.Add(rowId);
        }

        return keys;
    }

    public static List<long> ToRowIds(List<RowKey> keys)
    {
        var rowIds = new List<long>(keys.Count);
        foreach (RowKey key in keys)
        {
            rowIds.Add((long)key);
        }

        return rowIds;
    }

    public static List<(long NewRowId, byte[] RawRow)> ToRowIds(List<(RowKey NewRowId, byte[] RawRow)> rows)
    {
        var rowIds = new List<(long NewRowId, byte[] RawRow)>(rows.Count);
        foreach ((RowKey key, byte[] rawRow) in rows)
        {
            rowIds.Add(((long)key, rawRow));
        }

        return rowIds;
    }

    public static IEnumerable<(RowKey RowId, byte[] RawRow)> ToRowKeys(IEnumerable<(long RowId, byte[] RawRow)> rows)
    {
        foreach ((long rowId, byte[] rawRow) in rows)
        {
            yield return (rowId, rawRow);
        }
    }

    public static List<(RowKey NewRowId, byte[] RawRow)> ToRowKeys(List<(long NewRowId, byte[] RawRow)> rows)
    {
        var keys = new List<(RowKey NewRowId, byte[] RawRow)>(rows.Count);
        foreach ((long rowId, byte[] rawRow) in rows)
        {
            keys.Add((rowId, rawRow));
        }

        return keys;
    }
}

/// <summary>Exposes a built-in engine through the public <see cref="IStorageEngine"/> contract.</summary>
internal sealed class RowIdStorageEngineAdapter(IRowIdStorageEngine inner) : IStorageEngine
{
    public RowKey InsertRow(string databaseName, string tableName, byte[] rowBytes) => inner.InsertRow(databaseName, tableName, rowBytes);

    public List<RowKey> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes) =>
        RowKeyConversions.ToRowKeys(inner.InsertRows(databaseName, tableName, rowsBytes));

    public byte[] ReadRow(string databaseName, string tableName, RowKey rowId) => inner.ReadRow(databaseName, tableName, (long)rowId);

    public IEnumerable<(RowKey RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) =>
        RowKeyConversions.ToRowKeys(inner.ReadAllRows(databaseName, tableName));

    public void DeleteRow(string databaseName, string tableName, RowKey rowId) => inner.DeleteRow(databaseName, tableName, (long)rowId);

    public void DropTable(string databaseName, string tableName) => inner.DropTable(databaseName, tableName);

    public void DropDatabase(string databaseName) => inner.DropDatabase(databaseName);

    public List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) =>
        RowKeyConversions.ToRowKeys(inner.CompactTable(databaseName, tableName));
}
