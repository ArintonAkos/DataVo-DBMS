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
