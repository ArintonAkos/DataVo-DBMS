namespace DataVo.Core.StorageEngine;

/// <summary>A bounded page of rows from a resumable table scan.</summary>
/// <param name="Rows">The live rows, in key order. Each has either serialized bytes or a typed row.</param>
/// <param name="ReachedEnd">Whether the scan has no more rows to examine.</param>
/// <param name="Generation">Identifies the table's physical layout; a resumed scan must see the same value.</param>
internal readonly record struct RowPage(List<(long RowId, byte[]? Raw, StoredRow? Typed)> Rows, bool ReachedEnd, long Generation);

/// <summary>
/// Reads a table in bounded pages. Locks are held only while one page is read, so a slow reader never blocks
/// writers between pages.
/// </summary>
internal interface IPagedRowScan
{
    /// <summary>Reads up to <paramref name="maxRows"/> live rows after a position.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The table to read.</param>
    /// <param name="afterRowId">The last row already read, or <see langword="null"/> to start.</param>
    /// <param name="maxRows">The largest page to return.</param>
    /// <returns>The next page.</returns>
    RowPage ReadPage(string databaseName, string tableName, long? afterRowId, int maxRows);
}
