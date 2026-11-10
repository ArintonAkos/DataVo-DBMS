namespace DataVo.Core.StorageEngine;

/// <summary>Visits one row of a page: its serialized bytes, or a typed row (then the bytes are empty).</summary>
/// <remarks>The bytes are only valid during the call.</remarks>
internal delegate void PagedRowVisitor<TState>(ref TState state, long rowId, ReadOnlySpan<byte> raw, StoredRow? typed);

/// <summary>How a page read ended.</summary>
/// <param name="ReachedEnd">Whether the scan has no more rows to examine.</param>
/// <param name="Generation">Identifies the table's physical layout; a resumed scan must see the same value.</param>
internal readonly record struct PageScan(bool ReachedEnd, long Generation);

/// <summary>
/// Reads a table in bounded pages. Locks are held only while one page is read, so a slow reader never blocks
/// writers between pages.
/// </summary>
internal interface IPagedRowScan
{
    /// <summary>Visits up to <paramref name="maxRows"/> live rows after a position, in row id order, without copying them.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The table to read.</param>
    /// <param name="afterRowId">The last row already read, or <see langword="null"/> to start.</param>
    /// <param name="expectedGeneration">The layout generation the scan started on, or <see langword="null"/> on its first page.</param>
    /// <param name="maxRows">The most rows to visit.</param>
    /// <param name="state">The visitor's state.</param>
    /// <param name="visit">Called once per live row while the table's lock is held.</param>
    /// <returns>Whether the table was read to its end, and its layout generation.</returns>
    /// <exception cref="InvalidOperationException">The table was rewritten since <paramref name="expectedGeneration"/>.</exception>
    PageScan VisitPage<TState>(
        string databaseName,
        string tableName,
        long? afterRowId,
        long? expectedGeneration,
        int maxRows,
        ref TState state,
        PagedRowVisitor<TState> visit);
}

internal static class PagedRowScanErrors
{
    public static InvalidOperationException Rewritten(string tableName) =>
        new($"Table {tableName} was rewritten while it was being streamed.");
}
