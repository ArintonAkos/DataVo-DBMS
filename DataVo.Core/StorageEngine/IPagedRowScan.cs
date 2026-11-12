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
/// <summary>
/// Receives the rows of <see cref="IPagedRowScan.ScanPage{TVisitor}"/>. Implementations are structs, and the scan
/// is generic over the struct, so the visitor's code is compiled into the engine's row loop: no delegate is
/// called per row.
/// </summary>
internal interface IPagedRowVisitor
{
    /// <summary>Receives one live row: serialized bytes, or a typed row from a typed backend.</summary>
    void Visit(long rowId, ReadOnlySpan<byte> raw, StoredRow? typed);

    /// <summary>
    /// The length of this visitor's row image records, or 0 when it does not read images. A backend that keeps
    /// rows as scattered objects may keep such records side by side and hand them to <see cref="VisitImage"/>
    /// instead of calling <see cref="Visit"/>.
    /// </summary>
    int ImageBytes { get; }

    /// <summary>Identifies the column layout <see cref="EncodeImage"/> writes; records of another version are stale.</summary>
    int ImageVersion { get; }

    /// <summary>Writes one row as an image record of <see cref="ImageBytes"/> bytes.</summary>
    void EncodeImage(ReadOnlySpan<byte> raw, StoredRow? typed, Span<byte> record);

    /// <summary>Receives one live row as the image record <see cref="EncodeImage"/> wrote for it.</summary>
    void VisitImage(long rowId, ReadOnlySpan<byte> record);
}

/// <summary>Runs a <see cref="PagedRowVisitor{TState}"/> delegate as a struct visitor.</summary>
internal struct DelegatePagedRowVisitor<TState> : IPagedRowVisitor
{
    private readonly PagedRowVisitor<TState> _visit;

    public TState State;

    public DelegatePagedRowVisitor(TState state, PagedRowVisitor<TState> visit)
    {
        State = state;
        _visit = visit;
    }

    public void Visit(long rowId, ReadOnlySpan<byte> raw, StoredRow? typed) => _visit(ref State, rowId, raw, typed);

    public readonly int ImageBytes => 0;

    public readonly int ImageVersion => 0;

    public readonly void EncodeImage(ReadOnlySpan<byte> raw, StoredRow? typed, Span<byte> record) => throw new NotSupportedException();

    public readonly void VisitImage(long rowId, ReadOnlySpan<byte> record) => throw new NotSupportedException();
}

internal interface IPagedRowScan
{
    /// <summary>
    /// Visits up to <paramref name="maxRows"/> live rows after <paramref name="afterRowId"/>, like
    /// <see cref="VisitPage{TState}"/>, with the visitor compiled into the scan loop.
    /// </summary>
    /// <exception cref="InvalidOperationException">The table was rewritten since <paramref name="expectedGeneration"/>.</exception>
    PageScan ScanPage<TVisitor>(
        string databaseName,
        string tableName,
        long? afterRowId,
        long? expectedGeneration,
        int maxRows,
        ref TVisitor visitor)
        where TVisitor : struct, IPagedRowVisitor;

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
