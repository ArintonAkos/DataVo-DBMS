using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.Runtime.Changes;

/// <summary>
/// Represents a single committed row mutation with explicit before/after images.
/// </summary>
/// <remarks>
/// This is the value-oriented (record) view of a change, complementing the Z-set weight algebra
/// used internally by the incremental dataflow layer. Inserts carry only an after-image, deletes only a
/// before-image, and updates carry both. Each image is available typed (<see cref="TypedBefore"/> /
/// <see cref="TypedAfter"/>) and as an owned dictionary (<see cref="Before"/> / <see cref="After"/>); whichever
/// form the change was captured in, the other is built on first access and cached, so typed readers never pay
/// for dictionaries and dictionary readers never pay for typed cells.
/// </remarks>
public sealed class RowChange
{
    private IReadOnlyDictionary<string, object?>? _before;
    private IReadOnlyDictionary<string, object?>? _after;
    private TypedRow? _typedBefore;
    private TypedRow? _typedAfter;

    /// <summary>
    /// Initializes a new <see cref="RowChange"/> from dictionary images.
    /// </summary>
    /// <param name="table">The table the row belongs to.</param>
    /// <param name="rowId">The physical row identifier.</param>
    /// <param name="kind">The kind of mutation.</param>
    /// <param name="before">The row image prior to the change, or <c>null</c> for inserts.</param>
    /// <param name="after">The row image after the change, or <c>null</c> for deletes.</param>
    public RowChange(string table, RowKey rowId, ChangeKind kind,
        IReadOnlyDictionary<string, object?>? before,
        IReadOnlyDictionary<string, object?>? after)
        : this(table, rowId, kind, before, after, typedAfter: null)
    {
    }

    /// <summary>
    /// Initializes a new <see cref="RowChange"/> from dictionary images plus an optional typed after-image.
    /// </summary>
    /// <param name="table">The table the row belongs to.</param>
    /// <param name="rowId">The physical row identifier.</param>
    /// <param name="kind">The kind of mutation.</param>
    /// <param name="before">The row image prior to the change, or <c>null</c> for inserts.</param>
    /// <param name="after">The row image after the change, or <c>null</c> for deletes.</param>
    /// <param name="typedAfter">The typed row image after the change, or <c>null</c> to derive it from <paramref name="after"/>.</param>
    public RowChange(string table, RowKey rowId, ChangeKind kind,
        IReadOnlyDictionary<string, object?>? before,
        IReadOnlyDictionary<string, object?>? after,
        TypedRow? typedAfter)
    {
        Table = table;
        RowId = rowId;
        Kind = kind;
        _before = before;
        _after = after;
        _typedAfter = typedAfter;
    }

    /// <summary>
    /// Initializes an insert <see cref="RowChange"/> carrying only a typed after-image. The owned
    /// dictionary <see cref="After"/> is materialized lazily on first access from <paramref name="typedAfter"/>,
    /// so borrowed-only subscribers (which read <see cref="TypedAfter"/>) never pay for it.
    /// </summary>
    /// <param name="table">The table the row belongs to.</param>
    /// <param name="rowId">The physical row identifier.</param>
    /// <param name="typedAfter">The typed row image after the insert.</param>
    public RowChange(string table, RowKey rowId, TypedRow typedAfter)
        : this(table, rowId, ChangeKind.Insert, typedBefore: null, typedAfter)
    {
    }

    private RowChange(string table, RowKey rowId, ChangeKind kind, TypedRow? typedBefore, TypedRow? typedAfter)
    {
        Table = table;
        RowId = rowId;
        Kind = kind;
        _typedBefore = typedBefore;
        _typedAfter = typedAfter;
    }

    /// <summary>
    /// Creates a change from typed images only; the dictionary images are built lazily for owned readers.
    /// </summary>
    /// <param name="table">The table the row belongs to.</param>
    /// <param name="rowId">The physical row identifier.</param>
    /// <param name="kind">The kind of mutation.</param>
    /// <param name="typedBefore">The typed image prior to the change, or <c>null</c> for inserts.</param>
    /// <param name="typedAfter">The typed image after the change, or <c>null</c> for deletes.</param>
    /// <returns>The change.</returns>
    public static RowChange FromTyped(string table, RowKey rowId, ChangeKind kind, TypedRow? typedBefore, TypedRow? typedAfter)
        => new(table, rowId, kind, typedBefore, typedAfter);

    /// <summary>Gets the table the row belongs to.</summary>
    public string Table { get; }

    /// <summary>
    /// Gets the physical row identifier.
    /// </summary>
    /// <remarks>
    /// This is the storage slot id, <b>not</b> a stable logical identity: the engine performs
    /// out-of-place updates (an update is a physical delete + reinsert), which reassigns the row id.
    /// Reactive operators that need a stable per-row identity therefore key on the table's primary key
    /// (or on the before/after predicate result), never on this value.
    /// </remarks>
    public RowKey RowId { get; }

    /// <summary>Gets the kind of mutation.</summary>
    public ChangeKind Kind { get; }

    /// <summary>Gets the row image prior to the change, or <c>null</c> for inserts.</summary>
    /// <remarks>Built from <see cref="TypedBefore"/> on first access when the change was captured typed.</remarks>
    public IReadOnlyDictionary<string, object?>? Before => _before ??= Materialize(_typedBefore);

    /// <summary>Gets the row image after the change, or <c>null</c> for deletes.</summary>
    /// <remarks>
    /// For typed captures this dictionary is materialized lazily from <see cref="TypedAfter"/> on first access
    /// (and cached), so the borrowed typed lane never allocates it. Vector cells are cloned via
    /// <c>CellValue.ToObject()</c>, preserving the public-boundary safety of the eager path.
    /// </remarks>
    public IReadOnlyDictionary<string, object?>? After => _after ??= Materialize(_typedAfter);

    /// <summary>Gets the typed row image prior to the change, or <c>null</c> for inserts.</summary>
    /// <remarks>Built from <see cref="Before"/> on first access when the change was captured as a dictionary.</remarks>
    public TypedRow? TypedBefore => _typedBefore ??= TypedRow.FromImage(_before);

    /// <summary>Gets the typed row image after the change, or <c>null</c> for deletes.</summary>
    /// <remarks>Built from <see cref="After"/> on first access when the change was captured as a dictionary.</remarks>
    public TypedRow? TypedAfter => _typedAfter ??= TypedRow.FromImage(_after);

    private static IReadOnlyDictionary<string, object?>? Materialize(TypedRow? typed)
    {
        if (typed is not { } row)
        {
            return null;
        }

        var dict = new Dictionary<string, object?>(row.Schema.ColumnCount, StringComparer.OrdinalIgnoreCase);
        ReadOnlySpan<CellValue> cells = row.Cells.Span;
        for (int i = 0; i < cells.Length; i++)
        {
            dict[row.Schema.ColumnAt(i)] = cells[i].ToObject();
        }

        return dict;
    }
}
