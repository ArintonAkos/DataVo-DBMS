using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.Runtime.Changes;

/// <summary>
/// Represents a single committed row mutation with explicit before/after images.
/// </summary>
/// <remarks>
/// This is the value-oriented (record) view of a change, complementing the Z-set weight algebra
/// used internally by the incremental dataflow layer. Inserts carry only <see cref="After"/>,
/// deletes only <see cref="Before"/>, and updates carry both.
/// </remarks>
public sealed class RowChange
{
    /// <summary>
    /// Initializes a new <see cref="RowChange"/>.
    /// </summary>
    /// <param name="table">The table the row belongs to.</param>
    /// <param name="rowId">The physical row identifier.</param>
    /// <param name="kind">The kind of mutation.</param>
    /// <param name="before">The row image prior to the change, or <c>null</c> for inserts.</param>
    /// <param name="after">The row image after the change, or <c>null</c> for deletes.</param>
    public RowChange(string table, long rowId, ChangeKind kind,
        IReadOnlyDictionary<string, object?>? before,
        IReadOnlyDictionary<string, object?>? after)
        : this(table, rowId, kind, before, after, typedAfter: null)
    {
    }

    /// <summary>
    /// Initializes a new <see cref="RowChange"/>.
    /// </summary>
    /// <param name="table">The table the row belongs to.</param>
    /// <param name="rowId">The physical row identifier.</param>
    /// <param name="kind">The kind of mutation.</param>
    /// <param name="before">The row image prior to the change, or <c>null</c> for inserts.</param>
    /// <param name="after">The row image after the change, or <c>null</c> for deletes.</param>
    /// <param name="typedAfter">The typed row image after the change, or <c>null</c> for dictionary paths and deletes.</param>
    public RowChange(string table, long rowId, ChangeKind kind,
        IReadOnlyDictionary<string, object?>? before,
        IReadOnlyDictionary<string, object?>? after,
        TypedRow? typedAfter)
    {
        Table = table;
        RowId = rowId;
        Kind = kind;
        Before = before;
        _after = after;
        TypedAfter = typedAfter;
    }

    /// <summary>
    /// Initializes an insert <see cref="RowChange"/> carrying only a typed after-image. The owned
    /// dictionary <see cref="After"/> is materialized lazily on first access from <paramref name="typedAfter"/>,
    /// so borrowed-only subscribers (which read <see cref="TypedAfter"/>) never pay for it.
    /// </summary>
    /// <param name="table">The table the row belongs to.</param>
    /// <param name="rowId">The physical row identifier.</param>
    /// <param name="typedAfter">The typed row image after the insert.</param>
    public RowChange(string table, long rowId, TypedRow typedAfter)
    {
        Table = table;
        RowId = rowId;
        Kind = ChangeKind.Insert;
        Before = null;
        _after = null; // built on first owned read from TypedAfter
        TypedAfter = typedAfter;
    }

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
    public long RowId { get; }

    /// <summary>Gets the kind of mutation.</summary>
    public ChangeKind Kind { get; }

    /// <summary>Gets the row image prior to the change, or <c>null</c> for inserts.</summary>
    public IReadOnlyDictionary<string, object?>? Before { get; }

    private IReadOnlyDictionary<string, object?>? _after;

    /// <summary>Gets the row image after the change, or <c>null</c> for deletes.</summary>
    /// <remarks>
    /// For typed inserts captured with the <see cref="RowChange(string, long, TypedRow)"/> constructor,
    /// this dictionary is materialized lazily from <see cref="TypedAfter"/> on first access (and cached),
    /// so the borrowed typed lane never allocates it. Vector cells are cloned via
    /// <c>CellValue.ToObject()</c>, preserving the public-boundary safety of the eager path.
    /// </remarks>
    public IReadOnlyDictionary<string, object?>? After
    {
        get
        {
            if (_after is null && TypedAfter is { } typed)
            {
                var dict = new Dictionary<string, object?>(typed.Schema.ColumnCount, StringComparer.OrdinalIgnoreCase);
                ReadOnlySpan<CellValue> cells = typed.Cells.Span;
                for (int i = 0; i < cells.Length; i++)
                {
                    dict[typed.Schema.ColumnAt(i)] = cells[i].ToObject();
                }

                _after = dict;
            }

            return _after;
        }
    }

    /// <summary>Gets the typed row image after the change, or <c>null</c> for dictionary paths and deletes.</summary>
    public TypedRow? TypedAfter { get; }
}
