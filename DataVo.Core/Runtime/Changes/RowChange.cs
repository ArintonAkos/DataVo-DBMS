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
    {
        Table = table;
        RowId = rowId;
        Kind = kind;
        Before = before;
        After = after;
    }

    /// <summary>Gets the table the row belongs to.</summary>
    public string Table { get; }

    /// <summary>Gets the physical row identifier.</summary>
    public long RowId { get; }

    /// <summary>Gets the kind of mutation.</summary>
    public ChangeKind Kind { get; }

    /// <summary>Gets the row image prior to the change, or <c>null</c> for inserts.</summary>
    public IReadOnlyDictionary<string, object?>? Before { get; }

    /// <summary>Gets the row image after the change, or <c>null</c> for deletes.</summary>
    public IReadOnlyDictionary<string, object?>? After { get; }
}
