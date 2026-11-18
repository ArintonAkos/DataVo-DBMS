namespace DataVo.Core;

/// <summary>What one <see cref="DataVoWriteBatch"/> operation does.</summary>
public enum DataVoBatchOperation
{
    /// <summary>Inserts the row, or sets the layout's columns of the row with the same primary key.</summary>
    Upsert,

    /// <summary>Deletes the row with the primary key the values carry; nothing happens when there is none.</summary>
    Delete,
}

/// <summary>
/// An ordered list of primary-key upserts and deletes that <see cref="DataVoContext.ApplyBatch"/> writes as one
/// atomic unit, without SQL.
/// </summary>
/// <remarks>
/// <para>
/// Values are positional: each operation takes one value per <see cref="DataVoBatchLayout.Columns"/> entry.
/// The batch copies them into one internal buffer, so the caller's span can be reused right away. A delete
/// reads only the primary-key columns of its values.
/// </para>
/// <para>
/// Operations on the same row combine in order before anything is written: later upserts overwrite earlier
/// values, a delete cancels earlier upserts, and an upsert after a delete starts from the column defaults. A
/// batch can mix tables of one database, which lets a caller store its own bookkeeping row (such as a resume
/// position) in the same commit as the data. Reuse a batch with <see cref="Clear"/>. Not thread-safe.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var players = new DataVoBatchLayout("Players", ["Id", "Name", "Hp"]);
/// var batch = new DataVoWriteBatch();
/// batch.Upsert(players, 1, "Ann", 100);
/// batch.Upsert(players, 2, "Bob", 80);
/// batch.Delete(players, 3, null, null);
/// DataVoBatchResult result = context.ApplyBatch(batch);
/// </code>
/// </example>
public sealed class DataVoWriteBatch
{
    private readonly List<Entry> _entries = [];
    private object?[] _values = new object?[64];
    private int _valueCount;

    /// <summary>Gets the number of operations in the batch.</summary>
    public int Count => _entries.Count;

    /// <summary>Adds an upsert.</summary>
    /// <param name="layout">The table and the columns <paramref name="values"/> carry.</param>
    /// <param name="values">One value per layout column, in layout order.</param>
    /// <exception cref="ArgumentException">The number of values does not match the layout.</exception>
    public void Upsert(DataVoBatchLayout layout, params ReadOnlySpan<object?> values) =>
        Add(layout, DataVoBatchOperation.Upsert, values);

    /// <summary>Adds a delete.</summary>
    /// <param name="layout">The table and the columns <paramref name="values"/> carry; it must list the primary key.</param>
    /// <param name="values">One value per layout column; only the primary-key values are read.</param>
    /// <exception cref="ArgumentException">The number of values does not match the layout.</exception>
    public void Delete(DataVoBatchLayout layout, params ReadOnlySpan<object?> values) =>
        Add(layout, DataVoBatchOperation.Delete, values);

    /// <summary>Removes every operation and releases the values, keeping the buffers for reuse.</summary>
    public void Clear()
    {
        Array.Clear(_values, 0, _valueCount);
        _valueCount = 0;
        _entries.Clear();
    }

    /// <summary>Gets the operation at <paramref name="index"/>.</summary>
    internal Entry this[int index] => _entries[index];

    /// <summary>Gets the values of an operation.</summary>
    internal ReadOnlySpan<object?> ValuesOf(Entry entry) =>
        _values.AsSpan(entry.Offset, entry.Layout.Columns.Count);

    private void Add(DataVoBatchLayout layout, DataVoBatchOperation operation, ReadOnlySpan<object?> values)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(layout);
        if (values.Length != layout.Columns.Count)
        {
            throw new ArgumentException(
                $"The layout for {layout.TableName} has {layout.Columns.Count} columns but {values.Length} values were given.",
                nameof(values));
        }

        if (_valueCount + values.Length > _values.Length)
        {
            Array.Resize(ref _values, Math.Max(_values.Length * 2, _valueCount + values.Length));
        }

        values.CopyTo(_values.AsSpan(_valueCount));
        _entries.Add(new Entry(layout, operation, _valueCount));
        _valueCount += values.Length;
    }

    /// <summary>One operation: its layout, kind and where its values start in the shared buffer.</summary>
    internal readonly record struct Entry(DataVoBatchLayout Layout, DataVoBatchOperation Operation, int Offset);
}

/// <summary>What <see cref="DataVoContext.ApplyBatch"/> did to the stored rows.</summary>
/// <param name="Inserted">Rows that did not exist and were inserted.</param>
/// <param name="Updated">Existing rows whose values changed.</param>
/// <param name="Deleted">Existing rows that were deleted.</param>
/// <param name="Unchanged">
/// Rows the batch left as they were: an upsert that matched the stored values, or a delete of a missing row.
/// Re-applying a batch therefore reports every row here.
/// </param>
public readonly record struct DataVoBatchResult(int Inserted, int Updated, int Deleted, int Unchanged);
