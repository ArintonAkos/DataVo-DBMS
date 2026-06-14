using System.Collections;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// A read-only <see cref="IReadOnlyDictionary{TKey,TValue}"/> adapter over a typed row (schema + cells),
/// for legacy consumers during the typed-storage migration. Values are boxed on demand via
/// <see cref="CellValue.ToObject"/> — VECTOR cells return a clone — so a caller can never mutate stored
/// row state through this view. There is no mutation surface (read-only).
/// </summary>
/// <remarks>
/// Boxing on access makes this a transition bridge, not an endpoint: hot paths should consume the typed
/// row directly (<see cref="StoredRowView"/>) rather than this adapter.
/// </remarks>
internal sealed class StoredRowDictionaryView : IReadOnlyDictionary<string, object?>
{
    private readonly ReactiveRowSchema _schema;
    private readonly CellValue[] _cells;

    public StoredRowDictionaryView(ReactiveRowSchema schema, CellValue[] cells)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(cells);
        _schema = schema;
        _cells = cells;
    }

    public object? this[string key] =>
        TryGetValue(key, out object? value)
            ? value
            : throw new KeyNotFoundException($"Column '{key}' is not in the row schema.");

    public IEnumerable<string> Keys
    {
        get
        {
            for (int i = 0; i < _cells.Length; i++)
            {
                yield return _schema.ColumnAt(i);
            }
        }
    }

    public IEnumerable<object?> Values
    {
        get
        {
            for (int i = 0; i < _cells.Length; i++)
            {
                yield return _cells[i].ToObject();
            }
        }
    }

    public int Count => _cells.Length;

    public bool ContainsKey(string key) => _schema.TryGetOrdinal(key, out _);

    public bool TryGetValue(string key, out object? value)
    {
        if (_schema.TryGetOrdinal(key, out int ordinal))
        {
            value = _cells[ordinal].ToObject();
            return true;
        }

        value = null;
        return false;
    }

    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        for (int i = 0; i < _cells.Length; i++)
        {
            yield return new KeyValuePair<string, object?>(_schema.ColumnAt(i), _cells[i].ToObject());
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
