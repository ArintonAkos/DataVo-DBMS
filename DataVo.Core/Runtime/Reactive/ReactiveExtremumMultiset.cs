namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Maintains duplicate-aware ordered values for reactive MIN/MAX aggregates.
/// </summary>
/// <remarks>
/// Values are typed cells: occurrence counts live in a hash map and the distinct values in an ordered set
/// (<see cref="CellValueComparer"/>, consistent with <see cref="CellValueEqualityComparer"/>), so adding or
/// removing another copy of a value already present touches only the map and allocates nothing.
/// </remarks>
internal sealed class ReactiveExtremumMultiset
{
    private readonly Dictionary<CellValue, long> _counts = new(CellValueEqualityComparer.Instance);
    private readonly SortedSet<CellValue> _distinctValues = new(CellValueComparer.Instance);

    /// <summary>Gets the current minimum value, or <see cref="CellValue.Null"/> when empty.</summary>
    public CellValue Min => _distinctValues.Count == 0 ? CellValue.Null : _distinctValues.Min;

    /// <summary>Gets the current maximum value, or <see cref="CellValue.Null"/> when empty.</summary>
    public CellValue Max => _distinctValues.Count == 0 ? CellValue.Null : _distinctValues.Max;

    /// <summary>Adds one occurrence of <paramref name="value"/>.</summary>
    public void Add(CellValue value)
    {
        if (_counts.TryGetValue(value, out long count))
        {
            _counts[value] = count + 1;
            return;
        }

        _counts[value] = 1;
        _distinctValues.Add(value);
    }

    /// <summary>Removes one occurrence of <paramref name="value"/> when present.</summary>
    public void Remove(CellValue value)
    {
        if (!_counts.TryGetValue(value, out long count))
        {
            return;
        }

        if (count <= 1)
        {
            _counts.Remove(value);
            _distinctValues.Remove(value);
            return;
        }

        _counts[value] = count - 1;
    }
}
