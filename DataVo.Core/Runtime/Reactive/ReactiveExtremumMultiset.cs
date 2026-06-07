namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Maintains duplicate-aware ordered values for reactive MIN/MAX aggregates.
/// </summary>
internal sealed class ReactiveExtremumMultiset
{
    private readonly SortedDictionary<object, long> _counts = new(ReactiveValueComparer.Instance);
    private readonly SortedSet<object> _distinctValues = new(ReactiveValueComparer.Instance);

    /// <summary>Gets the current minimum value, or <c>null</c> when empty.</summary>
    public object? Min => _distinctValues.Count == 0 ? null : _distinctValues.Min;

    /// <summary>Gets the current maximum value, or <c>null</c> when empty.</summary>
    public object? Max => _distinctValues.Count == 0 ? null : _distinctValues.Max;

    /// <summary>Adds one occurrence of <paramref name="value"/>.</summary>
    public void Add(object value)
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
    public void Remove(object value)
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
