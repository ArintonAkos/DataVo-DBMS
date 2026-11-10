using System.Collections;

namespace DataVo.Core;

/// <summary>
/// The rows of a <see cref="DataVoTable{T}"/> that match a predicate. Nothing is read until the query is
/// enumerated or one of its methods is called, and it reads again each time.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <typeparam name="TState">The type of the value the predicate receives.</typeparam>
/// <example>
/// <code>
/// var active = db.Table&lt;Player&gt;().Where(minLevel, static (p, min) => p.Level >= min);
/// int howMany = active.Count();
/// Player? any = active.FirstOrDefault();
/// </code>
/// </example>
public readonly struct DataVoQuery<T, TState> : IEnumerable<T>
{
    private readonly DataVoTable<T> _table;
    private readonly TState _state;
    private readonly Func<T, TState, bool> _predicate;
    private readonly DataVoLinqPlan<T, TState>? _plan;

    internal DataVoQuery(DataVoTable<T> table, TState state, Func<T, TState, bool> predicate, DataVoLinqPlan<T, TState>? plan)
    {
        _table = table;
        _state = state;
        _predicate = predicate;
        _plan = plan;
    }

    /// <summary>
    /// Gets whether the query was compiled into a prepared statement at build time. When false, it reads the
    /// whole table and filters it with the predicate.
    /// </summary>
    public bool IsCompiled => _plan is not null;

    /// <summary>Returns the first matching row, or the default value when none matches.</summary>
    /// <returns>A matching row, or the default value.</returns>
    public T? FirstOrDefault() => _plan is null
        ? _table.FirstOrDefault(_state, _predicate)
        : _plan.FirstOrDefault(_table, _state, _predicate);

    /// <summary>Returns the only matching row, or the default value when none matches.</summary>
    /// <returns>The matching row, or the default value.</returns>
    /// <exception cref="InvalidOperationException">More than one row matches.</exception>
    public T? SingleOrDefault() => _plan is null
        ? _table.SingleOrDefault(_state, _predicate)
        : _plan.SingleOrDefault(_table, _state, _predicate);

    /// <summary>Gets whether any row matches.</summary>
    /// <returns><see langword="true"/> when at least one row matches.</returns>
    public bool Any()
    {
        Enumerator rows = Open(firstOnly: true);
        return rows.MoveNext();
    }

    /// <summary>Counts the matching rows.</summary>
    /// <returns>The number of matching rows.</returns>
    public int Count()
    {
        int count = 0;
        Enumerator rows = Open(firstOnly: false);
        while (rows.MoveNext())
        {
            count++;
        }

        return count;
    }

    /// <summary>Reads the matching rows into a list.</summary>
    /// <returns>The matching rows.</returns>
    public List<T> ToList()
    {
        var items = new List<T>();
        Enumerator rows = Open(firstOnly: false);
        while (rows.MoveNext())
        {
            items.Add(rows.Current);
        }

        return items;
    }

    /// <summary>Runs the query and enumerates the matching rows, building each one as it is reached.</summary>
    /// <returns>An enumerator over the matching rows.</returns>
    public Enumerator GetEnumerator() => Open(firstOnly: false);

    IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private Enumerator Open(bool firstOnly)
    {
        if (_table is null)
        {
            throw new InvalidOperationException("This query was not created by a DataVoTable.");
        }

        // Rows a plan returns already match; rows from a scan still need the predicate.
        return _plan?.Rows(_table, _state, firstOnly) is { } planned
            ? new Enumerator(_table, planned, _state, null)
            : new Enumerator(_table, _table.ScanRows(), _state, _predicate);
    }

    /// <summary>Enumerates a query's rows.</summary>
    public struct Enumerator : IEnumerator<T>
    {
        private readonly DataVoTable<T> _table;
        private readonly List<Dictionary<string, object?>> _rows;
        private readonly TState _state;
        private readonly Func<T, TState, bool>? _filter;
        private int _index;
        private T _current;

        internal Enumerator(DataVoTable<T> table, List<Dictionary<string, object?>> rows, TState state, Func<T, TState, bool>? filter)
        {
            _table = table;
            _rows = rows;
            _state = state;
            _filter = filter;
            _index = 0;
            _current = default!;
        }

        /// <summary>Gets the row the enumerator is on.</summary>
        public readonly T Current => _current;

        readonly object? IEnumerator.Current => _current;

        /// <summary>Advances to the next matching row.</summary>
        /// <returns><see langword="true"/> when there is another row.</returns>
        public bool MoveNext()
        {
            while (_index < _rows.Count)
            {
                T item = _table.Read(_rows[_index++]);
                if (_filter is null || _filter(item, _state))
                {
                    _current = item;
                    return true;
                }
            }

            _current = default!;
            return false;
        }

        /// <summary>Returns to before the first row.</summary>
        public void Reset()
        {
            _index = 0;
            _current = default!;
        }

        /// <summary>Releases nothing; the rows are already in memory.</summary>
        public readonly void Dispose()
        {
        }
    }
}
