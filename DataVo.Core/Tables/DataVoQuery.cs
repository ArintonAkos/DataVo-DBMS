using System.Collections;

namespace DataVo.Core;

/// <summary>
/// The rows of a <see cref="DataVoTable{T}"/> that match a predicate. Nothing is read until the query is
/// enumerated or one of its methods is called, and it reads again each time. Enumeration streams: rows are read a
/// page at a time, never all at once.
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
        if (_plan is not null && _plan.Binds(_state))
        {
            return _plan.Any(Table, _state);
        }

        using Enumerator rows = GetEnumerator();
        return rows.MoveNext();
    }

    /// <summary>Counts the matching rows without keeping them.</summary>
    /// <returns>The number of matching rows.</returns>
    public int Count()
    {
        if (_plan is not null && _plan.Binds(_state) && _plan.TryCountMatching(Table, _state, out int matching))
        {
            return matching;
        }

        using Enumerator rows = GetEnumerator();
        return rows.CountRemaining();
    }

    /// <summary>Reads the matching rows into a list.</summary>
    /// <returns>The matching rows.</returns>
    public List<T> ToList()
    {
        var items = new List<T>();
        using Enumerator rows = GetEnumerator();
        while (rows.MoveNext())
        {
            items.Add(rows.Current);
        }

        return items;
    }

    /// <summary>
    /// Runs the query and enumerates the matching rows. Rows are read a page at a time and each is built as it
    /// is reached, so memory stays flat however many rows match.
    /// </summary>
    /// <returns>An enumerator over the matching rows.</returns>
    /// <remarks>
    /// No lock is held between pages, so changes committed during enumeration may or may not appear. A query that
    /// pins the primary key reads its one row directly. Inside a transaction, for an equality on an indexed column,
    /// and for conditions the paged scan cannot apply (<c>||</c>, a null test, or <c>!=</c> on a nullable column),
    /// the matching rows are read first and then enumerated.
    /// </remarks>
    public Enumerator GetEnumerator()
    {
        DataVoTable<T> table = Table;
        if (_plan is null || !_plan.Binds(_state))
        {
            // Rows from an unfiltered scan still need the predicate.
            return new Enumerator(table, table.Scan().GetEnumerator(), _state, _predicate);
        }

        if (_plan.IsUnique)
        {
            return _plan.TryFirst(table, _state, borrowed: false, out T item) ? new Enumerator(item) : default;
        }

        return new Enumerator(table, _plan.Stream(table, _state).GetEnumerator(), _state, null);
    }

    IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private DataVoTable<T> Table => _table ?? throw new InvalidOperationException("This query was not created by a DataVoTable.");

    /// <summary>Enumerates a query's rows.</summary>
    public struct Enumerator : IEnumerator<T>
    {
        private readonly DataVoTable<T>? _table;
        private readonly IEnumerator<DataVoRow>? _rows;
        private readonly TState _state;
        private readonly Func<T, TState, bool>? _filter;
        private bool _pendingSingle;
        private T _single;
        private T _current;

        internal Enumerator(DataVoTable<T> table, IEnumerator<DataVoRow> rows, TState state, Func<T, TState, bool>? filter)
        {
            _table = table;
            _rows = rows;
            _state = state;
            _filter = filter;
            _pendingSingle = false;
            _single = default!;
            _current = default!;
        }

        internal Enumerator(T single)
        {
            _table = null;
            _rows = null;
            _state = default!;
            _filter = null;
            _pendingSingle = true;
            _single = single;
            _current = default!;
        }

        /// <summary>Gets the row the enumerator is on.</summary>
        public readonly T Current => _current;

        readonly object? IEnumerator.Current => _current;

        /// <summary>Advances to the next matching row.</summary>
        /// <returns><see langword="true"/> when there is another row.</returns>
        public bool MoveNext()
        {
            if (_pendingSingle)
            {
                _pendingSingle = false;
                _current = _single;
                _single = default!;
                return true;
            }

            while (_rows is not null && _rows.MoveNext())
            {
                T item = _table!.Read(_rows.Current);
                if (_filter is null || _filter(item, _state))
                {
                    _current = item;
                    return true;
                }
            }

            _current = default!;
            return false;
        }

        /// <summary>Not supported; enumerate the query again instead.</summary>
        /// <exception cref="NotSupportedException">Always.</exception>
        public readonly void Reset() => throw new NotSupportedException();

        /// <summary>Ends the scan.</summary>
        public readonly void Dispose() => _rows?.Dispose();

        /// <summary>Counts the rows left, building a model only when the predicate has to see it.</summary>
        internal int CountRemaining()
        {
            int count = 0;
            if (_filter is not null || _rows is null)
            {
                while (MoveNext())
                {
                    count++;
                }

                return count;
            }

            while (_rows.MoveNext())
            {
                count++;
            }

            return count;
        }
    }
}
