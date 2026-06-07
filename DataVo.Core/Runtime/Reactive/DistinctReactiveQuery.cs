using System.Globalization;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table incremental <c>SELECT DISTINCT</c> operator (L4).
/// </summary>
/// <remarks>
/// DISTINCT is the DBSP <c>distinct</c> operator: a tuple is in the result iff its underlying
/// multiplicity is positive. This operator keeps a per-distinct-tuple <b>count</b> (the projected row's
/// multiplicity). A committed batch is folded into signed per-tuple deltas (Insert→+1, Delete→−1,
/// Update→−before/+after over the projection), and a tuple is emitted as <c>Added</c> only on the
/// <c>0 → &gt;0</c> count transition and as <c>Removed</c> only on the <c>&gt;0 → 0</c> transition;
/// intermediate count changes that keep the tuple present produce no output. This is what makes the
/// result a true set even though the source carries duplicates. The state is keyed by a canonical,
/// order-independent <see cref="TupleKey"/> of the projected row, so it is unaffected by the engine's
/// out-of-place <c>UPDATE</c> row-id reassignment.
/// </remarks>
internal sealed class DistinctReactiveQuery : IReactiveQuery
{
    /// <summary>The maintained state for one distinct projected tuple: its image and current multiplicity.</summary>
    private sealed class TupleState
    {
        public required IReadOnlyDictionary<string, object?> Row { get; set; }
        public int Count { get; set; }
    }

    private readonly ReactivePredicate _predicate;
    private readonly bool _selectStar;
    private readonly List<string> _projection = [];
    private readonly Dictionary<string, TupleState> _tuples = new(StringComparer.Ordinal);

    /// <summary>
    /// Compiles the supplied parsed <c>SELECT DISTINCT</c> into an incremental distinct operator.
    /// </summary>
    /// <param name="select">The parsed single-table <c>SELECT DISTINCT</c>.</param>
    /// <exception cref="NotSupportedException">Thrown for a shape outside the supported DISTINCT subset (joins, grouping, ordering, computed projections, and so on).</exception>
    public DistinctReactiveQuery(SelectStatement select)
    {
        Validate(select);

        Table = select.FromTable!.Name;
        _predicate = ReactivePredicate.Compile(select.WhereExpression);

        foreach (SelectColumnNode column in select.Columns)
        {
            if (IsStarColumn(column))
            {
                _selectStar = true;
                continue;
            }

            _projection.Add(ResolveProjectedColumnName(column));
        }

        if (_projection.Count == 0)
        {
            _selectStar = true;
        }
    }

    /// <summary>Gets the source table this distinct operator observes.</summary>
    public string Table { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [Table];

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach ((long _, IReadOnlyDictionary<string, object?> row) in rows)
        {
            if (_predicate.Matches(row))
            {
                AddSeedTuple(Project(row));
            }
        }
    }

    /// <inheritdoc />
    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
        // Fold the batch into a single signed delta per distinct projected tuple, then apply each delta
        // to the maintained count and emit only the boundary (0<->positive) transitions.
        Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Delta)> deltas = new(StringComparer.Ordinal);

        foreach (RowChange change in changes)
        {
            if (change.Before is not null && change.Kind is ChangeKind.Delete or ChangeKind.Update && _predicate.Matches(change.Before))
            {
                AddDelta(deltas, Project(change.Before), -1);
            }

            if (change.After is not null && change.Kind is ChangeKind.Insert or ChangeKind.Update && _predicate.Matches(change.After))
            {
                AddDelta(deltas, Project(change.After), +1);
            }
        }

        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];

        foreach ((string key, (IReadOnlyDictionary<string, object?> row, int delta)) in deltas)
        {
            _tuples.TryGetValue(key, out TupleState? state);
            int oldCount = state?.Count ?? 0;
            int newCount = oldCount + delta;

            if (oldCount <= 0 && newCount > 0)
            {
                _tuples[key] = new TupleState { Row = row, Count = newCount };
                added.Add(row);
            }
            else if (oldCount > 0 && newCount <= 0)
            {
                _tuples.Remove(key);
                removed.Add(state!.Row);
            }
            else if (newCount > 0 && state is not null)
            {
                state.Count = newCount;
                state.Row = row;
            }
        }

        return new QueryChange(added, removed, []);
    }

    private void AddSeedTuple(IReadOnlyDictionary<string, object?> row)
    {
        string key = TupleKey(row);
        if (_tuples.TryGetValue(key, out TupleState? state))
        {
            state.Count++;
            return;
        }

        _tuples[key] = new TupleState { Row = row, Count = 1 };
    }

    private static void AddDelta(
        Dictionary<string, (IReadOnlyDictionary<string, object?> Row, int Delta)> deltas,
        IReadOnlyDictionary<string, object?> row,
        int weight)
    {
        string key = TupleKey(row);
        if (deltas.TryGetValue(key, out (IReadOnlyDictionary<string, object?> Row, int Delta) existing))
        {
            deltas[key] = (row, existing.Delta + weight);
            return;
        }

        deltas[key] = (row, weight);
    }

    private IReadOnlyDictionary<string, object?> Project(IReadOnlyDictionary<string, object?> row)
    {
        if (_selectStar)
        {
            return Copy(row);
        }

        var projected = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (string column in _projection)
        {
            projected[column] = row.TryGetValue(column, out object? value) ? value : null;
        }

        return projected;
    }

    private static IReadOnlyDictionary<string, object?> Copy(IReadOnlyDictionary<string, object?> row) =>
        new Dictionary<string, object?>(
            row.ToDictionary(pair => pair.Key, pair => pair.Value),
            StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Computes a canonical, collision-resistant string key for a projected row, used as the identity of
    /// a distinct tuple. Columns are sorted by name so key equality is independent of dictionary order,
    /// and each name and value component is length-prefixed so distinct content can never alias. Shared
    /// by the UNION and recursive-CTE operators, which key their multiplicity/set state the same way.
    /// </summary>
    internal static string TupleKey(IReadOnlyDictionary<string, object?> row)
    {
        return string.Join(
            "",
            row.OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase)
                .Select(pair => EncodeComponent(pair.Key) + EncodeComponent(ValueKey(pair.Value))));
    }

    private static string EncodeComponent(string value) =>
        value.Length.ToString(CultureInfo.InvariantCulture) + ":" + value;

    /// <summary>
    /// Encodes a single scalar value as a type-tagged, culture-invariant string component for use in a
    /// tuple/correlation/identity key. The type tag prevents cross-type aliasing (for example the string
    /// <c>"1"</c> versus the integer <c>1</c>), and the invariant formatting keeps keys stable across
    /// locales. Shared by every operator that keys state by value (subqueries, recursive CTEs).
    /// </summary>
    internal static string ValueKey(object? value)
    {
        if (value is null)
        {
            return "null:";
        }

        return value switch
        {
            string s => "string:" + s,
            char c => "char:" + c,
            bool b => "bool:" + (b ? "1" : "0"),
            byte v => "byte:" + v.ToString(CultureInfo.InvariantCulture),
            sbyte v => "sbyte:" + v.ToString(CultureInfo.InvariantCulture),
            short v => "short:" + v.ToString(CultureInfo.InvariantCulture),
            ushort v => "ushort:" + v.ToString(CultureInfo.InvariantCulture),
            int v => "int:" + v.ToString(CultureInfo.InvariantCulture),
            uint v => "uint:" + v.ToString(CultureInfo.InvariantCulture),
            long v => "long:" + v.ToString(CultureInfo.InvariantCulture),
            ulong v => "ulong:" + v.ToString(CultureInfo.InvariantCulture),
            float v => "float:" + v.ToString(CultureInfo.InvariantCulture),
            double v => "double:" + v.ToString(CultureInfo.InvariantCulture),
            decimal v => "decimal:" + v.ToString(CultureInfo.InvariantCulture),
            DateOnly v => "dateonly:" + v.ToString("O", CultureInfo.InvariantCulture),
            DateTime v => "datetime:" + v.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
            DateTimeOffset v => "datetimeoffset:" + v.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
            Guid v => "guid:" + v.ToString(),
            byte[] bytes => "bytes:" + Convert.ToBase64String(bytes),
            _ => value.GetType().FullName + ":" + Convert.ToString(value, CultureInfo.InvariantCulture)
        };
    }

    private static void Validate(SelectStatement select)
    {
        if (!select.IsDistinct)
        {
            throw new NotSupportedException("Reactive DISTINCT requires SELECT DISTINCT.");
        }

        if (select.FromTable is null)
        {
            throw new NotSupportedException("Reactive DISTINCT requires a single FROM table.");
        }

        if (select.Joins.Count > 0)
        {
            throw new NotSupportedException("Reactive DISTINCT does not support JOINs.");
        }

        if (select.GroupByExpression is not null || select.HavingExpression is not null)
        {
            throw new NotSupportedException("Reactive DISTINCT does not support GROUP BY / HAVING.");
        }

        if (select.OrderByExpression is not null || select.LimitExpression is not null)
        {
            throw new NotSupportedException("Reactive DISTINCT does not support ORDER BY / LIMIT.");
        }

        if (select.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive DISTINCT does not support common table expressions.");
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (column.Expression is AggregateExpressionNode || column.Expression is WindowFunctionExpressionNode)
            {
                throw new NotSupportedException("Reactive DISTINCT does not support aggregate or window functions.");
            }

            if (column.RawExpression.Contains('(') && !IsStarColumn(column))
            {
                throw new NotSupportedException($"Reactive DISTINCT does not support computed projection '{column.RawExpression}'.");
            }
        }
    }

    private static bool IsStarColumn(SelectColumnNode column)
    {
        return column.RawExpression.Trim() == "*"
            || (column.Expression is null && column.RawExpression.Trim().EndsWith("*", StringComparison.Ordinal));
    }

    private static string ResolveProjectedColumnName(SelectColumnNode column)
    {
        if (column.Expression is ColumnRefNode columnRef)
        {
            return columnRef.Column;
        }

        if (column.Expression is ResolvedColumnRefNode resolved)
        {
            return resolved.Column;
        }

        string raw = column.RawExpression.Trim();
        int dot = raw.LastIndexOf('.');
        return dot >= 0 ? raw[(dot + 1)..] : raw;
    }
}
