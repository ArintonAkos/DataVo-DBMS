using System.Globalization;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table incremental DISTINCT operator.
/// </summary>
internal sealed class DistinctReactiveQuery : IReactiveQuery
{
    private sealed class TupleState
    {
        public required IReadOnlyDictionary<string, object?> Row { get; set; }
        public int Count { get; set; }
    }

    private readonly ReactivePredicate _predicate;
    private readonly bool _selectStar;
    private readonly List<string> _projection = [];
    private readonly Dictionary<string, TupleState> _tuples = new(StringComparer.Ordinal);

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

    public string Table { get; }

    public IReadOnlyCollection<string> Tables => [Table];

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

    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
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

    internal static string TupleKey(IReadOnlyDictionary<string, object?> row)
    {
        return string.Join(
            "",
            row.OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase)
                .Select(pair => EncodeComponent(pair.Key) + EncodeComponent(ValueKey(pair.Value))));
    }

    private static string EncodeComponent(string value) =>
        value.Length.ToString(CultureInfo.InvariantCulture) + ":" + value;

    private static string ValueKey(object? value)
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
