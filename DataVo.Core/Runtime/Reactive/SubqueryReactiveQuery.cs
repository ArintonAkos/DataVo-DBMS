using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Incremental IN/EXISTS subquery operator implemented as a semi/anti-join.
/// </summary>
/// <remarks>
/// Two shapes are maintained behind the same routing. An <em>uncorrelated</em> subquery tracks a single
/// global membership index (a per-IN-value count, or a single EXISTS count). A <em>correlated</em>
/// subquery (<c>… WHERE S.k = R.k [AND &lt;inner pred&gt;]</c>) indexes the inner relation by the
/// correlation key, so a change to <c>S</c> re-evaluates only the outer rows whose correlation key was
/// affected.
/// </remarks>
internal sealed class SubqueryReactiveQuery : IReactiveQuery
{
    private readonly SubqueryShape _shape;
    private readonly ReactivePredicate _innerPredicate;
    private readonly IReadOnlyList<string> _outerPrimaryKeys;
    private readonly bool _selectStar;
    private readonly List<string> _projection = [];
    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _outerRows = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<string>> _outerByValue = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _innerValues = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _emitted = new(StringComparer.Ordinal);

    // Correlated state: inner match counts keyed by correlation key (EXISTS) or (correlationKey, inValue)
    // (IN), and the outer-row index keyed by the same so an affected key's outer rows can be re-evaluated.
    private readonly Dictionary<string, int> _innerByKey = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<string>> _outerByCorrelation = new(StringComparer.Ordinal);

    private int _innerExistsCount;
    private bool _seeded;

    /// <summary>
    /// Compiles the supplied IN/EXISTS subquery shape into an incremental semi/anti-join operator.
    /// </summary>
    /// <param name="shape">The extracted IN/NOT IN/EXISTS/NOT EXISTS shape.</param>
    /// <param name="engine">The owning engine (catalog access for the outer table's primary key).</param>
    /// <param name="databaseName">The database that owns the outer and inner tables.</param>
    /// <exception cref="NotSupportedException">Thrown when the outer table has no primary key to identify rows across updates.</exception>
    public SubqueryReactiveQuery(SubqueryShape shape, DataVoEngine engine, string databaseName)
    {
        _shape = shape;
        _innerPredicate = ReactivePredicate.Compile(shape.InnerWhere);
        _outerPrimaryKeys = engine.Catalog.GetTablePrimaryKeys(shape.OuterTable, databaseName);
        if (_outerPrimaryKeys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive subqueries require a primary key on outer table '{shape.OuterTable}' to identify rows across updates.");
        }

        foreach (SelectColumnNode column in shape.OuterColumns)
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

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [_shape.OuterTable, _shape.InnerTable];

    private bool IsCorrelated => _shape.IsCorrelated;

    private bool IsExistsKind => _shape.Kind is SubqueryKind.Exists or SubqueryKind.NotExists;

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        if (IsOuter(table))
        {
            foreach ((long _, IReadOnlyDictionary<string, object?> row) in rows)
            {
                AddOuter(row);
            }
        }
        else if (IsInner(table))
        {
            foreach ((long _, IReadOnlyDictionary<string, object?> row) in rows)
            {
                if (_innerPredicate.Matches(row))
                {
                    AddInner(row, +1);
                }
            }
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Outer changes affect only their own row's membership and are added directly as candidates. Inner
    /// changes shift the membership index: for a correlated or IN subquery only the outer rows indexed
    /// under the touched correlation/value key are re-evaluated; an <em>uncorrelated</em> EXISTS shares a
    /// single global count, so any inner change can flip every outer row and forces a full re-evaluation
    /// of the outer relation. NOT IN / NOT EXISTS are the anti-join
    /// complement and fall out of the same membership test negated in <see cref="Qualifies"/>.
    /// </remarks>
    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
        EnsureBaseline();
        var candidates = new HashSet<string>(StringComparer.Ordinal);
        bool recheckAllOuter = false;
        HashSet<string> touchedInnerValues = new(StringComparer.Ordinal);

        foreach (RowChange change in changes)
        {
            if (IsOuter(change.Table))
            {
                if (change.Before is not null && change.Kind is ChangeKind.Delete or ChangeKind.Update)
                {
                    string pk = Identity(change.Before);
                    candidates.Add(pk);
                    RemoveOuter(change.Before);
                }

                if (change.After is not null && change.Kind is ChangeKind.Insert or ChangeKind.Update)
                {
                    string pk = Identity(change.After);
                    AddOuter(change.After);
                    candidates.Add(pk);
                }

                continue;
            }

            if (!IsInner(change.Table))
            {
                continue;
            }

            if (change.Before is not null && change.Kind is ChangeKind.Delete or ChangeKind.Update && _innerPredicate.Matches(change.Before))
            {
                string? key = AddInner(change.Before, -1);
                if (key is not null)
                {
                    touchedInnerValues.Add(key);
                }

                recheckAllOuter |= IsExistsKind && !IsCorrelated;
            }

            if (change.After is not null && change.Kind is ChangeKind.Insert or ChangeKind.Update && _innerPredicate.Matches(change.After))
            {
                string? key = AddInner(change.After, +1);
                if (key is not null)
                {
                    touchedInnerValues.Add(key);
                }

                recheckAllOuter |= IsExistsKind && !IsCorrelated;
            }
        }

        if (recheckAllOuter)
        {
            foreach (string pk in _outerRows.Keys)
            {
                candidates.Add(pk);
            }
        }
        else
        {
            Dictionary<string, HashSet<string>> index = IsCorrelated ? _outerByCorrelation : _outerByValue;
            foreach (string valueKey in touchedInnerValues)
            {
                if (index.TryGetValue(valueKey, out HashSet<string>? pks))
                {
                    foreach (string pk in pks)
                    {
                        candidates.Add(pk);
                    }
                }
            }
        }

        return Diff(candidates);
    }

    /// <summary>
    /// Materializes the qualifying outer rows into the emitted baseline on the first <see cref="Apply"/>
    /// (after both sides are seeded), so the first delivered delta is diffed against the correct
    /// starting point. The baseline itself is never delivered.
    /// </summary>
    private void EnsureBaseline()
    {
        if (_seeded)
        {
            return;
        }

        _seeded = true;
        foreach ((string pk, IReadOnlyDictionary<string, object?> row) in _outerRows)
        {
            if (Qualifies(row))
            {
                _emitted[pk] = Project(row);
            }
        }
    }

    /// <summary>
    /// Re-evaluates each candidate outer row's qualification from final state and diffs it against the
    /// currently emitted set, producing the exact added/removed/updated output.
    /// </summary>
    private QueryChange Diff(HashSet<string> candidates)
    {
        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];
        List<IReadOnlyDictionary<string, object?>> updated = [];
        List<IReadOnlyDictionary<string, object?>> updatedBefore = [];

        foreach (string pk in candidates)
        {
            IReadOnlyDictionary<string, object?>? current = null;
            if (_outerRows.TryGetValue(pk, out IReadOnlyDictionary<string, object?>? row) && Qualifies(row))
            {
                current = Project(row);
            }

            bool wasEmitted = _emitted.TryGetValue(pk, out IReadOnlyDictionary<string, object?>? previous);
            if (current is null)
            {
                if (wasEmitted)
                {
                    _emitted.Remove(pk);
                    removed.Add(previous!);
                }

                continue;
            }

            if (!wasEmitted)
            {
                _emitted[pk] = current;
                added.Add(current);
            }
            else if (!RowsEqual(previous!, current))
            {
                _emitted[pk] = current;
                updatedBefore.Add(previous!);
                updated.Add(current);
            }
        }

        return new QueryChange(added, removed, updated, updatedBefore);
    }

    private void AddOuter(IReadOnlyDictionary<string, object?> row)
    {
        string pk = Identity(row);
        if (_outerRows.TryGetValue(pk, out IReadOnlyDictionary<string, object?>? existing))
        {
            RemoveOuterIndex(pk, existing);
        }

        IReadOnlyDictionary<string, object?> copy = Copy(row);
        _outerRows[pk] = copy;
        AddOuterIndex(pk, copy);
    }

    private void RemoveOuter(IReadOnlyDictionary<string, object?> row)
    {
        string pk = Identity(row);
        if (_outerRows.TryGetValue(pk, out IReadOnlyDictionary<string, object?>? existing))
        {
            RemoveOuterIndex(pk, existing);
            _outerRows.Remove(pk);
        }
    }

    private void AddOuterIndex(string pk, IReadOnlyDictionary<string, object?> row)
    {
        if (IsCorrelated)
        {
            AddToIndex(_outerByCorrelation, OuterCorrelationKey(row), pk);
            return;
        }

        if (IsExistsKind)
        {
            return;
        }

        AddToIndex(_outerByValue, OuterValueKey(row), pk);
    }

    private void RemoveOuterIndex(string pk, IReadOnlyDictionary<string, object?> row)
    {
        if (IsCorrelated)
        {
            RemoveFromIndex(_outerByCorrelation, OuterCorrelationKey(row), pk);
            return;
        }

        if (IsExistsKind)
        {
            return;
        }

        RemoveFromIndex(_outerByValue, OuterValueKey(row), pk);
    }

    private static void AddToIndex(Dictionary<string, HashSet<string>> index, string key, string pk)
    {
        if (!index.TryGetValue(key, out HashSet<string>? pks))
        {
            pks = new HashSet<string>(StringComparer.Ordinal);
            index[key] = pks;
        }

        pks.Add(pk);
    }

    private static void RemoveFromIndex(Dictionary<string, HashSet<string>> index, string key, string pk)
    {
        if (index.TryGetValue(key, out HashSet<string>? pks))
        {
            pks.Remove(pk);
            if (pks.Count == 0)
            {
                index.Remove(key);
            }
        }
    }

    private string? AddInner(IReadOnlyDictionary<string, object?> row, int weight)
    {
        if (IsCorrelated)
        {
            string key = InnerCorrelationKey(row);
            Bump(_innerByKey, key, weight);
            return key;
        }

        if (IsExistsKind)
        {
            _innerExistsCount += weight;
            return null;
        }

        string valueKey = InnerValueKey(row);
        Bump(_innerValues, valueKey, weight);
        return valueKey;
    }

    private static void Bump(Dictionary<string, int> counts, string key, int weight)
    {
        int next = counts.GetValueOrDefault(key) + weight;
        if (next <= 0)
        {
            counts.Remove(key);
        }
        else
        {
            counts[key] = next;
        }
    }

    /// <summary>
    /// Decides whether an outer row belongs in the result: tests membership against the maintained inner
    /// index (correlated → per-correlation-key count; uncorrelated EXISTS → global count; IN →
    /// per-value count), then negates the test for the NOT IN / NOT EXISTS anti-join variants.
    /// </summary>
    private bool Qualifies(IReadOnlyDictionary<string, object?> outerRow)
    {
        bool contains;
        if (IsCorrelated)
        {
            contains = _innerByKey.GetValueOrDefault(OuterCorrelationKey(outerRow)) > 0;
        }
        else if (IsExistsKind)
        {
            contains = _innerExistsCount > 0;
        }
        else
        {
            contains = _innerValues.GetValueOrDefault(OuterValueKey(outerRow)) > 0;
        }

        return _shape.Kind is SubqueryKind.NotIn or SubqueryKind.NotExists
            ? !contains
            : contains;
    }

    private string OuterValueKey(IReadOnlyDictionary<string, object?> row)
    {
        return ValueKeyOf(row, _shape.OuterColumn);
    }

    private string InnerValueKey(IReadOnlyDictionary<string, object?> row)
    {
        return _shape.InnerColumn is null ? string.Empty : ValueKeyOf(row, _shape.InnerColumn);
    }

    /// <summary>
    /// Builds the correlation key for an outer row: the outer correlation column value for EXISTS, or the
    /// (correlation, IN value) compound key for IN.
    /// </summary>
    private string OuterCorrelationKey(IReadOnlyDictionary<string, object?> row)
    {
        string correlation = ValueKeyOf(row, _shape.OuterCorrelationColumn!);
        return IsExistsKind ? correlation : Compound(correlation, ValueKeyOf(row, _shape.OuterColumn));
    }

    /// <summary>
    /// Builds the correlation key for an inner row: the inner correlation column value for EXISTS, or the
    /// (correlation, projected IN value) compound key for IN — aligned with <see cref="OuterCorrelationKey"/>.
    /// </summary>
    private string InnerCorrelationKey(IReadOnlyDictionary<string, object?> row)
    {
        string correlation = ValueKeyOf(row, _shape.InnerCorrelationColumn!);
        return IsExistsKind ? correlation : Compound(correlation, InnerValueKey(row));
    }

    private static string Compound(string correlation, string value) =>
        correlation.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + correlation + value;

    private static string ValueKeyOf(IReadOnlyDictionary<string, object?> row, string column)
    {
        object? value = row.TryGetValue(column, out object? candidate) ? candidate : null;
        return DistinctReactiveQuery.ValueKey(value);
    }

    private string Identity(IReadOnlyDictionary<string, object?> row)
    {
        return string.Join(
            "",
            _outerPrimaryKeys.Select(column =>
            {
                object? value = row.TryGetValue(column, out object? v) ? v : null;
                return DistinctReactiveQuery.ValueKey(value);
            }));
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

    private static bool RowsEqual(IReadOnlyDictionary<string, object?> a, IReadOnlyDictionary<string, object?> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        foreach ((string key, object? value) in a)
        {
            if (!b.TryGetValue(key, out object? other)
                || ReactiveValueComparer.Instance.Compare(value, other) != 0)
            {
                return false;
            }
        }

        return true;
    }

    private bool IsOuter(string table) => table.Equals(_shape.OuterTable, StringComparison.OrdinalIgnoreCase);

    private bool IsInner(string table) => table.Equals(_shape.InnerTable, StringComparison.OrdinalIgnoreCase);

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
