using DataVo.Core.Execution;
using DataVo.Core.Execution.Volcano;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// GROUP BY on the typed paths: groups numbered by key without allocating per row, aggregate state stored per
/// group number, and HAVING, ORDER BY and LIMIT applied to the groups before any result row is built.
/// </summary>
/// <remarks>
/// A query such as <c>SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId HAVING COUNT(*) &gt;= 12
/// ORDER BY N DESC LIMIT 100</c> has about 125,000 groups on the benchmark data. Keying each row by a boxed
/// <c>object[]</c>, keeping an object per group, and building two dictionaries per group for the general HAVING and
/// ORDER BY to read cost 260 MB per call. Here a one-column key is looked up by its value, the state of every group
/// lives in a few arrays, and only the groups that pass HAVING and make the top of the ORDER BY become rows.
/// </remarks>
internal partial class Select
{
    /// <summary>A group key compared the way the general path's hash aggregate compares it (by value).</summary>
    private readonly struct GroupKey(object?[] values) : IEquatable<GroupKey>
    {
        public object?[] Values { get; } = values;

        public bool Equals(GroupKey other)
        {
            for (int i = 0; i < Values.Length; i++)
            {
                if (!Equals(Values[i], other.Values[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj) => obj is GroupKey other && Equals(other);

        public override int GetHashCode()
        {
            var hash = new HashCode();
            foreach (object? value in Values)
            {
                hash.Add(value);
            }

            return hash.ToHashCode();
        }
    }

    /// <summary>
    /// Numbers the groups of a GROUP BY in the order they first appear. Keys compare as the general path compares
    /// them (by value, with <see cref="object.Equals(object?, object?)"/>), so an INT 5 and a BIGINT 5 stay apart as
    /// they do there. A one-column key is looked up without allocating; a group's key values are boxed once, when
    /// the group is created. Keys of several columns use an <c>object[]</c> per row, as before.
    /// </summary>
    private sealed class GroupLookup
    {
        private readonly Dictionary<int, int> _int32 = [];
        private readonly Dictionary<long, int> _int64 = [];
        private readonly Dictionary<object, int> _values = [];
        private readonly Dictionary<GroupKey, int> _composite = [];
        private readonly List<object?> _singleKeys = [];
        private readonly List<object?[]> _compositeKeys = [];
        private int _nullGroup = -1;

        /// <summary>The number of groups.</summary>
        public int Count => _singleKeys.Count + _compositeKeys.Count;

        /// <summary>The group of a one-column key held in <paramref name="cell"/> of <paramref name="column"/>.</summary>
        public int FindOrAdd(Column column, CellValue cell, out bool added)
        {
            added = false;
            if (cell.IsNull)
            {
                if (_nullGroup < 0)
                {
                    _nullGroup = AddSingle(null);
                    added = true;
                }

                return _nullGroup;
            }

            bool boxedAsCellType = !column.Type.Equals("FLOAT", StringComparison.OrdinalIgnoreCase);
            if (boxedAsCellType && cell.Type == CellType.Int32)
            {
                int key = cell.AsInt32();
                if (!_int32.TryGetValue(key, out int group))
                {
                    group = _int32[key] = AddSingle(key);
                    added = true;
                }

                return group;
            }

            if (boxedAsCellType && cell.Type == CellType.Int64)
            {
                long key = cell.AsInt64();
                if (!_int64.TryGetValue(key, out int group))
                {
                    group = _int64[key] = AddSingle(key);
                    added = true;
                }

                return group;
            }

            object? value = StorageContext.ConvertCellForLegacyDictionary(column, cell);
            if (value is null)
            {
                return FindOrAdd(column, default, out added);
            }

            if (!_values.TryGetValue(value, out int found))
            {
                found = _values[value] = AddSingle(value);
                added = true;
            }

            return found;
        }

        /// <summary>The group of a key of several columns.</summary>
        public int FindOrAdd(object?[] keys, out bool added)
        {
            var key = new GroupKey(keys);
            added = !_composite.TryGetValue(key, out int group);
            if (added)
            {
                group = _composite[key] = _compositeKeys.Count;
                _compositeKeys.Add(keys);
            }

            return group;
        }

        /// <summary>The value of key column <paramref name="column"/> of <paramref name="group"/>.</summary>
        public object? KeyOf(int group, int column) =>
            _compositeKeys.Count > 0 ? _compositeKeys[group][column] : _singleKeys[group];

        private int AddSingle(object? value)
        {
            _singleKeys.Add(value);
            return _singleKeys.Count - 1;
        }
    }

    /// <summary>
    /// The aggregate state of every group of a typed scan, as arrays indexed by <c>group * aggregates + i</c>: no
    /// object per group. Only the arrays the aggregates use are allocated.
    /// </summary>
    private sealed class TypedGroupStates
    {
        private readonly int _aggregates;
        private long[] _counts = [];
        private double[] _sums = [];
        private SqlSumAccumulator[] _totals = [];
        private object?[] _extremes = [];
        private bool[] _hasExtreme = [];
        private readonly bool _useSums;
        private readonly bool _useTotals;
        private readonly bool _useExtremes;

        public TypedGroupStates(List<TypedAggregate> aggregates)
        {
            _aggregates = aggregates.Count;
            foreach (TypedAggregate aggregate in aggregates)
            {
                _useSums |= aggregate.Spec.Function == HashAggregateOperator.AggregateFunction.Avg;
                _useTotals |= aggregate.Spec.Function == HashAggregateOperator.AggregateFunction.Sum;
                _useExtremes |= aggregate.Spec.Function is HashAggregateOperator.AggregateFunction.Min or HashAggregateOperator.AggregateFunction.Max;
            }
        }

        /// <summary>The number of groups.</summary>
        public int Count { get; private set; }

        /// <summary>Adds a group with every aggregate empty; returns its number.</summary>
        public int Add()
        {
            int needed = (Count + 1) * _aggregates;
            if (needed > _counts.Length)
            {
                int size = Math.Max(needed, Math.Max(16 * _aggregates, _counts.Length * 2));
                Array.Resize(ref _counts, size);
                if (_useSums)
                {
                    Array.Resize(ref _sums, size);
                }

                if (_useTotals)
                {
                    Array.Resize(ref _totals, size);
                }

                if (_useExtremes)
                {
                    Array.Resize(ref _extremes, size);
                    Array.Resize(ref _hasExtreme, size);
                }
            }

            return Count++;
        }

        public ref long CountOf(int group, int i) => ref _counts[(group * _aggregates) + i];

        public ref double SumOf(int group, int i) => ref _sums[(group * _aggregates) + i];

        public ref SqlSumAccumulator TotalOf(int group, int i) => ref _totals[(group * _aggregates) + i];

        public ref object? ExtremeOf(int group, int i) => ref _extremes[(group * _aggregates) + i];

        public ref bool HasExtremeOf(int group, int i) => ref _hasExtreme[(group * _aggregates) + i];
    }

    /// <summary>The group number of the row in <c>state.Cells</c>.</summary>
    private static int FindTypedGroup(TypedScanState state)
    {
        int[] ordinals = state.GroupOrdinals!;
        GroupLookup lookup = state.GroupKeys!;
        if (ordinals.Length == 0 && lookup.Count == 1)
        {
            return 0; // no GROUP BY: every row belongs to the one group, with no key to build or look up
        }

        int group;
        bool added;
        if (ordinals.Length == 1)
        {
            group = lookup.FindOrAdd(state.Columns[ordinals[0]], state.Cells[ordinals[0]], out added);
        }
        else
        {
            var keys = new object?[ordinals.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = StorageContext.ConvertCellForLegacyDictionary(state.Columns[ordinals[i]], state.Cells[ordinals[i]]);
            }

            group = lookup.FindOrAdd(keys, out added);
        }

        if (added)
        {
            state.GroupStates!.Add();
        }

        return group;
    }

    /// <summary>The value of aggregate <paramref name="i"/> of <paramref name="group"/>, typed as the general path types it.</summary>
    private static object? TypedAggregateValue(TypedScanState state, int group, int i)
    {
        TypedGroupStates states = state.GroupStates!;
        return state.Aggregates![i].Spec.Function switch
        {
            HashAggregateOperator.AggregateFunction.Count => states.CountOf(group, i),
            // SQL: SUM and AVG over no non-null values are NULL.
            HashAggregateOperator.AggregateFunction.Sum => states.TotalOf(group, i).Result,
            HashAggregateOperator.AggregateFunction.Avg => states.CountOf(group, i) == 0 ? null : states.SumOf(group, i) / states.CountOf(group, i),
            _ => states.ExtremeOf(group, i),
        };
    }

    /// <summary>
    /// The result rows of a typed GROUP BY: one per group that passes HAVING, and when ORDER BY has a LIMIT only
    /// the first LIMIT + OFFSET of them in order.
    /// </summary>
    private ListedTable FinishAggregates(TypedScanState state)
    {
        if (state.GroupStates!.Count == 0)
        {
            return FinishAggregatesOverNoRows(state);
        }

        List<int> groups = SelectTypedGroups(state);
        var rows = new List<JoinedRow>(groups.Count);
        foreach (int group in groups)
        {
            rows.Add(NewTypedGroupRow(state, group));
        }

        _volcanoAggregatePushedDown = true;
        return new ListedTable(rows);
    }

    /// <summary>
    /// The result when no row matched, as the general path builds it: with GROUP BY no row; without, one row with
    /// no table columns whose COUNTs are 0 (an <see cref="int"/>, as the general COUNT returns) and whose other
    /// aggregates are NULL. HAVING, ORDER BY and LIMIT then run over it as usual.
    /// </summary>
    private ListedTable FinishAggregatesOverNoRows(TypedScanState state)
    {
        _volcanoAggregatePushedDown = true;
        if (state.GroupOrdinals!.Length > 0)
        {
            return new ListedTable([]);
        }

        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (TypedAggregate aggregate in state.Aggregates!)
        {
            values[aggregate.Spec.OutputColumn] = aggregate.Spec.Function == HashAggregateOperator.AggregateFunction.Count ? 0 : null;
        }

        var row = new JoinedRow();
        row.Add(GroupBy.HASH_VALUE, new Row(values));
        return new ListedTable([row]);
    }

    /// <summary>
    /// The groups to return, in order. HAVING and the ORDER BY keys are evaluated by the general evaluator on one
    /// reused row, filled per group, so they mean exactly what they mean on the general path; a group's row is
    /// only built if it is returned. The ORDER BY and LIMIT that run afterwards then see only those rows, already
    /// filtered and in order, and keep them as they are.
    /// </summary>
    private List<int> SelectTypedGroups(TypedScanState state)
    {
        int count = state.GroupStates!.Count;
        ExpressionNode? having = _model.GetHavingExpression();
        // DISTINCT and window functions run over all the grouped rows, so they need every group, not a top-N.
        OrderByNode? orderBy = _model.GetOrderByExpression() is { Columns.Count: > 0 } order
            && _model.LimitTake.HasValue
            && !_model.IsDistinct
            && _model.GetWindowFunctionColumns().Count == 0 ? order : null;
        if (having is null && orderBy is null)
        {
            return [.. Enumerable.Range(0, count)];
        }

        int limit = orderBy is null ? int.MaxValue : _model.RowsNeededForLimit;
        (int Kind, int Index)[] sources = orderBy is null ? [] : [.. orderBy.Columns.Select(column => ResolveTypedOrderKey(state, column.Column.Name))];
        bool needsRow = having is not null || sources.Any(source => source.Kind == OrderKeyGeneral);
        var kept = new List<int>();
        var top = new List<(object?[] Keys, int Group)>();
        JoinedRow scratch = NewTypedGroupRow(state, 0);
        object?[] keys = new object?[sources.Length];
        for (int group = 0; group < count && limit > 0; group++)
        {
            if (needsRow)
            {
                FillTypedGroupRow(state, group, scratch);
            }

            if (having is not null && !EvaluatePredicate(having, scratch))
            {
                continue;
            }

            if (orderBy is null)
            {
                kept.Add(group);
                continue;
            }

            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = sources[i].Kind switch
                {
                    OrderKeyAggregate => TypedAggregateValue(state, group, sources[i].Index),
                    OrderKeyGroupColumn => state.GroupKeys!.KeyOf(group, sources[i].Index),
                    _ => ResolveOrderByValue(scratch, orderBy.Columns[i].Column.Name),
                };
            }

            OfferTypedGroup(orderBy, top, limit, keys, group);
        }

        return orderBy is null ? kept : [.. top.Select(entry => entry.Group)];
    }

    private const int OrderKeyGeneral = 0;
    private const int OrderKeyAggregate = 1;
    private const int OrderKeyGroupColumn = 2;

    /// <summary>
    /// Where an ORDER BY key of a grouped row comes from, when that is certain without the general evaluator: an
    /// aggregate (named by its SELECT alias or its header) or a group column (by name, or by a SELECT alias of it).
    /// Anything else, or a name both could mean, is read by the general evaluator (<see cref="OrderKeyGeneral"/>).
    /// </summary>
    private (int Kind, int Index) ResolveTypedOrderKey(TypedScanState state, string token)
    {
        List<TypedAggregate> aggregates = state.Aggregates!;
        List<string> groupColumns = state.GroupColumnNames!;
        ExpressionNode? aliased = _model.GetSelectColumnByAlias(token)?.Expression;
        if (aliased is AggregateExpressionNode aggregate)
        {
            // The general path reads HASH_VALUE[header]; the typed row writes each aggregate under its output name.
            int at = LastAggregateNamed(aggregates, AggregateExpressionFormatter.BuildHeader(aggregate));
            return at >= 0 ? (OrderKeyAggregate, at) : (OrderKeyGeneral, 0);
        }

        string? column = aliased switch
        {
            null => token,
            ColumnRefNode reference => reference.Column,
            ResolvedColumnRefNode resolved => resolved.Column,
            _ => null,
        };
        if (column is null)
        {
            return (OrderKeyGeneral, 0);
        }

        if (aliased is null && LastAggregateNamed(aggregates, token) is int named and >= 0)
        {
            return (OrderKeyAggregate, named);
        }

        int dot = column.LastIndexOf('.');
        string name = dot >= 0 ? column[(dot + 1)..] : column;
        bool qualifiedHere = dot < 0 || column[..dot].Equals(state.TableName, StringComparison.OrdinalIgnoreCase)
            || column[..dot].Equals(_model.FromTable.TableAlias, StringComparison.OrdinalIgnoreCase);
        int group = groupColumns.FindIndex(candidate => candidate.Equals(name, StringComparison.OrdinalIgnoreCase));
        bool ambiguous = dot < 0 && LastAggregateNamed(aggregates, name) >= 0;
        return group >= 0 && qualifiedHere && !ambiguous ? (OrderKeyGroupColumn, group) : (OrderKeyGeneral, 0);
    }

    /// <summary>The last aggregate whose output name is <paramref name="name"/> (the one a grouped row keeps), or -1.</summary>
    private static int LastAggregateNamed(List<TypedAggregate> aggregates, string name) =>
        aggregates.FindLastIndex(aggregate => aggregate.Spec.OutputColumn.Equals(name, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Keeps the first <paramref name="limit"/> groups in ORDER BY order, compared as the general sort compares
    /// them; ties keep the order the groups first appeared in, as its stable sort does.
    /// </summary>
    private static void OfferTypedGroup(OrderByNode orderBy, List<(object?[] Keys, int Group)> top, int limit, object?[] keys, int group)
    {
        if (top.Count >= limit && CompareTypedGroups(orderBy, keys, group, top[^1].Keys, top[^1].Group) >= 0)
        {
            return;
        }

        int low = 0;
        int high = top.Count;
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            if (CompareTypedGroups(orderBy, top[mid].Keys, top[mid].Group, keys, group) <= 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        top.Insert(low, ((object?[])keys.Clone(), group));
        if (top.Count > limit)
        {
            top.RemoveAt(top.Count - 1);
        }
    }

    private static int CompareTypedGroups(OrderByNode orderBy, object?[] left, int leftGroup, object?[] right, int rightGroup)
    {
        for (int i = 0; i < left.Length; i++)
        {
            int order = DynamicObjectComparer.Instance.Compare(left[i], right[i]);
            if (order != 0)
            {
                return orderBy.Columns[i].IsAscending ? order : -order;
            }
        }

        return leftGroup.CompareTo(rightGroup);
    }

    /// <summary>A new row for <paramref name="group"/>: its key columns and, under the aggregate key, its aggregates.</summary>
    private static JoinedRow NewTypedGroupRow(TypedScanState state, int group)
    {
        var row = new JoinedRow(state.TableName, new Row(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)));
        row.Add(GroupBy.HASH_VALUE, new Row(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)));
        FillTypedGroupRow(state, group, row);
        return row;
    }

    /// <summary>Writes the key and aggregate values of <paramref name="group"/> into <paramref name="row"/>.</summary>
    private static void FillTypedGroupRow(TypedScanState state, int group, JoinedRow row)
    {
        Row keyValues = row[state.TableName];
        for (int i = 0; i < state.GroupColumnNames!.Count; i++)
        {
            keyValues[state.GroupColumnNames[i]] = state.GroupKeys!.KeyOf(group, i);
        }

        Row aggregateValues = row[GroupBy.HASH_VALUE];
        for (int i = 0; i < state.Aggregates!.Count; i++)
        {
            aggregateValues[state.Aggregates[i].Spec.OutputColumn] = TypedAggregateValue(state, group, i);
        }
    }
}
