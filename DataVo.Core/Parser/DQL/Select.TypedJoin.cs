using DataVo.Core.Enums;
using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Aggregations;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using CatalogColumn = DataVo.Core.Models.Catalog.Column;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// The typed join path: two stored tables joined on one integer equality, filtered by per-table
/// <c>column op literal</c> comparisons and aggregated (with or without GROUP BY). The smaller table is
/// hashed by its join key, the larger one is streamed and probed, and each group keeps typed running
/// aggregates plus its first joined row. The output has the general path's shape: the group's first
/// joined row with the aggregate values under the aggregation key, computed with the same rules as the
/// general aggregate functions. Joined rows are never materialized one by one.
/// </summary>
internal partial class Select
{
    private sealed class JoinSide
    {
        public required string Name { get; init; }
        public required TableDetail Detail { get; init; }
        public required IReadOnlyList<CatalogColumn> Columns { get; init; }
        public required List<TypedPredicate> Predicates { get; init; }
        public required int KeyOrdinal { get; init; }
        public required int ClusteredOrdinal { get; init; }
        public required bool ClusteredIsGuid { get; init; }
        public bool[] FilterMask { get; set; } = [];
        public bool[] NeededMask { get; set; } = [];
    }

    private enum JoinAggregateKind { CountRows, CountValues, Sum, Avg, Min, Max }

    private sealed class JoinAggregate(Aggregation function, JoinAggregateKind kind, int side, int ordinal)
    {
        public Aggregation Function { get; } = function;
        public JoinAggregateKind Kind { get; } = kind;
        public int Side { get; } = side;
        public int Ordinal { get; } = ordinal;
    }

    private sealed class JoinGroup(JoinedRow representative, int aggregates)
    {
        public JoinedRow Representative { get; } = representative;
        public int[] Counts { get; } = new int[aggregates];
        public double[] Sums { get; } = new double[aggregates];
        public DataVo.Core.Execution.SqlSumAccumulator[] Totals { get; } = new DataVo.Core.Execution.SqlSumAccumulator[aggregates];
        public object?[] Extremes { get; } = new object?[aggregates];
    }

    private sealed class JoinProbeState
    {
        public required JoinSide Build { get; init; }
        public required JoinSide Probe { get; init; }
        public required int BuildIndex { get; init; }
        public required Dictionary<long, List<CellValue[]>> Hash { get; init; }
        public required List<(int Side, int Ordinal)> GroupColumns { get; init; }
        public required List<JoinAggregate> Aggregates { get; init; }
        public required string[] SideNames { get; init; }
        public required CellValue[] Cells { get; init; }
        public Dictionary<GroupKey, JoinGroup> Groups { get; } = [];
        public List<JoinGroup> GroupOrder { get; } = [];
    }

    private sealed class JoinBuildState(JoinSide side)
    {
        public JoinSide Side { get; } = side;
        public CellValue[] Cells { get; } = new CellValue[side.Columns.Count];
        public Dictionary<long, List<CellValue[]>> Hash { get; } = [];
    }

    /// <summary>Returns the aggregated rows, or null when the query needs the general path.</summary>
    private ListedTable? TryEvaluateTypedJoinAggregate()
    {
        if (_activeSnapshot is not null
            || ast.Ctes.Count > 0
            || !_model.JoinStatement.ContainsJoin()
            || _model.JoinStatement.Model.JoinConditions.Count != 1
            || _model.IsDistinct
            || _model.Database is null
            || _model.TableService is null
            || _model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0
            || _model.AggregateStatement.Model.Functions.Count == 0)
        {
            return null;
        }

        JoinModel.JoinCondition condition = _model.JoinStatement.Model.JoinConditions[0];
        if (!condition.JoinType.Equals(JoinTypes.INNER, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        DataVoEngine engine = DataVoEngine.Current();
        JoinSide? left = BuildJoinSide(engine, condition.LeftColumn);
        JoinSide? right = BuildJoinSide(engine, condition.RightColumn);
        if (left is null || right is null
            || left.Detail.TableName.Equals(right.Detail.TableName, StringComparison.OrdinalIgnoreCase)
            || !IsIntegerColumn(left.Columns[left.KeyOrdinal])
            || !IsIntegerColumn(right.Columns[right.KeyOrdinal]))
        {
            return null;
        }

        JoinSide[] sides = [left, right];
        ExpressionNode? where = _model.WhereStatement.IsEvaluatable() ? _model.WhereStatement.GetExpression() : null;
        if (where is not null && !TryCollectSidePredicates(
                where,
                reference => ResolveJoinColumn(reference, sides),
                [left.Columns, right.Columns],
                [left.Predicates, right.Predicates]))
        {
            return null;
        }

        var groupColumns = new List<(int Side, int Ordinal)>();
        foreach (Column column in _model.GroupByStatement.Model.Columns)
        {
            if (ResolveJoinSideColumn(column.TableName, column.ColumnName, sides) is not { } resolved)
            {
                return null;
            }

            groupColumns.Add(resolved);
        }

        var aggregates = new List<JoinAggregate>();
        foreach (Aggregation function in _model.AggregateStatement.Model.Functions)
        {
            // A plain column argument (its reference is kept as the expression) or COUNT(*); nothing computed.
            if (function.Field is null
                || function.ArgumentExpression is not (null or ColumnRefNode or ResolvedColumnRefNode))
            {
                return null;
            }

            function.ValidateArgument();
            JoinAggregateKind? kind = function switch
            {
                Count when function.Field.TableName == "*" => JoinAggregateKind.CountRows,
                Count => JoinAggregateKind.CountValues,
                Sum => JoinAggregateKind.Sum,
                Avg => JoinAggregateKind.Avg,
                Min => JoinAggregateKind.Min,
                Max => JoinAggregateKind.Max,
                _ => null,
            };
            if (kind is null)
            {
                return null;
            }

            if (kind == JoinAggregateKind.CountRows)
            {
                aggregates.Add(new JoinAggregate(function, kind.Value, -1, -1));
                continue;
            }

            if (ResolveJoinSideColumn(function.Field.TableName, function.Field.ColumnName, sides) is not { } target)
            {
                return null;
            }

            aggregates.Add(new JoinAggregate(function, kind.Value, target.Side, target.Ordinal));
        }

        foreach (JoinSide side in sides)
        {
            side.FilterMask = new bool[side.Columns.Count];
            foreach (TypedPredicate predicate in side.Predicates)
            {
                side.FilterMask[predicate.Ordinal] = true;
            }

            side.FilterMask[side.KeyOrdinal] = true;
            side.NeededMask = new bool[side.Columns.Count];
        }

        foreach ((int side, int ordinal) in groupColumns)
        {
            sides[side].NeededMask[ordinal] = true;
        }

        foreach (JoinAggregate aggregate in aggregates)
        {
            if (aggregate.Side >= 0)
            {
                sides[aggregate.Side].NeededMask[aggregate.Ordinal] = true;
            }
        }

        // Hash the smaller table; stream the larger one.
        string databaseName = _model.Database;
        long leftRows = EstimateRows(engine, left, databaseName);
        long rightRows = EstimateRows(engine, right, databaseName);
        int buildIndex = leftRows <= rightRows ? 0 : 1;
        JoinSide build = sides[buildIndex];
        JoinSide probe = sides[1 - buildIndex];

        var buildState = new JoinBuildState(build);
        if (!engine.StorageContext.ScanTableRows(build.Detail.TableName, databaseName, null, null, ref buildState, VisitJoinBuildRow))
        {
            return null;
        }

        var probeState = new JoinProbeState
        {
            Build = build,
            Probe = probe,
            BuildIndex = buildIndex,
            Hash = buildState.Hash,
            GroupColumns = groupColumns,
            Aggregates = aggregates,
            SideNames = [left.Name, right.Name],
            Cells = new CellValue[probe.Columns.Count],
        };
        if (!engine.StorageContext.ScanTableRows(probe.Detail.TableName, databaseName, null, null, ref probeState, VisitJoinProbeRow))
        {
            return null;
        }

        if (probeState.GroupOrder.Count == 0)
        {
            return null; // aggregates over no rows: the general path owns those semantics
        }

        var rows = new List<JoinedRow>(probeState.GroupOrder.Count);
        foreach (JoinGroup group in probeState.GroupOrder)
        {
            JoinedRow row = group.Representative;
            row.Add(Aggregation.HASH_VALUE, new Row());
            for (int i = 0; i < aggregates.Count; i++)
            {
                row[Aggregation.HASH_VALUE].Add(aggregates[i].Function.GetHeaderName(), FinishJoinAggregate(aggregates[i].Kind, group, i));
            }

            rows.Add(row);
        }

        _volcanoAggregatePushedDown = true;
        return new ListedTable(rows);
    }

    private static object? FinishJoinAggregate(JoinAggregateKind kind, JoinGroup group, int i) => kind switch
    {
        // The general aggregate functions: COUNT is an int, SUM is an integer for integers, AVG a double; both NULL over no values.
        JoinAggregateKind.CountRows or JoinAggregateKind.CountValues => group.Counts[i],
        JoinAggregateKind.Sum => group.Totals[i].Result,
        JoinAggregateKind.Avg => group.Counts[i] == 0 ? null : group.Sums[i] / group.Counts[i],
        _ => group.Extremes[i],
    };

    private JoinSide? BuildJoinSide(DataVoEngine engine, Column joinColumn)
    {
        TableDetail detail;
        try
        {
            detail = _model.TableService!.GetTableDetailByAliasOrName(joinColumn.TableName);
        }
        catch (Exception)
        {
            return null;
        }

        if (detail.IsMaterialized || _model.CteTables.ContainsKey(detail.TableName))
        {
            return null;
        }

        IReadOnlyList<CatalogColumn> columns = engine.Catalog.GetTableColumns(detail.TableName, _model.Database!);
        int keyOrdinal = OrdinalOf(columns, joinColumn.ColumnName);
        if (keyOrdinal < 0)
        {
            return null;
        }

        ClusteredKeyLayout? clustered = engine.StorageContext.ResolveClusteredKey(_model.Database!, detail.TableName);
        return new JoinSide
        {
            Name = joinColumn.TableName,
            Detail = detail,
            Columns = columns,
            Predicates = [],
            KeyOrdinal = keyOrdinal,
            ClusteredOrdinal = clustered?.Ordinal ?? -1,
            ClusteredIsGuid = clustered is not null
                && string.Equals(columns[clustered.Ordinal].Type, "GUID", StringComparison.OrdinalIgnoreCase),
        };
    }

    private static bool IsIntegerColumn(CatalogColumn column) => column.Type.Equals("INT", StringComparison.OrdinalIgnoreCase);

    private static long EstimateRows(DataVoEngine engine, JoinSide side, string databaseName) =>
        engine.StorageContext.TryEstimateRowCount(side.Detail.TableName, databaseName, out long count) ? count : long.MaxValue;

    private static (int Side, int Ordinal)? ResolveJoinSideColumn(string tableOrAlias, string column, JoinSide[] sides)
    {
        for (int side = 0; side < sides.Length; side++)
        {
            JoinSide candidate = sides[side];
            if (tableOrAlias.Equals(candidate.Name, StringComparison.OrdinalIgnoreCase)
                || tableOrAlias.Equals(candidate.Detail.TableName, StringComparison.OrdinalIgnoreCase)
                || tableOrAlias.Equals(candidate.Detail.TableAlias, StringComparison.OrdinalIgnoreCase))
            {
                int ordinal = OrdinalOf(candidate.Columns, column);
                return ordinal < 0 ? null : (side, ordinal);
            }
        }

        return null;
    }

    private static (int Side, string Column)? ResolveJoinColumn(ExpressionNode? node, JoinSide[] sides)
    {
        (string? table, string column) = node switch
        {
            ResolvedColumnRefNode resolved => (resolved.TableName, resolved.Column),
            ColumnRefNode reference => (reference.TableOrAlias, reference.Column),
            _ => ((string?)null, string.Empty),
        };

        if (column.Length == 0)
        {
            return null;
        }

        if (table is null)
        {
            // An unqualified column must belong to exactly one side.
            int found = -1;
            for (int side = 0; side < sides.Length; side++)
            {
                if (OrdinalOf(sides[side].Columns, column) >= 0)
                {
                    if (found >= 0)
                    {
                        return null;
                    }

                    found = side;
                }
            }

            return found < 0 ? null : (found, column);
        }

        return ResolveJoinSideColumn(table, column, sides) is { } resolvedSide ? (resolvedSide.Side, column) : null;
    }

    private static void LoadJoinCells(JoinSide side, RowKey rowId, ReadOnlySpan<byte> rowBytes, StoredRow? typedRow, bool[] mask, CellValue[] cells)
    {
        if (typedRow is not null)
        {
            for (int i = 0; i < mask.Length; i++)
            {
                if (mask[i])
                {
                    cells[i] = typedRow[i];
                }
            }
        }
        else
        {
            RowSerializer.DecodeCellsByOrdinal(rowBytes, side.Columns, mask, cells);
        }

        int key = side.ClusteredOrdinal;
        if (key >= 0 && mask[key] && cells[key].IsNull)
        {
            cells[key] = side.ClusteredIsGuid ? CellValue.From(rowId.AsGuid()) : KeyCell(side.Columns[key], rowId);
        }
    }

    private static bool PassesFilters(JoinSide side, CellValue[] cells)
    {
        foreach (TypedPredicate predicate in side.Predicates)
        {
            if (!Matches(predicate, cells[predicate.Ordinal], side.Columns[predicate.Ordinal]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryJoinKey(CellValue cell, out long key)
    {
        switch (cell.Type)
        {
            case CellType.Int32:
                key = cell.AsInt32();
                return true;
            case CellType.Int64:
                key = cell.AsInt64();
                return true;
            default:
                key = 0;
                return false; // NULL never joins
        }
    }

    private static bool VisitJoinBuildRow(ref JoinBuildState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, StoredRow? typedRow)
    {
        JoinSide side = state.Side;
        CellValue[] cells = state.Cells;
        LoadJoinCells(side, rowId, rowBytes, typedRow, side.FilterMask, cells);
        if (!PassesFilters(side, cells) || !TryJoinKey(cells[side.KeyOrdinal], out long key))
        {
            return true;
        }

        // Build rows are kept whole: the first row of a group is returned with every column.
        var all = new bool[side.Columns.Count];
        Array.Fill(all, true);
        var row = new CellValue[side.Columns.Count];
        LoadJoinCells(side, rowId, rowBytes, typedRow, all, row);
        if (!state.Hash.TryGetValue(key, out List<CellValue[]>? bucket))
        {
            bucket = [];
            state.Hash[key] = bucket;
        }

        bucket.Add(row);
        return true;
    }

    private static bool VisitJoinProbeRow(ref JoinProbeState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, StoredRow? typedRow)
    {
        JoinSide probe = state.Probe;
        CellValue[] cells = state.Cells;
        LoadJoinCells(probe, rowId, rowBytes, typedRow, probe.FilterMask, cells);
        if (!PassesFilters(probe, cells)
            || !TryJoinKey(cells[probe.KeyOrdinal], out long key)
            || !state.Hash.TryGetValue(key, out List<CellValue[]>? matches))
        {
            return true;
        }

        LoadJoinCells(probe, rowId, rowBytes, typedRow, probe.NeededMask, cells);
        bool fullyLoaded = false;
        int probeIndex = 1 - state.BuildIndex;
        foreach (CellValue[] buildRow in matches)
        {
            var keys = new object?[state.GroupColumns.Count];
            for (int i = 0; i < keys.Length; i++)
            {
                (int side, int ordinal) = state.GroupColumns[i];
                keys[i] = side == probeIndex
                    ? StorageContext.ConvertCellForLegacyDictionary(probe.Columns[ordinal], cells[ordinal])
                    : StorageContext.ConvertCellForLegacyDictionary(state.Build.Columns[ordinal], buildRow[ordinal]);
            }

            var groupKey = new GroupKey(keys);
            if (!state.Groups.TryGetValue(groupKey, out JoinGroup? group))
            {
                if (!fullyLoaded)
                {
                    var all = new bool[probe.Columns.Count];
                    Array.Fill(all, true);
                    LoadJoinCells(probe, rowId, rowBytes, typedRow, all, cells);
                    fullyLoaded = true;
                }

                var representative = new JoinedRow();
                for (int side = 0; side < 2; side++)
                {
                    bool isProbe = side == probeIndex;
                    JoinSide owner = isProbe ? probe : state.Build;
                    CellValue[] source = isProbe ? cells : buildRow;
                    var values = new Dictionary<string, object?>(owner.Columns.Count);
                    for (int c = 0; c < owner.Columns.Count; c++)
                    {
                        values[owner.Columns[c].Name] = StorageContext.ConvertCellForLegacyDictionary(owner.Columns[c], source[c]);
                    }

                    representative.Add(state.SideNames[side], new Row(values));
                }

                group = new JoinGroup(representative, state.Aggregates.Count);
                state.Groups[groupKey] = group;
                state.GroupOrder.Add(group);
            }

            for (int i = 0; i < state.Aggregates.Count; i++)
            {
                JoinAggregate aggregate = state.Aggregates[i];
                if (aggregate.Kind == JoinAggregateKind.CountRows)
                {
                    group.Counts[i]++;
                    continue;
                }

                bool fromProbe = aggregate.Side == probeIndex;
                CatalogColumn column = fromProbe ? probe.Columns[aggregate.Ordinal] : state.Build.Columns[aggregate.Ordinal];
                CellValue cell = fromProbe ? cells[aggregate.Ordinal] : buildRow[aggregate.Ordinal];
                if (cell.IsNull)
                {
                    continue;
                }

                switch (aggregate.Kind)
                {
                    case JoinAggregateKind.CountValues:
                        group.Counts[i]++;
                        break;
                    case JoinAggregateKind.Sum:
                        switch (cell.Type)
                        {
                            case CellType.Int32: group.Totals[i].Add((long)cell.AsInt32()); break;
                            case CellType.Int64: group.Totals[i].Add(cell.AsInt64()); break;
                            case CellType.Double: group.Totals[i].Add((double)(float)cell.AsDouble()); break;
                            default: group.Totals[i].Add(StorageContext.ConvertCellForLegacyDictionary(column, cell)!); break;
                        }

                        break;
                    case JoinAggregateKind.Avg:
                        group.Sums[i] += cell.Type switch
                        {
                            CellType.Int32 => cell.AsInt32(),
                            CellType.Int64 => cell.AsInt64(),
                            CellType.Double => (float)cell.AsDouble(),
                            _ => Convert.ToDouble(StorageContext.ConvertCellForLegacyDictionary(column, cell), System.Globalization.CultureInfo.InvariantCulture),
                        };
                        group.Counts[i]++;
                        break;
                    default:
                        // Enumerable.Min/Max over boxed values: nulls skipped, default comparer.
                        object? value = StorageContext.ConvertCellForLegacyDictionary(column, cell);
                        object? current = group.Extremes[i];
                        int order = current is null ? 0 : Comparer<object?>.Default.Compare(value, current);
                        if (current is null || (aggregate.Kind == JoinAggregateKind.Min ? order < 0 : order > 0))
                        {
                            group.Extremes[i] = value;
                        }

                        break;
                }
            }
        }

        return true;
    }
}
