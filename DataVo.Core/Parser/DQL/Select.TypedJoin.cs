using DataVo.Core.Enums;
using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Aggregations;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Utils;
using CatalogColumn = DataVo.Core.Models.Catalog.Column;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// The typed join path: stored tables joined by INNER and LEFT equalities on integer columns, filtered by
/// per-table <c>column op literal</c> conditions, and either aggregated (with or without GROUP BY) or returned as
/// rows: the top rows of an <c>ORDER BY ... LIMIT</c>, or every joined row (up to a LIMIT without ORDER BY).
/// </summary>
/// <remarks>
/// <para>
/// One table is streamed (the probe): the largest one when every join is INNER, the FROM table when a LEFT JOIN
/// must preserve it. With INNER joins only, a small hashed table may instead select the probe rows through an
/// index on the probe's join column (see <c>Select.TypedJoinIndex.cs</c>). Every other table is hashed by its join key, and each probe row is extended table by table
/// along the join steps. An INNER step drops a row with no match; a LEFT step pads it with NULLs.
/// </para>
/// <para>
/// A WHERE condition on the probe or on an INNER-joined table filters that table's rows before they are joined,
/// which gives the same rows. A condition on a LEFT-joined table is tested after its step, on the matched row or
/// on the NULLs, as SQL defines it. Only the cells the conditions, keys, groups, aggregates and sort keys read are
/// decoded from a probe row; whole rows are built as dictionaries only for a group's first row or a row that is
/// returned, in the general path's shape. A hashed table keeps only the columns read after the join (see
/// <c>Select.TypedJoinBuild.cs</c>).
/// </para>
/// </remarks>
internal partial class Select
{
    private sealed class JoinSide
    {
        public required string Name { get; init; }
        public required TableDetail Detail { get; init; }
        public required IReadOnlyList<CatalogColumn> Columns { get; init; }
        public required List<TypedPredicate> Predicates { get; init; }
        public required int ClusteredOrdinal { get; init; }
        public required bool ClusteredIsGuid { get; init; }

        /// <summary>Whether a LEFT JOIN adds this table: unmatched rows get NULLs, and its conditions run after the join.</summary>
        public bool Outer { get; set; }

        /// <summary>The cells of the NULL row a LEFT JOIN pads with.</summary>
        public CellValue[] NullCells => _nullCells ??= new CellValue[Columns.Count];

        /// <summary>Columns of a probe row read before it is matched: conditions and join keys.</summary>
        public bool[] FilterMask { get; set; } = [];

        /// <summary>Columns of a probe row read once it is part of a joined row: groups, aggregates, sort keys.</summary>
        public bool[] NeededMask { get; set; } = [];

        /// <summary>Columns of a hashed table kept for joined rows; null for the probe, whose rows are kept whole.</summary>
        public bool[]? KeepMask { get; set; }

        /// <summary>Whether a hashed table keeps nothing but its join key, so only its key counts are hashed.</summary>
        public bool KeyOnly { get; set; }

        private CellValue[]? _nullCells;
    }

    /// <summary>One join step: the table it adds, hashed by its key, and the joined column that supplies the key.</summary>
    private sealed class JoinStep(int side, int keyOrdinal, int sourceSide, int sourceOrdinal, bool outer)
    {
        public int Side { get; } = side;
        public int KeyOrdinal { get; } = keyOrdinal;
        public int SourceSide { get; } = sourceSide;
        public int SourceOrdinal { get; } = sourceOrdinal;
        public bool Outer { get; } = outer;

        /// <summary>The added table's rows by key, filled before the probe is streamed.</summary>
        public JoinHashTable Table { get; set; } = null!;
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

    private sealed class JoinRunState
    {
        public required JoinSide[] Sides { get; init; }
        public required JoinStep[] Steps { get; init; }
        public required int ProbeIndex { get; init; }

        /// <summary>The cells of each table in the joined row being built; the probe's are reused per row.</summary>
        public required CellValue[]?[] Current { get; init; }
        public bool ProbeNeededLoaded;
        public bool ProbeFullyLoaded;

        // Aggregates.
        public List<(int Side, int Ordinal)>? GroupColumns { get; init; }
        public List<JoinAggregate>? Aggregates { get; init; }
        public GroupLookup GroupKeys { get; } = new();

        /// <summary>The groups by group number (the order they first appeared in).</summary>
        public List<JoinGroup> GroupOrder { get; } = [];

        // Returned rows: the top rows of ORDER BY ... LIMIT, or (Unordered) the rows in the order they are joined.
        public List<(int Side, int Ordinal)>? OrderKeys { get; init; }
        public bool[]? Ascending { get; init; }
        public int TopLimit { get; init; }
        public bool Unordered { get; init; }
        public List<(object?[] Keys, long Sequence, CellValue[][] Cells)> Top { get; } = [];
        public long Sequence;

        /// <summary>Set once an unordered result has its LIMIT of rows: the scan stops.</summary>
        public bool Done;
    }

    /// <summary>Returns the joined rows (aggregated, the top rows, or every row up to a LIMIT), or null when the query needs the general path.</summary>
    private ListedTable? TryEvaluateTypedJoin()
    {
        if (_activeSnapshot is not null
            || ast.Ctes.Count > 0
            || !_model.JoinStatement.ContainsJoin()
            || _model.JoinStatement.Model.JoinConditions.Count == 0
            || _model.IsDistinct
            || _model.Database is null
            || _model.TableService is null
            || _model.FromTable is null
            || _model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0)
        {
            return null;
        }

        bool aggregated = _model.AggregateStatement.Model.Functions.Count > 0;
        if (!aggregated
            && (_model.GroupByStatement.Model.Columns.Count > 0
                || _model.GetHavingExpression() is not null
                || (_model.LimitTake.HasValue && _model.RowsNeededForLimit <= 0)))
        {
            return null;
        }

        // Rows without a bounded top-N are returned in the order they are joined, streaming the FROM table.
        bool topRows = !aggregated && _model.GetOrderByExpression() is { Columns.Count: > 0 } && _model.LimitTake.HasValue;
        bool unordered = !aggregated && !topRows;
        DataVoEngine engine = DataVoEngine.Current();
        if (!TryBuildJoinSides(engine, out JoinSide[] sides, out int fromIndex)
            || !TryPlanJoinSteps(engine, sides, fromIndex, streamFrom: unordered, out int probeIndex, out JoinStep[] steps))
        {
            return null;
        }

        ExpressionNode? where = _model.WhereStatement.IsEvaluatable() ? _model.WhereStatement.GetExpression() : null;
        if (where is not null && !TryCollectSidePredicates(
                where,
                reference => ResolveJoinColumn(reference, sides),
                [.. sides.Select(side => side.Columns)],
                [.. sides.Select(side => side.Predicates)]))
        {
            return null;
        }

        JoinSide probe = sides[probeIndex];
        probe.FilterMask = new bool[probe.Columns.Count];
        probe.NeededMask = new bool[probe.Columns.Count];
        MarkFilterColumns(probe.Predicates, probe.FilterMask);
        foreach (JoinStep step in steps.Where(step => step.SourceSide == probeIndex))
        {
            probe.FilterMask[step.SourceOrdinal] = true;
        }

        List<(int Side, int Ordinal)>? groupColumns = null;
        List<JoinAggregate>? aggregates = null;
        List<(int Side, int Ordinal)>? orderKeys = null;
        bool[]? ascending = null;
        if (aggregated)
        {
            if (!TryResolveJoinAggregates(sides, out groupColumns, out aggregates))
            {
                return null;
            }

            foreach ((int side, int ordinal) in groupColumns.Concat(aggregates.Where(a => a.Side >= 0).Select(a => (a.Side, a.Ordinal))))
            {
                if (side == probeIndex)
                {
                    probe.NeededMask[ordinal] = true;
                }
            }
        }
        else if (topRows)
        {
            if (!TryResolveJoinOrderKeys(sides, out orderKeys, out ascending))
            {
                return null;
            }

            foreach ((int side, int ordinal) in orderKeys.Where(key => key.Side == probeIndex))
            {
                probe.NeededMask[ordinal] = true;
            }
        }

        IEnumerable<(int Side, int Ordinal)> used = (groupColumns ?? [])
            .Concat(aggregates?.Where(a => a.Side >= 0).Select(a => (a.Side, a.Ordinal)) ?? [])
            .Concat(orderKeys ?? []);
        MarkHashedColumns(sides, probeIndex, steps, used);

        string databaseName = _model.Database;
        if (!TryBuildJoinTables(engine, sides, steps, databaseName))
        {
            return null;
        }

        var state = new JoinRunState
        {
            Sides = sides,
            Steps = steps,
            ProbeIndex = probeIndex,
            Current = new CellValue[]?[sides.Length],
            GroupColumns = groupColumns,
            Aggregates = aggregates,
            OrderKeys = orderKeys,
            Ascending = ascending,
            TopLimit = aggregated ? 0 : _model.LimitTake.HasValue ? _model.RowsNeededForLimit : int.MaxValue,
            Unordered = unordered,
        };
        state.Current[probeIndex] = new CellValue[probe.Columns.Count];
        bool streamed = TryIndexDrivenProbeRows(engine, sides, steps, probeIndex, databaseName) is { } indexedRows
            ? engine.StorageContext.VisitRowsByKey(probe.Detail.TableName, databaseName, indexedRows, ref state, VisitJoinProbeRow)
            : engine.StorageContext.ScanTableRows(probe.Detail.TableName, databaseName, null, null, ref state, VisitJoinProbeRow);
        if (!streamed)
        {
            return null;
        }

        return aggregated ? FinishJoinAggregates(state) : FinishJoinTop(state);
    }

    /// <summary>
    /// One side per table of the join conditions, the FROM table first and the others in the order they are
    /// written. False for anything but INNER and LEFT equalities of integer columns of distinct stored tables.
    /// </summary>
    private bool TryBuildJoinSides(DataVoEngine engine, out JoinSide[] sides, out int fromIndex)
    {
        sides = [];
        fromIndex = -1;
        var found = new List<JoinSide>();
        foreach (JoinModel.JoinCondition condition in _model.JoinStatement.Model.JoinConditions)
        {
            if (!condition.JoinType.Equals(JoinTypes.INNER, StringComparison.OrdinalIgnoreCase)
                && !condition.JoinType.Equals(JoinTypes.LEFT, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            foreach (Column column in new[] { condition.LeftColumn, condition.RightColumn })
            {
                JoinSide? side = found.FirstOrDefault(candidate => candidate.Name.Equals(column.TableName, StringComparison.OrdinalIgnoreCase));
                if (side is null)
                {
                    side = BuildJoinSide(engine, column);
                    if (side is null || found.Any(other => other.Detail.TableName.Equals(side.Detail.TableName, StringComparison.OrdinalIgnoreCase)))
                    {
                        return false; // not a stored table, or a table joined to itself
                    }

                    found.Add(side);
                }

                int ordinal = OrdinalOf(side.Columns, column.ColumnName);
                if (ordinal < 0 || !IsIntegerColumn(side.Columns[ordinal]))
                {
                    return false;
                }
            }
        }

        fromIndex = found.FindIndex(side => side.Detail.TableName.Equals(_model.FromTable.TableName, StringComparison.OrdinalIgnoreCase));
        if (fromIndex < 0 || found.Count != _model.JoinStatement.Model.JoinConditions.Count + 1)
        {
            return false; // the conditions do not form a tree over the FROM table and each joined table once
        }

        JoinSide from = found[fromIndex];
        found.RemoveAt(fromIndex);
        found.Insert(0, from);
        fromIndex = 0;
        sides = [.. found];
        return true;
    }

    /// <summary>
    /// Chooses the probe and orders the steps so each adds one table keyed by a column of a table already joined.
    /// With a LEFT JOIN the FROM table is the probe and the steps follow the written order, which SQL's
    /// left-to-right evaluation requires; with INNER joins only, the largest table is the probe and any order
    /// that connects the tables gives the same rows. <paramref name="streamFrom"/> keeps the FROM table as the
    /// probe, so rows returned without an ORDER BY come in the FROM table's order.
    /// </summary>
    private bool TryPlanJoinSteps(DataVoEngine engine, JoinSide[] sides, int fromIndex, bool streamFrom, out int probeIndex, out JoinStep[] steps)
    {
        List<JoinModel.JoinCondition> conditions = _model.JoinStatement.Model.JoinConditions;
        bool anyOuter = conditions.Any(condition => condition.JoinType.Equals(JoinTypes.LEFT, StringComparison.OrdinalIgnoreCase));
        probeIndex = fromIndex;
        if (!anyOuter && !streamFrom)
        {
            long largest = EstimateRows(engine, sides[fromIndex], _model.Database!);
            for (int i = 0; i < sides.Length; i++)
            {
                long rows = EstimateRows(engine, sides[i], _model.Database!);
                if (rows > largest)
                {
                    (probeIndex, largest) = (i, rows);
                }
            }
        }

        var joined = new List<int> { probeIndex };
        var planned = new List<JoinStep>(conditions.Count);
        var remaining = new List<JoinModel.JoinCondition>(conditions);
        while (remaining.Count > 0)
        {
            int next = -1;
            JoinStep? step = null;
            for (int i = 0; i < remaining.Count && step is null; i++)
            {
                step = TryJoinStep(sides, joined, remaining[i]);
                next = step is null ? -1 : i;
                if (anyOuter && step is null)
                {
                    break; // written order only
                }
            }

            if (step is null)
            {
                steps = [];
                return false;
            }

            if (step.Outer)
            {
                sides[step.Side].Outer = true;
            }

            planned.Add(step);
            joined.Add(step.Side);
            remaining.RemoveAt(next);
        }

        steps = [.. planned];
        return true;
    }

    /// <summary>The step a condition makes when exactly one of its tables is joined already; for a LEFT JOIN, the new table must be the one it adds.</summary>
    private static JoinStep? TryJoinStep(JoinSide[] sides, List<int> joined, JoinModel.JoinCondition condition)
    {
        int left = Array.FindIndex(sides, side => side.Name.Equals(condition.LeftColumn.TableName, StringComparison.OrdinalIgnoreCase));
        int right = Array.FindIndex(sides, side => side.Name.Equals(condition.RightColumn.TableName, StringComparison.OrdinalIgnoreCase));
        bool leftJoined = joined.Contains(left);
        bool rightJoined = joined.Contains(right);
        if (leftJoined == rightJoined)
        {
            return null;
        }

        (int source, Column sourceColumn, int target, Column targetColumn) = leftJoined
            ? (left, condition.LeftColumn, right, condition.RightColumn)
            : (right, condition.RightColumn, left, condition.LeftColumn);
        bool outer = condition.JoinType.Equals(JoinTypes.LEFT, StringComparison.OrdinalIgnoreCase);
        return new JoinStep(
            target,
            OrdinalOf(sides[target].Columns, targetColumn.ColumnName),
            source,
            OrdinalOf(sides[source].Columns, sourceColumn.ColumnName),
            outer);
    }

    private bool TryResolveJoinAggregates(JoinSide[] sides, out List<(int Side, int Ordinal)> groupColumns, out List<JoinAggregate> aggregates)
    {
        groupColumns = [];
        aggregates = [];
        foreach (Column column in _model.GroupByStatement.Model.Columns)
        {
            if (ResolveJoinSideColumn(column.TableName, column.ColumnName, sides) is not { } resolved)
            {
                return false;
            }

            groupColumns.Add(resolved);
        }

        foreach (Aggregation function in _model.AggregateStatement.Model.Functions)
        {
            // A plain column argument (its reference is kept as the expression) or COUNT(*); nothing computed.
            // DISTINCT aggregates run on the general aggregation path.
            if (function.Field is null
                || function.IsDistinct
                || function.ArgumentExpression is not (null or ColumnRefNode or ResolvedColumnRefNode))
            {
                return false;
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
                return false;
            }

            if (kind == JoinAggregateKind.CountRows)
            {
                aggregates.Add(new JoinAggregate(function, kind.Value, -1, -1));
                continue;
            }

            if (ResolveJoinSideColumn(function.Field.TableName, function.Field.ColumnName, sides) is not { } target)
            {
                return false;
            }

            aggregates.Add(new JoinAggregate(function, kind.Value, target.Side, target.Ordinal));
        }

        return true;
    }

    /// <summary>The ORDER BY keys as table columns: a column, or a SELECT alias of one. False for anything computed.</summary>
    private bool TryResolveJoinOrderKeys(JoinSide[] sides, out List<(int Side, int Ordinal)> keys, out bool[] ascending)
    {
        OrderByNode orderBy = _model.GetOrderByExpression()!;
        keys = new List<(int Side, int Ordinal)>(orderBy.Columns.Count);
        ascending = new bool[orderBy.Columns.Count];
        for (int i = 0; i < orderBy.Columns.Count; i++)
        {
            string token = orderBy.Columns[i].Column.Name;
            ExpressionNode? reference = _model.GetSelectColumnByAlias(token)?.Expression;
            if (reference is null)
            {
                int dot = token.LastIndexOf('.');
                reference = new ColumnRefNode { TableOrAlias = dot > 0 ? token[..dot] : null, Column = dot > 0 ? token[(dot + 1)..] : token };
            }

            if (reference is not (ColumnRefNode or ResolvedColumnRefNode) || ResolveJoinColumn(reference, sides) is not { } column)
            {
                return false;
            }

            keys.Add((column.Side, OrdinalOf(sides[column.Side].Columns, column.Column)));
            ascending[i] = orderBy.Columns[i].IsAscending;
        }

        return true;
    }

    private static object? FinishJoinAggregate(JoinAggregateKind kind, JoinGroup group, int i) => kind switch
    {
        // The general aggregate functions: COUNT is an int, SUM is an integer for integers, AVG a double; both NULL over no values.
        JoinAggregateKind.CountRows or JoinAggregateKind.CountValues => group.Counts[i],
        JoinAggregateKind.Sum => group.Totals[i].Result,
        JoinAggregateKind.Avg => group.Counts[i] == 0 ? null : group.Sums[i] / group.Counts[i],
        _ => group.Extremes[i],
    };

    /// <summary>
    /// One row per group: the group's first joined row with the aggregates added. With no joined row, a query
    /// without GROUP BY still returns one row, as the general path does: COUNT is 0, the others NULL, and no
    /// table columns.
    /// </summary>
    private ListedTable FinishJoinAggregates(JoinRunState state)
    {
        List<JoinAggregate> aggregates = state.Aggregates!;
        if (state.GroupOrder.Count == 0 && state.GroupColumns!.Count == 0)
        {
            state.GroupOrder.Add(new JoinGroup(new JoinedRow(), aggregates.Count));
        }

        var rows = new List<JoinedRow>(state.GroupOrder.Count);
        foreach (JoinGroup group in state.GroupOrder)
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

    /// <summary>The returned rows, as joined rows of the kept columns; ORDER BY and LIMIT then run over them as usual.</summary>
    private static ListedTable FinishJoinTop(JoinRunState state)
    {
        var rows = new List<JoinedRow>(state.Top.Count);
        foreach ((object?[] _, long _, CellValue[][] cells) in state.Top)
        {
            rows.Add(BuildJoinedRow(state.Sides, cells));
        }

        return new ListedTable(rows);
    }

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
        if (columns.Count == 0)
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
            ClusteredOrdinal = clustered?.Ordinal ?? -1,
            ClusteredIsGuid = clustered is not null
                && string.Equals(columns[clustered.Ordinal].Type, "GUID", StringComparison.OrdinalIgnoreCase),
        };
    }

    private static bool IsIntegerColumn(CatalogColumn column) =>
        column.Type.Equals("INT", StringComparison.OrdinalIgnoreCase) || column.Type.Equals("BIGINT", StringComparison.OrdinalIgnoreCase);

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

    private static void LoadJoinCells(JoinSide side, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells, bool[] mask, CellValue[] cells)
    {
        if (!typedCells.IsEmpty)
        {
            for (int i = 0; i < mask.Length; i++)
            {
                if (mask[i])
                {
                    cells[i] = typedCells[i];
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

    private static bool VisitJoinProbeRow(ref JoinRunState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        JoinSide probe = state.Sides[state.ProbeIndex];
        CellValue[] cells = state.Current[state.ProbeIndex]!;
        LoadJoinCells(probe, rowId, rowBytes, typedCells, probe.FilterMask, cells);
        if (!MatchesAll(probe.Predicates, cells, probe.Columns))
        {
            return true;
        }

        state.ProbeNeededLoaded = false;
        state.ProbeFullyLoaded = false;
        ExtendJoinedRow(state, 0, rowId, rowBytes, typedCells);
        return !state.Done;
    }

    /// <summary>Adds the table of step <paramref name="stepIndex"/> to the joined row in every way it matches, then the next step.</summary>
    private static void ExtendJoinedRow(JoinRunState state, int stepIndex, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        if (stepIndex == state.Steps.Length)
        {
            EmitJoinedRow(state, rowId, rowBytes, typedCells);
            return;
        }

        JoinStep step = state.Steps[stepIndex];
        JoinSide side = state.Sides[step.Side];
        if (TryJoinKey(state.Current[step.SourceSide]![step.SourceOrdinal], out long key)
            && step.Table.TryMatch(key, out JoinMatches matches))
        {
            for (int i = 0; i < matches.Count; i++)
            {
                CellValue[] match = matches[i];

                // A matched row that fails a condition on its LEFT-joined table is dropped, not NULL-padded.
                if (step.Outer && !MatchesAll(side.Predicates, match, side.Columns))
                {
                    continue;
                }

                state.Current[step.Side] = match;
                ExtendJoinedRow(state, stepIndex + 1, rowId, rowBytes, typedCells);
            }
        }
        else if (step.Outer && MatchesAll(side.Predicates, side.NullCells, side.Columns))
        {
            state.Current[step.Side] = side.NullCells;
            ExtendJoinedRow(state, stepIndex + 1, rowId, rowBytes, typedCells);
        }
    }

    private static void EmitJoinedRow(JoinRunState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        // Most streamed rows usually match nothing; the columns only a joined row reads are decoded at its first one.
        if (!state.ProbeNeededLoaded)
        {
            JoinSide probe = state.Sides[state.ProbeIndex];
            LoadJoinCells(probe, rowId, rowBytes, typedCells, probe.NeededMask, state.Current[state.ProbeIndex]!);
            state.ProbeNeededLoaded = true;
        }

        if (state.Aggregates is not null)
        {
            AccumulateJoinedRow(state, rowId, rowBytes, typedCells);
        }
        else if (state.Unordered)
        {
            AppendJoinedRow(state, rowId, rowBytes, typedCells);
        }
        else
        {
            OfferJoinedRow(state, rowId, rowBytes, typedCells);
        }
    }

    /// <summary>Keeps a joined row of an unordered result, and stops the scan once the LIMIT is reached.</summary>
    private static void AppendJoinedRow(JoinRunState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        if (state.Done)
        {
            return;
        }

        LoadWholeProbeRow(state, rowId, rowBytes, typedCells);
        state.Top.Add(([], state.Sequence++, SnapshotJoinedRow(state)));
        state.Done = state.Top.Count >= state.TopLimit;
    }

    /// <summary>
    /// The cells of the joined row being built, safe to keep: the probe's cells and a key-only table's shared
    /// row are rewritten for the next row, so they are copied; other hashed rows are never changed.
    /// </summary>
    private static CellValue[][] SnapshotJoinedRow(JoinRunState state)
    {
        var snapshot = new CellValue[state.Sides.Length][];
        for (int side = 0; side < snapshot.Length; side++)
        {
            CellValue[] cells = state.Current[side]!;
            snapshot[side] = side == state.ProbeIndex || state.Sides[side].KeyOnly ? (CellValue[])cells.Clone() : cells;
        }

        return snapshot;
    }

    /// <summary>Decodes the rest of the probe row once a joined row from it is kept whole.</summary>
    private static void LoadWholeProbeRow(JoinRunState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        if (state.ProbeFullyLoaded)
        {
            return;
        }

        JoinSide probe = state.Sides[state.ProbeIndex];
        var all = new bool[probe.Columns.Count];
        Array.Fill(all, true);
        LoadJoinCells(probe, rowId, rowBytes, typedCells, all, state.Current[state.ProbeIndex]!);
        state.ProbeFullyLoaded = true;
    }

    private static JoinedRow BuildJoinedRow(JoinSide[] sides, CellValue[]?[] cells)
    {
        var row = new JoinedRow();
        for (int side = 0; side < sides.Length; side++)
        {
            // A hashed table has only its kept columns; reading another would be a planning bug, so it fails loudly.
            JoinSide owner = sides[side];
            CellValue[] source = cells[side]!;
            bool[]? keep = owner.KeepMask;
            var values = new Dictionary<string, object?>(owner.Columns.Count);
            for (int c = 0; c < owner.Columns.Count; c++)
            {
                if (keep is null || keep[c])
                {
                    values[owner.Columns[c].Name] = StorageContext.ConvertCellForLegacyDictionary(owner.Columns[c], source[c]);
                }
            }

            row.Add(owner.Name, new Row(values));
        }

        return row;
    }

    /// <summary>
    /// Keeps the best <c>TopLimit</c> joined rows, compared as the general ORDER BY compares them; ties keep the
    /// order they were produced in. The probe row is decoded whole and copied only when it enters the list.
    /// </summary>
    private static void OfferJoinedRow(JoinRunState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        long sequence = state.Sequence++;
        List<(int Side, int Ordinal)> orderKeys = state.OrderKeys!;
        var keys = new object?[orderKeys.Count];
        for (int i = 0; i < keys.Length; i++)
        {
            (int side, int ordinal) = orderKeys[i];
            keys[i] = StorageContext.ConvertCellForLegacyDictionary(state.Sides[side].Columns[ordinal], state.Current[side]![ordinal]);
        }

        List<(object?[] Keys, long Sequence, CellValue[][] Cells)> top = state.Top;
        if (top.Count >= state.TopLimit && CompareJoinedRows(state.Ascending!, keys, sequence, top[^1].Keys, top[^1].Sequence) >= 0)
        {
            return;
        }

        int low = 0;
        int high = top.Count;
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            if (CompareJoinedRows(state.Ascending!, top[mid].Keys, top[mid].Sequence, keys, sequence) <= 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        LoadWholeProbeRow(state, rowId, rowBytes, typedCells);
        top.Insert(low, (keys, sequence, SnapshotJoinedRow(state)));
        if (top.Count > state.TopLimit)
        {
            top.RemoveAt(top.Count - 1);
        }
    }

    private static int CompareJoinedRows(bool[] ascending, object?[] left, long leftSequence, object?[] right, long rightSequence)
    {
        for (int i = 0; i < left.Length; i++)
        {
            int order = DynamicObjectComparer.Instance.Compare(left[i], right[i]);
            if (order != 0)
            {
                return ascending[i] ? order : -order;
            }
        }

        return leftSequence.CompareTo(rightSequence);
    }

    /// <summary>The group of the joined row being built, created (with the row as its representative) at its first row.</summary>
    private static JoinGroup FindJoinGroup(JoinRunState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        List<(int Side, int Ordinal)> groupColumns = state.GroupColumns!;
        if (groupColumns.Count == 0 && state.GroupOrder.Count == 1)
        {
            return state.GroupOrder[0]; // no GROUP BY: one group, no key to build
        }

        int number;
        bool added;
        if (groupColumns.Count == 1)
        {
            // One key column: looked up by its value, with nothing allocated per joined row.
            (int side, int ordinal) = groupColumns[0];
            number = state.GroupKeys.FindOrAdd(state.Sides[side].Columns[ordinal], state.Current[side]![ordinal], out added);
        }
        else
        {
            object?[] keys = groupColumns.Count == 0 ? [] : new object?[groupColumns.Count];
            for (int i = 0; i < keys.Length; i++)
            {
                (int side, int ordinal) = groupColumns[i];
                keys[i] = StorageContext.ConvertCellForLegacyDictionary(state.Sides[side].Columns[ordinal], state.Current[side]![ordinal]);
            }

            number = state.GroupKeys.FindOrAdd(keys, out added);
        }

        if (added)
        {
            LoadWholeProbeRow(state, rowId, rowBytes, typedCells);
            state.GroupOrder.Add(new JoinGroup(BuildJoinedRow(state.Sides, state.Current), state.Aggregates!.Count));
        }

        return state.GroupOrder[number];
    }

    private static void AccumulateJoinedRow(JoinRunState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        List<JoinAggregate> aggregates = state.Aggregates!;
        JoinGroup group = FindJoinGroup(state, rowId, rowBytes, typedCells);

        for (int i = 0; i < aggregates.Count; i++)
        {
            JoinAggregate aggregate = aggregates[i];
            if (aggregate.Kind == JoinAggregateKind.CountRows)
            {
                group.Counts[i]++;
                continue;
            }

            CatalogColumn column = state.Sides[aggregate.Side].Columns[aggregate.Ordinal];
            CellValue cell = state.Current[aggregate.Side]![aggregate.Ordinal];
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
                        case CellType.Double: group.Totals[i].Add(StorageContext.FloatingValue(column, cell)); break;
                        default: group.Totals[i].Add(StorageContext.ConvertCellForLegacyDictionary(column, cell)!); break;
                    }

                    break;
                case JoinAggregateKind.Avg:
                    group.Sums[i] += cell.Type switch
                    {
                        CellType.Int32 => cell.AsInt32(),
                        CellType.Int64 => cell.AsInt64(),
                        CellType.Double => StorageContext.FloatingValue(column, cell),
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
}
