using DataVo.Core.Contracts.Results;
using DataVo.Core.Execution;
using DataVo.Core.Execution.Volcano;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using AggregateFunction = DataVo.Core.Execution.Volcano.HashAggregateOperator.AggregateFunction;

namespace DataVo.Core;

/// <summary>
/// The prepared fast path for a SELECT list made only of plain aggregates (<c>COUNT(*)</c>, <c>COUNT</c>,
/// <c>SUM</c>, <c>AVG</c>, <c>MIN</c>, <c>MAX</c> of a numeric column) over a key range of one table, such as
/// <c>SELECT COUNT(*), SUM(Score) FROM Items WHERE Id &gt;= @lo AND Id &lt; @hi</c>. The rows of the range are read
/// as typed cells or bytes and folded into per-thread accumulators: no row dictionary, row lock or boxed cell is
/// made per row, and the one result row is the thread's reused result slot. Results are typed as the general path
/// types them (see <c>Select.TypedScanGroups</c>).
/// </summary>
public sealed partial class DataVoPreparedStatement
{
    /// <summary>One aggregate of the SELECT list: its function, the column it reads (-1 for <c>COUNT(*)</c>) and its output field.</summary>
    private readonly record struct CompiledAggregate(AggregateFunction Function, int Ordinal, string Field);

    /// <summary>The running value of one aggregate over the rows read so far.</summary>
    private struct AggregateAccumulator
    {
        public long Count;
        public SqlSumAccumulator Sum;
        public double AverageSum;
        public CellValue Extreme;
        public bool HasExtreme;
    }

    /// <summary>The state a range read folds rows into.</summary>
    private struct AggregateScanState
    {
        public required CompiledSelectPlan Plan;
        public required ThreadSelectWorkspace Workspace;
        public required AggregateAccumulator[] Accumulators;
    }

    /// <summary>Whether every SELECT column is a non-DISTINCT aggregate, the only aggregate shape the fast path plans.</summary>
    private static bool IsAggregateOnlySelectList(List<SelectColumnNode> columns)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].Expression is not AggregateExpressionNode { IsDistinct: false })
            {
                return false;
            }
        }

        return columns.Count > 0;
    }

    /// <summary>
    /// Resolves an aggregate-only SELECT list to its aggregates and output fields; false when one is outside what
    /// the fast path computes exactly as the general path does (an argument that is not an exactly named numeric
    /// column of the table, or two outputs with the same name, which the general path rejects).
    /// </summary>
    private static bool TryResolveAggregateProjection(
        List<SelectColumnNode> selectColumns,
        string tableName,
        string? tableAlias,
        IReadOnlyList<Column> columns,
        FastColumnType[] columnTypes,
        out List<string> fields,
        out CompiledAggregate[] aggregates)
    {
        fields = new List<string>(selectColumns.Count);
        aggregates = new CompiledAggregate[selectColumns.Count];
        var headers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < selectColumns.Count; i++)
        {
            SelectColumnNode node = selectColumns[i];
            if (node.Expression is not AggregateExpressionNode { IsDistinct: false } aggregate
                || !TryResolveAggregateFunction(aggregate.FunctionName, out AggregateFunction function))
            {
                return false;
            }

            int ordinal = -1;
            if (aggregate.IsStar)
            {
                if (function != AggregateFunction.Count)
                {
                    return false;
                }
            }
            else
            {
                ordinal = ExactColumnOrdinal(aggregate.Argument, tableName, tableAlias, columns);
                if (ordinal < 0 || (function != AggregateFunction.Count && !IsNumeric(columnTypes[ordinal])))
                {
                    return false;
                }
            }

            string header = AggregateExpressionFormatter.BuildHeader(aggregate);
            string field = string.IsNullOrWhiteSpace(node.Alias) ? header : node.Alias!;
            if (!headers.Add(header) || !names.Add(field))
            {
                return false;
            }

            fields.Add(field);
            aggregates[i] = new CompiledAggregate(function, ordinal, field);
        }

        return aggregates.Length > 0;
    }

    private static bool TryResolveAggregateFunction(string name, out AggregateFunction function)
    {
        switch (name.ToUpperInvariant())
        {
            case "COUNT": function = AggregateFunction.Count; return true;
            case "SUM": function = AggregateFunction.Sum; return true;
            case "AVG": function = AggregateFunction.Avg; return true;
            case "MIN": function = AggregateFunction.Min; return true;
            case "MAX": function = AggregateFunction.Max; return true;
            default: function = default; return false;
        }
    }

    /// <summary>
    /// The ordinal of an aggregate argument that names a column of the table exactly as declared, unqualified or
    /// qualified by the table or its alias as written; -1 otherwise. Names in other cases are left to the general
    /// path, whose resolution of them differs by position.
    /// </summary>
    private static int ExactColumnOrdinal(ExpressionNode? argument, string tableName, string? tableAlias, IReadOnlyList<Column> columns)
    {
        if (argument is not ColumnRefNode column
            || (!string.IsNullOrEmpty(column.TableOrAlias)
                && !string.Equals(column.TableOrAlias, tableName, StringComparison.Ordinal)
                && !string.Equals(column.TableOrAlias, tableAlias, StringComparison.Ordinal)))
        {
            return -1;
        }

        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, column.Column, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }

    private static bool IsNumeric(FastColumnType type) =>
        type is FastColumnType.Int or FastColumnType.BigInt or FastColumnType.Float or FastColumnType.Double;

    /// <summary>
    /// Folds the rows of a bound key range into the plan's aggregates. False when the general path must answer:
    /// no key range bounds the read (a full scan, which the general path may answer from a columnar index), the
    /// key lane cannot list the range, the storage cannot be read directly, or a row writer started meanwhile.
    /// </summary>
    private bool TryExecuteAggregateRange(
        CompiledSelectPlan plan,
        ThreadSelectWorkspace workspace,
        ReadOnlySpan<BoundScanPredicate> predicates,
        RowKey? clusteredLower,
        RowKey? clusteredUpper,
        long writeLockEpoch,
        out QueryResult? result)
    {
        result = null;
        List<RowKey>? keyRangeRows = null;
        if (plan.PrimaryKeyRangeOrdinal >= 0)
        {
            keyRangeRows = TryPrimaryKeyRangeRows(plan, predicates, workspace);
            if (keyRangeRows is null)
            {
                return false;
            }
        }
        else if (clusteredLower is null && clusteredUpper is null)
        {
            return false;
        }

        AggregateAccumulator[] accumulators = workspace.Accumulators;
        Array.Clear(accumulators, 0, accumulators.Length);
        var state = new AggregateScanState { Plan = plan, Workspace = workspace, Accumulators = accumulators };
        bool read = keyRangeRows is not null
            ? _engine.StorageContext.VisitRowsByKey(plan.TableName, plan.DatabaseName, keyRangeRows, ref state, VisitAggregateRow, plan.ReadMask)
            : _engine.StorageContext.ScanTableRows(plan.TableName, plan.DatabaseName, clusteredLower, clusteredUpper, ref state, VisitAggregateRow, plan.ReadMask);

        // A writer that started during the read could be seen half applied; the general path reads under row locks.
        if (!read || _engine.LockManager.RowWriteLockEpoch != writeLockEpoch)
        {
            return false;
        }

        result = BuildAggregateResult(plan, workspace, accumulators);
        return true;
    }

    /// <summary>The result when no row can match: one row whose COUNTs are 0 and whose other aggregates are NULL.</summary>
    private static QueryResult AggregateOverNoRows(CompiledSelectPlan plan, ThreadSelectWorkspace workspace)
    {
        Array.Clear(workspace.Accumulators, 0, workspace.Accumulators.Length);
        return BuildAggregateResult(plan, workspace, workspace.Accumulators);
    }

    /// <summary>Tests one row against the WHERE and, when it matches, adds it to every aggregate.</summary>
    private static bool VisitAggregateRow(
        ref AggregateScanState state,
        RowKey rowId,
        ReadOnlySpan<byte> rowBytes,
        ReadOnlySpan<CellValue> typedCells)
    {
        CompiledSelectPlan plan = state.Plan;
        CellValue[] cells = state.Workspace.Cells;
        if (plan.Predicates.Length > 0)
        {
            LoadCells(plan, cells, rowId, rowBytes, typedCells, plan.FilterMask);
            BoundScanPredicate[] predicates = state.Workspace.BoundPredicates;
            for (int i = 0; i < plan.Predicates.Length; i++)
            {
                if (!MatchesBoundPredicate(predicates[i], cells[predicates[i].Ordinal], plan.Columns[predicates[i].Ordinal]))
                {
                    return true;
                }
            }
        }

        LoadCells(plan, cells, rowId, rowBytes, typedCells, plan.RestMask);
        CompiledAggregate[] aggregates = plan.Aggregates!;
        for (int i = 0; i < aggregates.Length; i++)
        {
            Accumulate(ref state.Accumulators[i], aggregates[i], plan.Columns, cells);
        }

        return true;
    }

    /// <summary>Adds one matched row's value to an aggregate, as <c>Select.TypedScan.Accumulate</c> does.</summary>
    private static void Accumulate(ref AggregateAccumulator accumulator, CompiledAggregate aggregate, IReadOnlyList<Column> columns, CellValue[] cells)
    {
        if (aggregate.Ordinal < 0)
        {
            accumulator.Count++;
            return;
        }

        CellValue cell = cells[aggregate.Ordinal];
        if (cell.IsNull)
        {
            return;
        }

        Column column = columns[aggregate.Ordinal];
        switch (aggregate.Function)
        {
            case AggregateFunction.Count:
                accumulator.Count++;
                break;
            case AggregateFunction.Sum:
                switch (cell.Type)
                {
                    case CellType.Int32: accumulator.Sum.Add((long)cell.AsInt32()); break;
                    case CellType.Int64: accumulator.Sum.Add(cell.AsInt64()); break;
                    case CellType.Double: accumulator.Sum.Add(StorageContext.FloatingValue(column, cell)); break;
                    default: accumulator.Sum.Add(StorageContext.ConvertCellForLegacyDictionary(column, cell)!); break;
                }

                break;
            case AggregateFunction.Avg:
                accumulator.AverageSum += cell.Type switch
                {
                    CellType.Int32 => cell.AsInt32(),
                    CellType.Int64 => cell.AsInt64(),
                    CellType.Double => StorageContext.FloatingValue(column, cell),
                    _ => Convert.ToDouble(StorageContext.ConvertCellForLegacyDictionary(column, cell), System.Globalization.CultureInfo.InvariantCulture),
                };
                accumulator.Count++;
                break;
            default:
                if (!accumulator.HasExtreme || BeatsExtreme(column, cell, accumulator.Extreme, aggregate.Function == AggregateFunction.Min))
                {
                    accumulator.Extreme = cell;
                    accumulator.HasExtreme = true;
                }

                break;
        }
    }

    /// <summary>Whether <paramref name="cell"/> is strictly below (MIN) or above (MAX) the kept value; ties keep the first.</summary>
    private static bool BeatsExtreme(Column column, CellValue cell, CellValue kept, bool min)
    {
        int order = cell.Type is CellType.Int32 or CellType.Int64 && kept.Type is CellType.Int32 or CellType.Int64
            ? AsInt64(cell).CompareTo(AsInt64(kept))
            : NumericValue(column, cell).CompareTo(NumericValue(column, kept));
        return min ? order < 0 : order > 0;
    }

    private static long AsInt64(CellValue cell) => cell.Type == CellType.Int32 ? cell.AsInt32() : cell.AsInt64();

    private static double NumericValue(Column column, CellValue cell) => cell.Type switch
    {
        CellType.Int32 => cell.AsInt32(),
        CellType.Int64 => cell.AsInt64(),
        _ => StorageContext.FloatingValue(column, cell),
    };

    /// <summary>
    /// The one result row, in the thread's next reused slot: COUNTs as <see cref="long"/>, SUM as
    /// <see cref="SqlSumAccumulator.Result"/> gives it, AVG as a <see cref="double"/>, MIN and MAX as the column's
    /// own value; SUM, AVG, MIN and MAX of no value are NULL.
    /// </summary>
    private static QueryResult BuildAggregateResult(CompiledSelectPlan plan, ThreadSelectWorkspace workspace, AggregateAccumulator[] accumulators)
    {
        RowResultSlot slot = workspace.NextSlot();
        CompiledAggregate[] aggregates = plan.Aggregates!;
        for (int i = 0; i < aggregates.Length; i++)
        {
            ref AggregateAccumulator accumulator = ref accumulators[i];
            CompiledAggregate aggregate = aggregates[i];
            slot.Row[aggregate.Field] = aggregate.Function switch
            {
                AggregateFunction.Count => slot.BoxInt64(i, accumulator.Count),
                AggregateFunction.Sum => accumulator.Sum.Count == 0 ? null
                    : accumulator.Sum.IsInteger ? slot.BoxInt64(i, accumulator.Sum.Integer) : slot.BoxDouble(i, accumulator.Sum.Real),
                AggregateFunction.Avg => accumulator.Count == 0 ? null : slot.BoxDouble(i, accumulator.AverageSum / accumulator.Count),
                _ => accumulator.HasExtreme ? BoxExtreme(slot, i, plan.Columns[aggregate.Ordinal], plan.ColumnTypes[aggregate.Ordinal], accumulator.Extreme) : null,
            };
        }

        return slot.HitResult;
    }

    /// <summary>A MIN or MAX value boxed as <see cref="StorageContext.ConvertCellForLegacyDictionary"/> would box it, in the slot's reused boxes.</summary>
    private static object? BoxExtreme(RowResultSlot slot, int index, Column column, FastColumnType type, CellValue cell) => cell.Type switch
    {
        CellType.Double when type == FastColumnType.Float => slot.BoxFloat(index, (float)cell.AsDouble()),
        CellType.Double => slot.BoxDouble(index, cell.AsDouble()),
        CellType.Int32 when type != FastColumnType.Float => slot.BoxInt32(index, cell.AsInt32()),
        CellType.Int64 when type != FastColumnType.Float => slot.BoxInt64(index, cell.AsInt64()),
        _ => StorageContext.ConvertCellForLegacyDictionary(column, cell),
    };
}
