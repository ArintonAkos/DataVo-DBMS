using System.Globalization;
using DataVo.Core.Enums;
using DataVo.Core.Execution.Volcano;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// GROUP BY answered from an index: a query that groups a whole table by one indexed integer column and only
/// counts rows, such as <c>SELECT CustomerId, COUNT(*) AS N FROM Orders GROUP BY CustomerId HAVING COUNT(*) &gt;= 12
/// ORDER BY N DESC, CustomerId LIMIT 100</c>, reads each key's row count from the index instead of scanning and
/// hashing the table. Every key the index yields is a finished group, so HAVING and the top-N run as the groups
/// stream past, and only the groups still in the running are kept: about 125,000 groups no longer become 125,000
/// hash entries.
/// </summary>
/// <remarks>
/// The plan applies only outside a transaction (an index holds committed rows), with no WHERE, when every aggregate
/// is <c>COUNT(*)</c> or <c>COUNT</c> of the key, and when ORDER BY names the key, so the order of the result does
/// not depend on the order groups are found in. NULL keys form one group, as on the scan path; indexes do not hold
/// them, so for a column that can be NULL that group is counted from the rows, reading only the column's null flag.
/// Like a scan outside a transaction, the plan does not freeze the table: a row a concurrent writer changes between
/// NULL and a key while the query runs may be counted on either side, or on both.
/// </remarks>
internal partial class Select
{
    /// <summary>
    /// Answers the aggregate scan in <paramref name="state"/> from an index when the query qualifies. Null when it
    /// does not (the caller scans); <see cref="IndexGroupOutcome.Failed"/> when the index stopped answering part way,
    /// so the gathered groups are unusable and the caller takes the general path.
    /// </summary>
    private ListedTable? TryGroupCountsFromIndex(DataVoEngine engine, string tableName, string databaseName, List<TypedPredicate> predicates, TypedScanState state, out IndexGroupOutcome outcome)
    {
        outcome = IndexGroupOutcome.NotPlanned;
        if (!TryPlanIndexGroupCounts(engine, tableName, databaseName, predicates, state, out string? indexName)
            || !engine.IndexManager.CanCountIntegerKeys(indexName!, tableName, databaseName))
        {
            return null;
        }

        int key = state.GroupOrdinals![0];
        long nulls = 0;
        bool keyMayBeNull = !state.Columns[key].IsNotNull
            && !engine.Catalog.GetTablePrimaryKeys(tableName, databaseName).Contains(state.Columns[key].Name, StringComparer.OrdinalIgnoreCase);
        if (keyMayBeNull && !TryCountNullKeys(engine, tableName, databaseName, state, key, out nulls))
        {
            return null;
        }

        var stream = new IndexGroupStream(
            state,
            new TypedGroupSelector(this, state),
            state.Columns[key].Type.Equals("INT", StringComparison.OrdinalIgnoreCase),
            TryCompileHavingCountBounds(state, tableName));
        if (nulls > 0)
        {
            OfferIndexGroup(ref stream, null, nulls);
        }

        if (!engine.IndexManager.TryVisitIntegerKeyCounts(indexName!, tableName, databaseName, ref stream, static (ref IndexGroupStream s, long value, long rows) => OfferIndexGroup(ref s, value, rows)))
        {
            outcome = IndexGroupOutcome.Failed;
            return null;
        }

        outcome = IndexGroupOutcome.Answered;
        return state.GroupStates!.Count == 0 ? FinishAggregatesOverNoRows(state) : FinishAggregates(state, stream.Selector.Result());
    }

    /// <summary>
    /// Counts the rows whose key is NULL, which indexes do not hold, by reading only that column's null flag from
    /// each row. False when the rows cannot be scanned here.
    /// </summary>
    private static bool TryCountNullKeys(DataVoEngine engine, string tableName, string databaseName, TypedScanState state, int key, out long nulls)
    {
        var count = (Columns: state.Columns, Key: key, Nulls: 0L, Rows: 0L);
        bool scanned = engine.StorageContext.ScanTableRows(tableName, databaseName, null, null, ref count,
            static (ref (IReadOnlyList<Column> Columns, int Key, long Nulls, long Rows) c, RowKey _, ReadOnlySpan<byte> bytes, ReadOnlySpan<CellValue> typed) =>
            {
                bool isNull = typed.IsEmpty ? RowSerializer.IsNullAt(bytes, c.Columns, c.Key) : typed[c.Key].IsNull;
                c.Nulls += isNull ? 1 : 0;
                c.Rows++;
                return true;
            });
        nulls = count.Nulls;
        if (scanned)
        {
            RuntimeQueryDiagnosticsScope.RecordTableScan(tableName, (int)Math.Min(count.Rows, int.MaxValue));
        }

        return scanned;
    }

    /// <summary>What <see cref="TryGroupCountsFromIndex"/> did.</summary>
    private enum IndexGroupOutcome { NotPlanned, Answered, Failed }

    /// <summary>Whether the query has the shape the index plan answers, and the index to read.</summary>
    private bool TryPlanIndexGroupCounts(DataVoEngine engine, string tableName, string databaseName, List<TypedPredicate> predicates, TypedScanState state, out string? indexName)
    {
        indexName = null;
        if (predicates.Count > 0 || state.GroupOrdinals is not { Length: 1 } groupOrdinals || _activeSnapshot is not null)
        {
            return false;
        }

        int key = groupOrdinals[0];
        Column column = state.Columns[key];
        bool integer = column.Type.Equals("INT", StringComparison.OrdinalIgnoreCase) || column.Type.Equals("BIGINT", StringComparison.OrdinalIgnoreCase);
        if (!integer
            || !state.Aggregates!.TrueForAll(aggregate => aggregate.Spec.Function == HashAggregateOperator.AggregateFunction.Count && (aggregate.Ordinal == -1 || aggregate.Ordinal == key))
            || _model.GetAggregateColumns().Exists(selected => selected.Expression is not AggregateExpressionNode { IsDistinct: false })
            || _model.GetOrderByExpression() is not { Columns.Count: > 0 } orderBy
            || !orderBy.Columns.Exists(order => ResolveTypedOrderKey(state, order.Column.Name).Kind == OrderKeyGroupColumn))
        {
            return false;
        }

        foreach (IndexFile index in engine.Catalog.GetTableIndexesReadOnly(tableName, databaseName))
        {
            if (index.AttributeNames.Count == 1
                && index.AttributeNames[0].Equals(column.Name, StringComparison.OrdinalIgnoreCase)
                && index.IndexKind.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
            {
                indexName = index.IndexFileName;
                return true;
            }
        }

        return false;
    }

    /// <summary>The groups of an index walk, offered to the selector as they arrive.</summary>
    private readonly struct IndexGroupStream(TypedScanState state, TypedGroupSelector selector, bool intKey, HavingCountBound[]? bounds)
    {
        public TypedScanState State { get; } = state;
        public TypedGroupSelector Selector { get; } = selector;
        public bool IntKey { get; } = intKey;
        public HavingCountBound[]? Bounds { get; } = bounds;
    }

    /// <summary>Makes one index key a group, fills its counts, and keeps it only if HAVING and the top-N keep it.</summary>
    private static bool OfferIndexGroup(ref IndexGroupStream stream, long? key, long rows)
    {
        if (stream.Bounds is { } bounds && !HavingCountBound.AllHold(bounds, key, rows))
        {
            return true; // HAVING rejects it: no group is built
        }

        TypedScanState state = stream.State;
        object? value = key is not { } k ? null : stream.IntKey ? checked((int)k) : k;
        int group = state.GroupKeys!.AddUnique(value);
        state.GroupStates!.Add();
        List<TypedAggregate> aggregates = state.Aggregates!;
        for (int i = 0; i < aggregates.Count; i++)
        {
            state.GroupStates.CountOf(group, i) = aggregates[i].Ordinal < 0 || key is not null ? rows : 0;
        }

        state.Matched += rows;
        if (!stream.Selector.Offer(group))
        {
            state.GroupKeys.RemoveLast();
            state.GroupStates.RemoveLast();
        }

        return true;
    }

    /// <summary>
    /// One comparison of a HAVING made only of <c>COUNT(*)</c> or <c>COUNT(key)</c> against numbers, tested on the
    /// count itself. It only rejects groups early; a group it lets through is still tested by the general evaluator.
    /// </summary>
    private readonly struct HavingCountBound(bool star, string op, double value)
    {
        /// <summary>Whether the comparison counts every row (<c>COUNT(*)</c>) rather than the non-NULL keys.</summary>
        public bool Star { get; } = star;

        public string Operator { get; } = op;

        public double Value { get; } = value;

        /// <summary>Whether every comparison holds for a group with <paramref name="key"/> and <paramref name="rows"/>.</summary>
        public static bool AllHold(HavingCountBound[] bounds, long? key, long rows)
        {
            foreach (HavingCountBound bound in bounds)
            {
                double count = bound.Star || key is not null ? rows : 0;
                bool holds = bound.Operator switch
                {
                    Operators.GREATER_THAN_OR_EQUAL_TO => count >= bound.Value,
                    Operators.GREATER_THAN => count > bound.Value,
                    Operators.LESS_THAN_OR_EQUAL_TO => count <= bound.Value,
                    Operators.LESS_THAN => count < bound.Value,
                    Operators.EQUALS => count == bound.Value,
                    _ => true,
                };
                if (!holds)
                {
                    return false;
                }
            }

            return true;
        }
    }

    /// <summary>
    /// Reads a HAVING that is an AND of <c>COUNT(...) op number</c> comparisons into bounds; null when it has any
    /// other part (then every group goes to the general evaluator).
    /// </summary>
    private HavingCountBound[]? TryCompileHavingCountBounds(TypedScanState state, string tableName)
    {
        if (_model.GetHavingExpression() is not { } having)
        {
            return null;
        }

        var bounds = new List<HavingCountBound>();
        return TryCollectHavingCountBounds(having, state, tableName, bounds) ? [.. bounds] : null;
    }

    private bool TryCollectHavingCountBounds(ExpressionNode node, TypedScanState state, string tableName, List<HavingCountBound> bounds)
    {
        if (node is not BinaryExpressionNode binary)
        {
            return false;
        }

        if (binary.Operator == Operators.AND)
        {
            return TryCollectHavingCountBounds(binary.Left, state, tableName, bounds)
                && TryCollectHavingCountBounds(binary.Right, state, tableName, bounds);
        }

        string op = binary.Operator;
        (ExpressionNode aggregateSide, ExpressionNode valueSide) = (binary.Left, binary.Right);
        if (binary.Left is LiteralNode && binary.Right is AggregateExpressionNode)
        {
            (aggregateSide, valueSide) = (binary.Right, binary.Left);
            op = Mirror(op);
        }

        if (aggregateSide is not AggregateExpressionNode { IsDistinct: false } count
            || !count.FunctionName.Equals("COUNT", StringComparison.OrdinalIgnoreCase)
            || op is not (Operators.GREATER_THAN_OR_EQUAL_TO or Operators.GREATER_THAN or Operators.LESS_THAN_OR_EQUAL_TO or Operators.LESS_THAN or Operators.EQUALS)
            || valueSide is not LiteralNode literal
            || !TryNumber(literal.Value, out double value))
        {
            return false;
        }

        // Only an aggregate the SELECT list computes: the general evaluator reads HAVING's aggregates from there.
        if (LastAggregateNamed(state.Aggregates!, AggregateExpressionFormatter.BuildHeader(count)) < 0)
        {
            return false;
        }

        bool star = count.IsStar;
        if (!star && !string.Equals(ColumnOfTable(count.Argument, tableName), state.Columns[state.GroupOrdinals![0]].Name, StringComparison.OrdinalIgnoreCase))
        {
            return false; // COUNT of another column: the index does not know its NULLs
        }

        bounds.Add(new HavingCountBound(star, op, value));
        return true;
    }

    /// <summary>The operator that keeps a comparison's meaning when its sides are swapped.</summary>
    private static string Mirror(string op) => op switch
    {
        Operators.GREATER_THAN_OR_EQUAL_TO => Operators.LESS_THAN_OR_EQUAL_TO,
        Operators.GREATER_THAN => Operators.LESS_THAN,
        Operators.LESS_THAN_OR_EQUAL_TO => Operators.GREATER_THAN_OR_EQUAL_TO,
        Operators.LESS_THAN => Operators.GREATER_THAN,
        _ => op,
    };

    /// <summary>A number from a literal or bound value; false for anything else, text included.</summary>
    private static bool TryNumber(object? value, out double number)
    {
        switch (value)
        {
            case int i: number = i; return true;
            case long l: number = l; return true;
            case double d: number = d; return true;
            case float f: number = f; return true;
            case decimal m: number = (double)m; return true;
            case string text when !text.StartsWith('\''):
                return double.TryParse(text, NumberStyles.Integer | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out number);
            default:
                number = 0;
                return false;
        }
    }
}
