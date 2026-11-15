using DataVo.Core.Constants;
using DataVo.Core.Enums;
using DataVo.Core.Execution.Volcano;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// The columnar pushdown of the typed single-table path: an ungrouped <c>COUNT</c>, <c>SUM</c>, <c>AVG</c>,
/// <c>MIN</c> or <c>MAX</c> whose WHERE is an AND of comparisons, over columns that are all in the table's
/// columnar index, is computed from the index's dense columns instead of from rows.
/// </summary>
/// <remarks>
/// The row scan stays the reference: the pushdown only takes a query whose result it can give exactly as the
/// row scan would, and leaves every other one to it. The results land in the same group the row scan fills.
/// </remarks>
internal partial class Select
{
    /// <summary>
    /// Fills <paramref name="state"/>'s single group from the table's columnar index.
    /// </summary>
    /// <returns><see langword="false"/> when the query must read rows; <paramref name="state"/> is then untouched.</returns>
    private static bool TryAggregateFromColumnarIndex(DataVoEngine engine, string databaseName, string tableName, TypedScanState state)
    {
        if (state.Aggregates is not { } aggregates
            || state.GroupOrdinals is not { Length: 0 }
            || aggregates.Count > ColumnarScan.MaxFolds
            || engine.StorageContext.ColumnarStore is null
            || MvccExecutionScope.CurrentSnapshot is not null
            || engine.StorageContext.LsmEngine?.HasStagedWrites(databaseName, tableName) == true)
        {
            return false;
        }

        // A statement already runs inside the engine's read scope, so the rows cannot be swapped out underneath.
        ColumnarIndex? index = engine.Columnar.Find(databaseName, tableName);
        if (index is null)
        {
            return false;
        }

        ColumnarScan.Workspace workspace = ColumnarScan.Rent();
        int steps = 0;
        if (!TryDescribeAll(index, state.Predicates, workspace.Steps, ref steps)
            || !ColumnarScan.TryBind(index, workspace.Steps.AsSpan(0, steps)))
        {
            return false;
        }

        for (int i = 0; i < aggregates.Count; i++)
        {
            // A floating-point sum is the row scan's only while the index is in the rows' order.
            if (!TryDescribe(index, aggregates[i], out workspace.Folds[i])
                || (!index.InKeyOrder && workspace.Folds[i].Kind is RowFoldKind.SumFloat or RowFoldKind.SumDouble))
            {
                return false;
            }
        }

        int matched;
        try
        {
            if (!engine.StorageContext.ColumnarStore.TryScan(
                    databaseName,
                    tableName,
                    index,
                    new ReadOnlySpan<ColumnarStep>(workspace.Steps, 0, steps),
                    new ReadOnlySpan<ColumnarFold>(workspace.Folds, 0, aggregates.Count),
                    workspace.Totals,
                    0,
                    out matched))
            {
                return false;
            }
        }
        catch (OverflowException)
        {
            return false; // SQL's SUM carries on in floating point past the range of BIGINT; the row scan does that
        }

        state.Matched = matched;
        state.Sequence = matched;
        if (matched == 0)
        {
            return true; // no group: answered as aggregates over no rows, as after a row scan
        }

        int group = state.GroupKeys!.FindOrAdd([], out _);
        state.GroupStates!.Add();
        for (int i = 0; i < aggregates.Count; i++)
        {
            Store(state.GroupStates, group, i, aggregates[i], index, workspace.Folds[i], in workspace.Totals[i]);
        }

        return true;
    }

    /// <summary>
    /// Writes an AND of predicates as postfix steps: each term, then <c>And</c> after every term but the first.
    /// An OR becomes its branches joined by <c>Or</c>; the steps' NULL handling (a NULL meets no comparison)
    /// is the row scan's, since the WHERE has no NOT left. False when a term cannot be written or the steps
    /// run out.
    /// </summary>
    private static bool TryDescribeAll(ColumnarIndex index, IReadOnlyList<TypedPredicate> predicates, ColumnarStep[] steps, ref int count)
    {
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!TryDescribeTerm(index, predicates[i], steps, ref count)
                || (i > 0 && !TryAppend(steps, ref count, ColumnarStep.And)))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryDescribeTerm(ColumnarIndex index, TypedPredicate predicate, ColumnarStep[] steps, ref int count)
    {
        if (predicate.Branches is not { } branches)
        {
            return count < steps.Length && TryDescribe(index, predicate, ref steps[count++]);
        }

        for (int i = 0; i < branches.Length; i++)
        {
            if (!TryDescribeAll(index, branches[i], steps, ref count)
                || (i > 0 && !TryAppend(steps, ref count, ColumnarStep.Or)))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryAppend(ColumnarStep[] steps, ref int count, byte kind)
    {
        if (count >= steps.Length)
        {
            return false;
        }

        steps[count++] = new ColumnarStep { Kind = kind };
        return true;
    }

    /// <summary>Writes a WHERE comparison as a columnar step, when the index compares exactly as the row scan does.</summary>
    private static bool TryDescribe(ColumnarIndex index, TypedPredicate predicate, ref ColumnarStep step)
    {
        int slot = index.SlotOf(predicate.Ordinal);
        if (slot < 0)
        {
            return false;
        }

        step = new ColumnarStep { Column = predicate.Ordinal };
        if (predicate.Kind is TypedPredicateKind.IsNull or TypedPredicateKind.IsNotNull)
        {
            step.Kind = ColumnarStep.NullTest;
            step.Test = predicate.Kind == TypedPredicateKind.IsNull ? ColumnarTest.Equal : ColumnarTest.NotEqual;
            return true;
        }

        step.Kind = ColumnarStep.Compare;
        switch (predicate.Operator)
        {
            case Operators.EQUALS: step.Test = ColumnarTest.Equal; break;
            case Operators.NOT_EQUALS: step.Test = ColumnarTest.NotEqual; break;
            case Operators.LESS_THAN: step.Test = ColumnarTest.Less; break;
            case Operators.LESS_THAN_OR_EQUAL_TO: step.Test = ColumnarTest.LessOrEqual; break;
            case Operators.GREATER_THAN: step.Test = ColumnarTest.Greater; break;
            case Operators.GREATER_THAN_OR_EQUAL_TO: step.Test = ColumnarTest.GreaterOrEqual; break;
            default: return false;
        }

        StorageKind kind = index.KindOf(slot);
        double number = predicate.Number;
        switch (predicate.Kind)
        {
            case TypedPredicateKind.Integer when kind == StorageKind.BigInt:
                step.ValueKind = 'l';
                step.Bits = predicate.Integer;
                return true;

            // The row scan compares an INT as a double with the literal. That is the integer comparison only
            // when the literal is itself a whole number an INT can hold.
            case TypedPredicateKind.Numeric when kind == StorageKind.Int
                && number >= int.MinValue && number <= int.MaxValue && number == Math.Floor(number):
                step.ValueKind = 'i';
                step.Bits = (int)number;
                return true;

            // A NaN in the column orders differently in the row scan's comparer than in a vector comparison.
            case TypedPredicateKind.Numeric when kind == StorageKind.Double && !double.IsNaN(number) && !index.AnyNaN(slot):
                step.ValueKind = 'd';
                step.Bits = BitConverter.DoubleToInt64Bits(number);
                return true;

            // The row scan widens a FLOAT before comparing; comparing as floats agrees only when the literal
            // is exactly a float.
            case TypedPredicateKind.Numeric when kind == StorageKind.Float && (float)number == number && !index.AnyNaN(slot):
                step.ValueKind = 'f';
                step.Bits = BitConverter.SingleToInt32Bits((float)number);
                return true;

            default:
                return false;
        }
    }

    /// <summary>Chooses how the index computes an aggregate, when it has the column and can compute it exactly.</summary>
    private static bool TryDescribe(ColumnarIndex index, TypedAggregate aggregate, out ColumnarFold fold)
    {
        fold = default;
        HashAggregateOperator.AggregateFunction function = aggregate.Spec.Function;
        if (aggregate.Ordinal < 0)
        {
            return function == HashAggregateOperator.AggregateFunction.Count; // COUNT(*): the matching rows
        }

        fold.Slot = index.SlotOf(aggregate.Ordinal);
        if (fold.Slot < 0)
        {
            return false;
        }

        if (function == HashAggregateOperator.AggregateFunction.Count)
        {
            return true; // RowFoldKind.None: only the values that are not NULL are counted
        }

        char type = index.KindOf(fold.Slot) switch
        {
            StorageKind.Int => 'i',
            StorageKind.BigInt => 'l',
            StorageKind.Float => 'f',
            StorageKind.Double => 'd',
            _ => '\0',
        };
        RowFold operation = function switch
        {
            HashAggregateOperator.AggregateFunction.Min => RowFold.Min,
            HashAggregateOperator.AggregateFunction.Max => RowFold.Max,
            _ => RowFold.Sum,
        };
        if (function is not (HashAggregateOperator.AggregateFunction.Sum or HashAggregateOperator.AggregateFunction.Avg
            or HashAggregateOperator.AggregateFunction.Min or HashAggregateOperator.AggregateFunction.Max))
        {
            return false;
        }

        fold.Kind = RowAggregate.KindFor(index.KindOf(fold.Slot), operation, type);
        return fold.Kind != RowFoldKind.None;
    }

    /// <summary>Puts one aggregate's columnar result where the row scan would have accumulated it.</summary>
    private static void Store(TypedGroupStates groups, int group, int at, TypedAggregate aggregate, ColumnarIndex index, ColumnarFold fold, in RowAggregate total)
    {
        if (aggregate.Ordinal < 0)
        {
            groups.CountOf(group, at) = total.Count;
            return;
        }

        bool whole = index.KindOf(fold.Slot) is StorageKind.Int or StorageKind.BigInt;
        switch (aggregate.Spec.Function)
        {
            case HashAggregateOperator.AggregateFunction.Count:
                groups.CountOf(group, at) = total.Values;
                break;
            case HashAggregateOperator.AggregateFunction.Sum when total.Values > 0:
                if (whole)
                {
                    groups.TotalOf(group, at).Add(total.Whole);
                }
                else
                {
                    groups.TotalOf(group, at).Add(total.Real);
                }

                break;
            case HashAggregateOperator.AggregateFunction.Avg:
                // Integers are added exactly here and one by one as doubles in the row scan; the two differ
                // only once a sum passes 2^53.
                groups.SumOf(group, at) = whole ? total.Whole : total.Real;
                groups.CountOf(group, at) = total.Values;
                break;
            case HashAggregateOperator.AggregateFunction.Min or HashAggregateOperator.AggregateFunction.Max when total.Values > 0:
                // Boxed as the column's own type, as the row scan's values are.
                groups.ExtremeOf(group, at) = index.KindOf(fold.Slot) switch
                {
                    StorageKind.Int => (object)(int)total.Whole,
                    StorageKind.BigInt => (object)total.Whole,
                    StorageKind.Float => (object)(float)total.Real,
                    _ => (object)total.Real,
                };
                groups.HasExtremeOf(group, at) = true;
                break;
        }
    }
}
