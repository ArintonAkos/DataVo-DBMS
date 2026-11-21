using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Utils;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Evaluates a single-table reactive <c>WHERE</c> predicate over a materialized row image.
/// </summary>
/// <remarks>
/// Comparison and NULL semantics are delegated to <see cref="ExpressionValueComparer"/>, the same
/// evaluator the batch single-table WHERE path uses, so reactive filtering matches a full
/// re-execution of the query. A <c>null</c> predicate matches every row. The supported predicate
/// surface is the linear (L1) subset: comparisons, <c>AND</c>/<c>OR</c>, <c>IS [NOT] NULL</c>, and
/// <c>LIKE</c>.
/// </remarks>
internal sealed class ReactivePredicate
{
    private readonly ExpressionNode? _predicate;

    private ReactivePredicate(ExpressionNode? predicate)
    {
        _predicate = predicate;
    }

    /// <summary>
    /// Validates and compiles the supplied WHERE expression (which may be <c>null</c>).
    /// </summary>
    /// <param name="predicate">The WHERE expression, or <c>null</c> for an unconditional match.</param>
    /// <returns>The compiled predicate.</returns>
    /// <exception cref="NotSupportedException">Thrown for a WHERE construct outside the linear subset.</exception>
    public static ReactivePredicate Compile(ExpressionNode? predicate)
    {
        if (predicate is not null)
        {
            Validate(predicate);
        }

        return new ReactivePredicate(predicate);
    }

    /// <summary>
    /// Evaluates the predicate against a row image.
    /// </summary>
    /// <param name="row">The row to test.</param>
    /// <returns><c>true</c> when the row satisfies the predicate (or there is none).</returns>
    public bool Matches(IReadOnlyDictionary<string, object?> row)
    {
        return _predicate is null || Evaluate(_predicate, row);
    }

    /// <summary>
    /// Evaluates the predicate against a borrowed typed row without materializing the owned dictionary image.
    /// </summary>
    /// <param name="row">The typed row to test.</param>
    /// <returns><c>true</c> when the row satisfies the predicate (or there is none).</returns>
    public bool Matches(RowRef row)
    {
        return _predicate is null || Evaluate(_predicate, row);
    }

    private static void Validate(ExpressionNode expression)
    {
        switch (expression)
        {
            case InSubqueryExpressionNode:
            case ExistsSubqueryExpressionNode:
            case ScalarSubqueryExpressionNode:
                throw new NotSupportedException("Reactive subscriptions do not support subqueries in WHERE.");

            case AggregateExpressionNode:
            case WindowFunctionExpressionNode:
                throw new NotSupportedException("Reactive subscriptions do not support aggregate or window functions in WHERE.");

            case BinaryExpressionNode binary:
                Validate(binary.Left);
                Validate(binary.Right);
                break;

            case ColumnRefNode:
            case ResolvedColumnRefNode:
            case LiteralNode:
                break;

            default:
                throw new NotSupportedException($"Reactive subscriptions do not support the WHERE construct '{expression.GetType().Name}'.");
        }
    }

    private static bool Evaluate(ExpressionNode expression, IReadOnlyDictionary<string, object?> row)
    {
        if (expression is not BinaryExpressionNode binary)
        {
            throw new NotSupportedException($"Reactive subscriptions cannot evaluate the WHERE construct '{expression.GetType().Name}'.");
        }

        switch (binary.Operator)
        {
            case Operators.AND:
                return Evaluate(binary.Left, row) && Evaluate(binary.Right, row);
            case Operators.OR:
                return Evaluate(binary.Left, row) || Evaluate(binary.Right, row);
            case Operators.IS_NULL:
                return ResolveValue(binary.Left, row) is null;
            case Operators.IS_NOT_NULL:
                return ResolveValue(binary.Left, row) is not null;
        }

        object? left = ResolveValue(binary.Left, row);
        object? right = ResolveValue(binary.Right, row);
        bool trimQuotes = binary.Left is not BoundValueLiteralNode && binary.Right is not BoundValueLiteralNode;

        return binary.Operator switch
        {
            Operators.EQUALS => ExpressionValueComparer.AreEqual(left, right, trimQuotedStrings: trimQuotes),
            Operators.NOT_EQUALS => left is not null && right is not null
                && !ExpressionValueComparer.AreEqual(left, right, trimQuotedStrings: trimQuotes),
            Operators.LESS_THAN => left is not null && right is not null
                && ExpressionValueComparer.Compare(left, right, trimQuotedStrings: trimQuotes) < 0,
            Operators.GREATER_THAN => left is not null && right is not null
                && ExpressionValueComparer.Compare(left, right, trimQuotedStrings: trimQuotes) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => left is not null && right is not null
                && ExpressionValueComparer.Compare(left, right, trimQuotedStrings: trimQuotes) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => left is not null && right is not null
                && ExpressionValueComparer.Compare(left, right, trimQuotedStrings: trimQuotes) >= 0,
            Operators.LIKE => ExpressionValueComparer.MatchesLike(left, right, trimQuotedStrings: trimQuotes),
            Operators.NOT_LIKE => ExpressionValueComparer.MatchesNotLike(left, right, trimQuotedStrings: trimQuotes),
            _ => throw new NotSupportedException($"Reactive subscriptions do not support the operator '{binary.Operator}' in WHERE.")
        };
    }

    private static bool Evaluate(ExpressionNode expression, RowRef row)
    {
        if (expression is not BinaryExpressionNode binary)
        {
            throw new NotSupportedException($"Reactive subscriptions cannot evaluate the WHERE construct '{expression.GetType().Name}'.");
        }

        switch (binary.Operator)
        {
            case Operators.AND:
                return Evaluate(binary.Left, row) && Evaluate(binary.Right, row);
            case Operators.OR:
                return Evaluate(binary.Left, row) || Evaluate(binary.Right, row);
            case Operators.IS_NULL:
                return ResolveCell(binary.Left, row).IsNull;
            case Operators.IS_NOT_NULL:
                return !ResolveCell(binary.Left, row).IsNull;
        }

        CellValue left = ResolveCell(binary.Left, row);
        CellValue right = ResolveCell(binary.Right, row);

        return binary.Operator switch
        {
            Operators.EQUALS => !left.IsNull && !right.IsNull && AreEqual(left, right),
            Operators.NOT_EQUALS => !left.IsNull && !right.IsNull && !AreEqual(left, right),
            Operators.LESS_THAN => !left.IsNull && !right.IsNull && CellValueComparer.Compare(left, right) < 0,
            Operators.GREATER_THAN => !left.IsNull && !right.IsNull && CellValueComparer.Compare(left, right) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => !left.IsNull && !right.IsNull && CellValueComparer.Compare(left, right) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => !left.IsNull && !right.IsNull && CellValueComparer.Compare(left, right) >= 0,
            Operators.LIKE => ExpressionValueComparer.MatchesLike(left.ToObject(), right.ToObject(), trimQuotedStrings: false),
            Operators.NOT_LIKE => ExpressionValueComparer.MatchesNotLike(left.ToObject(), right.ToObject(), trimQuotedStrings: false),
            _ => throw new NotSupportedException($"Reactive subscriptions do not support the operator '{binary.Operator}' in WHERE.")
        };
    }

    private static object? ResolveValue(ExpressionNode expression, IReadOnlyDictionary<string, object?> row)
    {
        return expression switch
        {
            NullLiteralNode => null,
            BoundValueLiteralNode bound => bound.Value,
            LiteralNode literal => UnquoteSqlLiteral(literal.Value),
            ColumnRefNode columnRef when IsBooleanLiteral(columnRef.Column, out bool value) => value,
            ColumnRefNode columnRef => row.TryGetValue(columnRef.Column, out object? value) ? value : null,
            ResolvedColumnRefNode resolved when IsBooleanLiteral(resolved.Column, out bool value) => value,
            ResolvedColumnRefNode resolved => row.TryGetValue(resolved.Column, out object? value) ? value : null,
            _ => throw new NotSupportedException($"Reactive subscriptions cannot evaluate the operand '{expression.GetType().Name}'.")
        };
    }

    private static CellValue ResolveCell(ExpressionNode expression, RowRef row)
    {
        return expression switch
        {
            NullLiteralNode => CellValue.Null,
            BoundValueLiteralNode bound => CellValue.FromBorrowedObject(bound.Value),
            LiteralNode literal => CellValue.FromBorrowedObject(UnquoteSqlLiteral(literal.Value)),
            ColumnRefNode columnRef when IsBooleanLiteral(columnRef.Column, out bool value) => CellValue.From(value),
            ColumnRefNode columnRef => row.TryGet(columnRef.Column, out CellValue value) ? value : CellValue.Null,
            ResolvedColumnRefNode resolved when IsBooleanLiteral(resolved.Column, out bool value) => CellValue.From(value),
            ResolvedColumnRefNode resolved => row.TryGet(resolved.Column, out CellValue value) ? value : CellValue.Null,
            _ => throw new NotSupportedException($"Reactive subscriptions cannot evaluate the operand '{expression.GetType().Name}'.")
        };
    }

    private static object? UnquoteSqlLiteral(object? value) =>
        value is string text && text.Length >= 2 && text[0] == '\'' && text[^1] == '\''
            ? text[1..^1]
            : value;

    private static bool AreEqual(CellValue left, CellValue right)
    {
        return CellValueEqualityComparer.AreEqual(left, right);
    }

    private static bool IsBooleanLiteral(string token, out bool value)
    {
        if (token.Equals("true", StringComparison.OrdinalIgnoreCase))
        {
            value = true;
            return true;
        }

        if (token.Equals("false", StringComparison.OrdinalIgnoreCase))
        {
            value = false;
            return true;
        }

        value = false;
        return false;
    }
}
