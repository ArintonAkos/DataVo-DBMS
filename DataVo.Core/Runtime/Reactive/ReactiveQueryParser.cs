using DataVo.Core.Parser.AST;
using SqlLexer = DataVo.Core.Parser.Lexer;
using SqlParser = DataVo.Core.Parser.Parser;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Shared parsing and shape-inspection helpers for reactive query operators.
/// </summary>
/// <remarks>
/// All reactive operators (linear, aggregate, top-K) parse exactly one single-table <c>SELECT</c>
/// through the engine's existing parser. This helper centralizes that parse and the predicates used
/// to route a query to the correct operator so the maintenance logic never reimplements SQL parsing.
/// </remarks>
internal static class ReactiveQueryParser
{
    /// <summary>
    /// Parses the supplied SQL as exactly one <see cref="SelectStatement"/>, raising
    /// <see cref="NotSupportedException"/> for anything that is not a single SELECT.
    /// </summary>
    /// <param name="sql">The reactive subscription SQL.</param>
    /// <returns>The parsed <see cref="SelectStatement"/>.</returns>
    public static SelectStatement ParseSingleSelect(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new NotSupportedException("Reactive subscriptions require a non-empty SELECT statement.");
        }

        List<SqlStatement> statements;
        try
        {
            var lexer = new SqlLexer(sql);
            var parser = new SqlParser(lexer.Tokenize());
            statements = parser.Parse();
        }
        catch (Exception ex)
        {
            throw new NotSupportedException($"Reactive subscription SQL could not be parsed: {ex.Message}", ex);
        }

        if (statements.Count != 1)
        {
            throw new NotSupportedException("Reactive subscriptions support exactly one SELECT statement.");
        }

        if (statements[0] is not SelectStatement select)
        {
            throw new NotSupportedException("Reactive subscriptions support only SELECT statements.");
        }

        return select;
    }

    /// <summary>
    /// Returns <c>true</c> when the statement projects at least one aggregate function or has a
    /// <c>GROUP BY</c> clause (the aggregate operator shape).
    /// </summary>
    /// <param name="select">The parsed SELECT.</param>
    public static bool IsAggregateShape(SelectStatement select)
    {
        if (select.GroupByExpression is not null)
        {
            return true;
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (ContainsAggregate(column.Expression))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns <c>true</c> when the statement is a maintained top-K shape (<c>ORDER BY … LIMIT</c>).
    /// </summary>
    /// <param name="select">The parsed SELECT.</param>
    public static bool IsTopKShape(SelectStatement select)
    {
        return select.OrderByExpression is not null && select.LimitExpression is not null;
    }

    private static bool ContainsAggregate(ExpressionNode? expression)
    {
        return expression switch
        {
            null => false,
            AggregateExpressionNode => true,
            BinaryExpressionNode binary => ContainsAggregate(binary.Left) || ContainsAggregate(binary.Right),
            _ => false,
        };
    }
}
