using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;
using SqlLexer = DataVo.Core.Parser.Lexer;
using SqlParser = DataVo.Core.Parser.Parser;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// The four reactive equi-join kinds.
/// </summary>
internal enum JoinKind
{
    /// <summary>INNER JOIN.</summary>
    Inner,

    /// <summary>LEFT [OUTER] JOIN.</summary>
    Left,

    /// <summary>RIGHT [OUTER] JOIN.</summary>
    Right,

    /// <summary>FULL [OUTER] JOIN.</summary>
    Full,
}

/// <summary>
/// One AND-ed equality conjunct of a join's <c>ON</c> condition, expressed as the left-table column
/// equated to the right-table column.
/// </summary>
/// <param name="LeftColumn">The (unqualified) column on the left/driving table.</param>
/// <param name="RightColumn">The (unqualified) column on the right/probed table.</param>
internal readonly record struct JoinKeyColumn(string LeftColumn, string RightColumn);

/// <summary>
/// The extracted, validated shape of a supported two-table reactive equi-join.
/// </summary>
/// <param name="LeftTable">The driving (FROM) table.</param>
/// <param name="LeftAlias">The driving table alias, or the table name when none is declared.</param>
/// <param name="RightTable">The joined table.</param>
/// <param name="RightAlias">The joined table alias, or the table name when none is declared.</param>
/// <param name="Kind">The join kind.</param>
/// <param name="Keys">The AND-ed equality conjuncts (left column ↔ right column).</param>
/// <param name="Where">The optional post-join <c>WHERE</c> predicate.</param>
/// <param name="Columns">The projected SELECT columns.</param>
internal sealed record JoinShape(
    string LeftTable,
    string LeftAlias,
    string RightTable,
    string RightAlias,
    JoinKind Kind,
    IReadOnlyList<JoinKeyColumn> Keys,
    ExpressionNode? Where,
    IReadOnlyList<SelectColumnNode> Columns);

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

    /// <summary>
    /// Attempts to extract a supported two-table equi-join shape from the parsed statement.
    /// </summary>
    /// <param name="select">The parsed SELECT.</param>
    /// <param name="shape">The extracted shape when the statement is a supported join.</param>
    /// <returns>
    /// <c>true</c> for a single two-table equi-join (one or more AND-ed equality conjuncts), optionally
    /// with a WHERE; <c>false</c> for a non-join, three-or-more-table join, or non-equi/<c>OR</c> ON.
    /// </returns>
    public static bool TryGetJoinShape(SelectStatement select, out JoinShape shape)
    {
        shape = null!;

        if (select.Joins.Count != 1 || select.FromTable is null)
        {
            return false;
        }

        // L3 forbids aggregation, grouping, ordering, limiting, distinct, CTEs and subqueries; those
        // belong to L2 (single-table) or L4. A join carrying any of them is out of this layer's scope.
        if (select.GroupByExpression is not null
            || select.HavingExpression is not null
            || select.OrderByExpression is not null
            || select.LimitExpression is not null
            || select.IsDistinct
            || select.Ctes.Count > 0)
        {
            return false;
        }

        JoinDetailNode join = select.Joins[0];
        if (!IsSupportedKind(join.JoinType, out JoinKind kind))
        {
            return false;
        }

        if (join.Condition is null)
        {
            // CROSS JOIN or a join without an ON equality is not an equi-join.
            return false;
        }

        string leftTable = select.FromTable.Name;
        string leftAlias = select.FromAlias?.Name ?? leftTable;
        string rightTable = join.TableName.Name;
        string rightAlias = join.Alias?.Name ?? rightTable;

        // A self-join would make the two sides indistinguishable for primary-key identity; reject it.
        if (rightTable.Equals(leftTable, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!TryResolveKey(join.Condition, leftTable, leftAlias, rightTable, rightAlias, out JoinKeyColumn key))
        {
            return false;
        }

        shape = new JoinShape(
            leftTable,
            leftAlias,
            rightTable,
            rightAlias,
            kind,
            [key],
            select.WhereExpression,
            select.Columns);

        return true;
    }

    private static bool IsSupportedKind(string joinType, out JoinKind kind)
    {
        if (JoinTypes.INNER.Equals(joinType, StringComparison.OrdinalIgnoreCase))
        {
            kind = JoinKind.Inner;
            return true;
        }

        if (JoinTypes.LEFT.Equals(joinType, StringComparison.OrdinalIgnoreCase))
        {
            kind = JoinKind.Left;
            return true;
        }

        if (JoinTypes.RIGHT.Equals(joinType, StringComparison.OrdinalIgnoreCase))
        {
            kind = JoinKind.Right;
            return true;
        }

        if (JoinTypes.FULL.Equals(joinType, StringComparison.OrdinalIgnoreCase))
        {
            kind = JoinKind.Full;
            return true;
        }

        // CROSS and anything else are not supported equi-joins.
        kind = default;
        return false;
    }

    private static bool TryResolveKey(
        JoinConditionNode condition,
        string leftTable,
        string leftAlias,
        string rightTable,
        string rightAlias,
        out JoinKeyColumn key)
    {
        key = default;

        ColumnRefNode a = condition.Left;
        ColumnRefNode b = condition.Right;

        bool aLeft = BelongsTo(a, leftTable, leftAlias);
        bool aRight = BelongsTo(a, rightTable, rightAlias);
        bool bLeft = BelongsTo(b, leftTable, leftAlias);
        bool bRight = BelongsTo(b, rightTable, rightAlias);

        // Each operand must be attributable to exactly one side, and the two operands must straddle the
        // two tables. Ambiguous/unqualified columns are rejected so the arrangement keys are unambiguous.
        if (aLeft && !aRight && bRight && !bLeft)
        {
            key = new JoinKeyColumn(a.Column, b.Column);
            return true;
        }

        if (aRight && !aLeft && bLeft && !bRight)
        {
            key = new JoinKeyColumn(b.Column, a.Column);
            return true;
        }

        return false;
    }

    private static bool BelongsTo(ColumnRefNode column, string table, string alias)
    {
        if (string.IsNullOrEmpty(column.TableOrAlias))
        {
            return false;
        }

        return column.TableOrAlias.Equals(table, StringComparison.OrdinalIgnoreCase)
            || column.TableOrAlias.Equals(alias, StringComparison.OrdinalIgnoreCase);
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
