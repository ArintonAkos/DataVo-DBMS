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
/// The four supported reactive subquery predicate kinds. IN/EXISTS are semi-joins; their negations are
/// the corresponding anti-joins.
/// </summary>
internal enum SubqueryKind
{
    /// <summary><c>WHERE col IN (SELECT …)</c> — semi-join on value membership.</summary>
    In,

    /// <summary><c>WHERE col NOT IN (SELECT …)</c> — anti-join on value membership.</summary>
    NotIn,

    /// <summary><c>WHERE EXISTS (SELECT …)</c> — semi-join on inner non-emptiness.</summary>
    Exists,

    /// <summary><c>WHERE NOT EXISTS (SELECT …)</c> — anti-join on inner non-emptiness.</summary>
    NotExists,
}

/// <summary>
/// The extracted, validated shape of a supported reactive IN/EXISTS subquery: a single outer table whose
/// <c>WHERE</c> is one IN/EXISTS predicate over a single-table inner SELECT.
/// </summary>
/// <param name="OuterTable">The outer (FROM) table.</param>
/// <param name="OuterAlias">The outer table alias, or its name when none is declared.</param>
/// <param name="OuterColumns">The outer SELECT's projected columns.</param>
/// <param name="OuterColumn">The outer column tested by an IN predicate (empty for EXISTS).</param>
/// <param name="InnerTable">The inner subquery's single source table.</param>
/// <param name="InnerAlias">The inner table alias, or its name when none is declared.</param>
/// <param name="InnerColumn">The inner column projected by an IN subquery, or <c>null</c> for EXISTS.</param>
/// <param name="InnerWhere">The residual inner-only WHERE predicate after the correlation conjunct is split out.</param>
/// <param name="Kind">The subquery predicate kind.</param>
/// <param name="IsCorrelated"><c>true</c> when the inner WHERE references the outer table via a single equality conjunct.</param>
/// <param name="OuterCorrelationColumn">The outer side of the correlation equality, or <c>null</c> when uncorrelated.</param>
/// <param name="InnerCorrelationColumn">The inner side of the correlation equality, or <c>null</c> when uncorrelated.</param>
internal sealed record SubqueryShape(
    string OuterTable,
    string OuterAlias,
    IReadOnlyList<SelectColumnNode> OuterColumns,
    string OuterColumn,
    string InnerTable,
    string InnerAlias,
    string? InnerColumn,
    ExpressionNode? InnerWhere,
    SubqueryKind Kind,
    bool IsCorrelated = false,
    string? OuterCorrelationColumn = null,
    string? InnerCorrelationColumn = null);

/// <summary>
/// Shared parsing and shape-inspection helpers used by <see cref="ReactiveRegistry"/> to route a
/// subscription to the correct reactive operator.
/// </summary>
/// <remarks>
/// Every reactive operator reuses the engine's existing SQL parser rather than reimplementing parsing.
/// This helper centralizes that single parse and the shape predicates that classify the result —
/// aggregate vs top-K vs distinct, two-table equi-join (<see cref="TryGetJoinShape"/>), and IN/EXISTS
/// subquery (<see cref="TryGetSubqueryShape"/>, including splitting a correlated inner WHERE into its
/// correlation conjunct and residual). Each classifier is conservative: it returns the shape only when
/// the query is fully within the supported subset, so an unsupported construct is rejected with a clear
/// error rather than silently mis-maintained.
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
        SqlStatement statement = ParseSingleStatement(sql);

        if (statement is not SelectStatement select)
        {
            throw new NotSupportedException("Reactive subscriptions support only SELECT statements.");
        }

        return select;
    }

    /// <summary>
    /// Parses the supplied SQL as exactly one SELECT-family statement.
    /// </summary>
    /// <param name="sql">The reactive subscription SQL.</param>
    public static SqlStatement ParseSingleStatement(string sql)
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
            if (statements[0] is UnionSelectStatement union)
            {
                return union;
            }

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
    /// Returns <c>true</c> when the statement is a single-table DISTINCT shape.
    /// </summary>
    /// <param name="select">The parsed SELECT.</param>
    public static bool IsDistinctShape(SelectStatement select)
    {
        return select.IsDistinct;
    }

    /// <summary>
    /// Attempts to extract a supported IN/EXISTS subquery shape (correlated or uncorrelated) from the
    /// parsed statement.
    /// </summary>
    /// <param name="select">The parsed SELECT.</param>
    /// <param name="shape">The extracted shape when the statement is a supported IN/EXISTS subquery.</param>
    /// <returns><c>true</c> for a supported single-outer-table IN/NOT IN/EXISTS/NOT EXISTS over a single-table inner SELECT; otherwise <c>false</c>.</returns>
    public static bool TryGetSubqueryShape(SelectStatement select, out SubqueryShape shape)
    {
        shape = null!;

        if (select.FromTable is null
            || select.Joins.Count > 0
            || select.IsDistinct
            || select.GroupByExpression is not null
            || select.HavingExpression is not null
            || select.OrderByExpression is not null
            || select.LimitExpression is not null
            || select.Ctes.Count > 0
            || select.WhereExpression is null)
        {
            return false;
        }

        string outerTable = select.FromTable.Name;
        string outerAlias = select.FromAlias?.Name ?? outerTable;

        if (select.WhereExpression is InSubqueryExpressionNode inSubquery)
        {
            if (!TryResolveColumn(inSubquery.Left, out string? outerQualifier, out string outerColumn)
                || (outerQualifier is not null
                    && !outerQualifier.Equals(outerTable, StringComparison.OrdinalIgnoreCase)
                    && !outerQualifier.Equals(outerAlias, StringComparison.OrdinalIgnoreCase))
                || inSubquery.Subquery is not SelectStatement inner
                || !TryValidateInnerSelect(inner, requireSingleProjection: true, out string innerTable, out string innerAlias, out string? innerColumn))
            {
                return false;
            }

            if (!TrySplitInnerWhere(inner.WhereExpression, outerTable, outerAlias, innerTable, innerAlias,
                    out ExpressionNode? inResidual, out string? inOuterCorr, out string? inInnerCorr))
            {
                return false;
            }

            shape = new SubqueryShape(
                outerTable,
                outerAlias,
                select.Columns,
                outerColumn,
                innerTable,
                innerAlias,
                innerColumn,
                inResidual,
                inSubquery.IsNegated ? SubqueryKind.NotIn : SubqueryKind.In,
                IsCorrelated: inOuterCorr is not null,
                OuterCorrelationColumn: inOuterCorr,
                InnerCorrelationColumn: inInnerCorr);
            return true;
        }

        if (select.WhereExpression is ExistsSubqueryExpressionNode exists
            && exists.Subquery is SelectStatement existsInner
            && TryValidateInnerSelect(existsInner, requireSingleProjection: false, out string existsInnerTable, out string existsInnerAlias, out _))
        {
            if (!TrySplitInnerWhere(existsInner.WhereExpression, outerTable, outerAlias, existsInnerTable, existsInnerAlias,
                    out ExpressionNode? existsResidual, out string? existsOuterCorr, out string? existsInnerCorr))
            {
                return false;
            }

            shape = new SubqueryShape(
                outerTable,
                outerAlias,
                select.Columns,
                OuterColumn: string.Empty,
                existsInnerTable,
                existsInnerAlias,
                InnerColumn: null,
                existsResidual,
                exists.IsNegated ? SubqueryKind.NotExists : SubqueryKind.Exists,
                IsCorrelated: existsOuterCorr is not null,
                OuterCorrelationColumn: existsOuterCorr,
                InnerCorrelationColumn: existsInnerCorr);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Splits an inner subquery <c>WHERE</c> into its (optional) single equality correlation conjunct
    /// <c>inner.col = outer.col</c> and the residual inner-only predicate. Returns <c>false</c> when the
    /// inner WHERE references the outer table in any way that is not a single supported equality
    /// correlation conjunct (so unsupported correlation shapes are rejected rather than silently wrong).
    /// </summary>
    private static bool TrySplitInnerWhere(
        ExpressionNode? innerWhere,
        string outerTable,
        string outerAlias,
        string innerTable,
        string innerAlias,
        out ExpressionNode? residual,
        out string? outerCorrelationColumn,
        out string? innerCorrelationColumn)
    {
        residual = innerWhere;
        outerCorrelationColumn = null;
        innerCorrelationColumn = null;

        if (innerWhere is null)
        {
            return true;
        }

        // Flatten the top-level AND-chain. OR cannot host a correlation conjunct we can isolate, so an OR
        // anywhere over an outer reference is rejected.
        var conjuncts = new List<ExpressionNode>();
        FlattenAnd(innerWhere, conjuncts);

        var residualConjuncts = new List<ExpressionNode>();
        foreach (ExpressionNode conjunct in conjuncts)
        {
            bool refsOuter = ContainsOuterReference(conjunct, outerTable, outerAlias);
            if (!refsOuter)
            {
                residualConjuncts.Add(conjunct);
                continue;
            }

            // This conjunct correlates to the outer table. Only a single, top-level
            // inner.col = outer.col equality is supported.
            if (outerCorrelationColumn is not null
                || conjunct is not BinaryExpressionNode binary
                || !binary.Operator.Equals(Operators.EQUALS, StringComparison.Ordinal)
                || !TryResolveColumn(binary.Left, out string? leftQualifier, out string leftColumn)
                || !TryResolveColumn(binary.Right, out string? rightQualifier, out string rightColumn))
            {
                return false;
            }

            bool leftOuter = QualifierMatches(leftQualifier, outerTable, outerAlias);
            bool leftInner = QualifierMatches(leftQualifier, innerTable, innerAlias);
            bool rightOuter = QualifierMatches(rightQualifier, outerTable, outerAlias);
            bool rightInner = QualifierMatches(rightQualifier, innerTable, innerAlias);

            if (leftOuter && rightInner && !leftInner && !rightOuter)
            {
                outerCorrelationColumn = leftColumn;
                innerCorrelationColumn = rightColumn;
            }
            else if (rightOuter && leftInner && !rightInner && !leftOuter)
            {
                outerCorrelationColumn = rightColumn;
                innerCorrelationColumn = leftColumn;
            }
            else
            {
                return false;
            }
        }

        residual = residualConjuncts.Count == 0
            ? null
            : residualConjuncts.Aggregate((left, right) => new BinaryExpressionNode
            {
                Operator = Operators.AND,
                Left = left,
                Right = right,
            });

        return true;
    }

    private static void FlattenAnd(ExpressionNode expression, List<ExpressionNode> conjuncts)
    {
        if (expression is BinaryExpressionNode binary && binary.Operator.Equals(Operators.AND, StringComparison.Ordinal))
        {
            FlattenAnd(binary.Left, conjuncts);
            FlattenAnd(binary.Right, conjuncts);
            return;
        }

        conjuncts.Add(expression);
    }

    private static bool QualifierMatches(string? qualifier, string table, string alias)
    {
        return qualifier is not null
            && (qualifier.Equals(table, StringComparison.OrdinalIgnoreCase)
                || qualifier.Equals(alias, StringComparison.OrdinalIgnoreCase));
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

    private static bool TryValidateInnerSelect(
        SelectStatement inner,
        bool requireSingleProjection,
        out string innerTable,
        out string innerAlias,
        out string? innerColumn)
    {
        innerTable = string.Empty;
        innerAlias = string.Empty;
        innerColumn = null;

        if (inner.FromTable is null
            || inner.Joins.Count > 0
            || inner.IsDistinct
            || inner.GroupByExpression is not null
            || inner.HavingExpression is not null
            || inner.OrderByExpression is not null
            || inner.LimitExpression is not null
            || inner.Ctes.Count > 0)
        {
            return false;
        }

        innerTable = inner.FromTable.Name;
        innerAlias = inner.FromAlias?.Name ?? innerTable;

        if (requireSingleProjection)
        {
            // The IN value column must be a plain inner column. A correlated scalar aggregate
            // (for example COUNT(*) = R.col) is out of scope and is rejected here.
            if (inner.Columns.Count != 1 || !TryResolveColumn(inner.Columns[0].Expression, out _, out string column))
            {
                return false;
            }

            innerColumn = column;
        }

        return true;
    }

    private static bool ContainsOuterReference(ExpressionNode? expression, string outerTable, string outerAlias)
    {
        return expression switch
        {
            null => false,
            ColumnRefNode column => !string.IsNullOrEmpty(column.TableOrAlias)
                && (column.TableOrAlias.Equals(outerTable, StringComparison.OrdinalIgnoreCase)
                    || column.TableOrAlias.Equals(outerAlias, StringComparison.OrdinalIgnoreCase)),
            ResolvedColumnRefNode resolved => resolved.TableName.Equals(outerTable, StringComparison.OrdinalIgnoreCase)
                || resolved.TableName.Equals(outerAlias, StringComparison.OrdinalIgnoreCase),
            BinaryExpressionNode binary => ContainsOuterReference(binary.Left, outerTable, outerAlias)
                || ContainsOuterReference(binary.Right, outerTable, outerAlias),
            InSubqueryExpressionNode => true,
            ExistsSubqueryExpressionNode => true,
            ScalarSubqueryExpressionNode => true,
            _ => false,
        };
    }

    private static bool TryResolveColumn(ExpressionNode? expression, out string? qualifier, out string column)
    {
        qualifier = null;
        column = string.Empty;

        switch (expression)
        {
            case ColumnRefNode columnRef:
                qualifier = string.IsNullOrWhiteSpace(columnRef.TableOrAlias) ? null : columnRef.TableOrAlias;
                column = columnRef.Column;
                return true;

            case ResolvedColumnRefNode resolved:
                qualifier = string.IsNullOrWhiteSpace(resolved.TableName) ? null : resolved.TableName;
                column = resolved.Column;
                return true;

            default:
                return false;
        }
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
