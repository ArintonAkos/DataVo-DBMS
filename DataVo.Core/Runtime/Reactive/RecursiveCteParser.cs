using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;
using SqlLexer = DataVo.Core.Parser.Lexer;
using SqlParser = DataVo.Core.Parser.Parser;
using SqlToken = DataVo.Core.Parser.Token;
using SqlTokenType = DataVo.Core.Parser.TokenType;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Describes how each maintained CTE column is produced by the recursive arm: from the joined CTE side
/// or from the joined base-table side, by source column name.
/// </summary>
/// <param name="FromCte"><c>true</c> when this output column reads the recursive CTE side; otherwise the base side.</param>
/// <param name="SourceColumn">The (unqualified) source column on the chosen side.</param>
internal readonly record struct RecursiveColumnSource(bool FromCte, string SourceColumn);

/// <summary>
/// The extracted, validated shape of a supported linear <c>WITH RECURSIVE</c> reachability/closure CTE:
/// <c>WITH RECURSIVE cte AS (&lt;base&gt; UNION ALL &lt;recursive&gt;) SELECT … FROM cte</c>, where the
/// recursive arm equi-joins <c>cte</c> to a single base table.
/// </summary>
/// <param name="CteName">The recursive CTE name.</param>
/// <param name="OutputColumns">The CTE's output column names (the base arm projection names).</param>
/// <param name="BaseTable">The base arm's single source table.</param>
/// <param name="BaseSourceColumns">The base arm's projected source columns, aligned with <paramref name="OutputColumns"/>.</param>
/// <param name="RecursiveBaseTable">The base table the recursive arm joins the CTE to.</param>
/// <param name="CteJoinColumn">The CTE-side equi-join column (an output column of the CTE).</param>
/// <param name="BaseJoinColumn">The base-table-side equi-join column.</param>
/// <param name="RecursiveColumns">Per output column, how the recursive arm derives it (CTE side or base side).</param>
/// <param name="FinalColumns">The final outer SELECT's projected columns over the CTE.</param>
/// <param name="FinalWhere">The optional final outer SELECT WHERE predicate over the CTE.</param>
internal sealed record RecursiveCteShape(
    string CteName,
    IReadOnlyList<string> OutputColumns,
    string BaseTable,
    IReadOnlyList<string> BaseSourceColumns,
    string RecursiveBaseTable,
    string CteJoinColumn,
    string BaseJoinColumn,
    IReadOnlyList<RecursiveColumnSource> RecursiveColumns,
    IReadOnlyList<SelectColumnNode> FinalColumns,
    ExpressionNode? FinalWhere);

/// <summary>
/// Recognizes the supported reactive <c>WITH RECURSIVE</c> shape. The engine's primary parser does not
/// accept the <c>RECURSIVE</c> keyword nor a <c>UNION ALL</c> CTE body, so this helper tokenizes the SQL,
/// slices the base arm, recursive arm, and final SELECT at their structural boundaries, and parses each
/// slice with the existing SELECT parser. Anything outside the supported linear closure shape is rejected
/// with <see cref="NotSupportedException"/> so behavior is honest about scope.
/// </summary>
internal static class RecursiveCteParser
{
    /// <summary>
    /// Cheap, allocation-light pre-check: returns <c>true</c> when the SQL begins with
    /// <c>WITH RECURSIVE</c>. Used to route to <see cref="Parse"/> before the engine's primary parser
    /// (which rejects the <c>RECURSIVE</c> keyword) is invoked. Returns <c>false</c> on any lex error.
    /// </summary>
    /// <param name="sql">The candidate reactive subscription SQL.</param>
    public static bool LooksRecursive(string sql)
    {
        List<SqlToken> tokens;
        try
        {
            tokens = new SqlLexer(sql).Tokenize();
        }
        catch
        {
            return false;
        }

        int i = SkipTrivia(tokens, 0);
        if (!IsValue(tokens, i, "WITH"))
        {
            return false;
        }

        i = SkipTrivia(tokens, i + 1);
        return IsValue(tokens, i, "RECURSIVE");
    }

    /// <summary>
    /// Tokenizes the SQL and slices it into the base arm, recursive arm, and final SELECT at their
    /// structural boundaries — the top-level <c>UNION ALL</c> and the CTE body's matching close paren,
    /// both located at brace depth 1 — then parses and validates each slice into a
    /// <see cref="RecursiveCteShape"/>.
    /// </summary>
    /// <param name="sql">The <c>WITH RECURSIVE … SELECT … FROM cte</c> statement.</param>
    /// <returns>The extracted, validated recursive-CTE shape.</returns>
    /// <exception cref="NotSupportedException">Thrown for any shape outside the supported linear closure form (missing/duplicate <c>UNION ALL</c>, <c>UNION</c>-distinct recursion, non-linear or mutual recursion, a non-equi recursive join, and so on).</exception>
    public static RecursiveCteShape Parse(string sql)
    {
        List<SqlToken> tokens = new SqlLexer(sql).Tokenize();

        int i = 0;
        Expect(tokens, ref i, "WITH");
        Expect(tokens, ref i, "RECURSIVE");

        SqlToken nameToken = tokens[i];
        if (nameToken.Type != SqlTokenType.Identifier && nameToken.Type != SqlTokenType.Keyword)
        {
            throw new NotSupportedException("Reactive recursive CTE requires a CTE name.");
        }

        string cteName = nameToken.Value;
        i++;
        Expect(tokens, ref i, "AS");
        ExpectPunctuation(tokens, ref i, "(");

        int bodyStart = i;

        // Find the top-level UNION ALL separating the two arms, and the matching close paren, at depth 1.
        int depth = 1;
        int unionIndex = -1;
        int closeIndex = -1;
        for (int j = bodyStart; j < tokens.Count; j++)
        {
            SqlToken t = tokens[j];
            if (t.Type == SqlTokenType.Punctuation && t.Value == "(")
            {
                depth++;
            }
            else if (t.Type == SqlTokenType.Punctuation && t.Value == ")")
            {
                depth--;
                if (depth == 0)
                {
                    closeIndex = j;
                    break;
                }
            }
            else if (depth == 1 && IsValue(tokens, j, "UNION"))
            {
                if (unionIndex >= 0)
                {
                    throw new NotSupportedException("Reactive recursive CTE supports exactly one UNION ALL between the base and recursive arms.");
                }

                int afterUnion = SkipTrivia(tokens, j + 1);
                if (!IsValue(tokens, afterUnion, "ALL"))
                {
                    throw new NotSupportedException("Reactive recursive CTE requires UNION ALL (UNION-distinct recursion is not supported).");
                }

                unionIndex = j;
            }
        }

        if (unionIndex < 0 || closeIndex < 0)
        {
            throw new NotSupportedException("Reactive recursive CTE must have the form (<base> UNION ALL <recursive>).");
        }

        int allIndex = SkipTrivia(tokens, unionIndex + 1);

        List<SqlToken> baseTokens = Slice(tokens, bodyStart, unionIndex);
        List<SqlToken> recursiveTokens = Slice(tokens, allIndex + 1, closeIndex);
        List<SqlToken> finalTokens = Slice(tokens, closeIndex + 1, tokens.Count - TrailingEof(tokens));

        SelectStatement baseSelect = ParseSelect(baseTokens, "recursive CTE base arm");
        SelectStatement recursiveSelect = ParseSelect(recursiveTokens, "recursive CTE recursive arm");
        SelectStatement finalSelect = ParseSelect(finalTokens, "recursive CTE final SELECT");

        return Build(cteName, baseSelect, recursiveSelect, finalSelect);
    }

    private static RecursiveCteShape Build(
        string cteName,
        SelectStatement baseSelect,
        SelectStatement recursiveSelect,
        SelectStatement finalSelect)
    {
        // Base arm: SELECT <cols> FROM <baseTable> with no joins/grouping/etc.
        if (baseSelect.FromTable is null
            || baseSelect.Joins.Count > 0
            || baseSelect.IsDistinct
            || baseSelect.GroupByExpression is not null
            || baseSelect.HavingExpression is not null
            || baseSelect.OrderByExpression is not null
            || baseSelect.LimitExpression is not null
            || baseSelect.WhereExpression is not null
            || baseSelect.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive recursive CTE base arm must be a plain SELECT over one base table.");
        }

        var outputColumns = new List<string>();
        var baseSourceColumns = new List<string>();
        foreach (SelectColumnNode column in baseSelect.Columns)
        {
            if (!TryColumn(column.Expression, out _, out string source))
            {
                throw new NotSupportedException("Reactive recursive CTE base arm must project plain columns.");
            }

            baseSourceColumns.Add(source);
            outputColumns.Add(column.Alias ?? source);
        }

        if (outputColumns.Count < 1)
        {
            throw new NotSupportedException("Reactive recursive CTE must project at least one column.");
        }

        // Recursive arm: SELECT <cols> FROM cte <a> INNER JOIN baseTable <b> ON <a/b equality>.
        if (recursiveSelect.FromTable is null
            || recursiveSelect.Joins.Count != 1
            || recursiveSelect.IsDistinct
            || recursiveSelect.GroupByExpression is not null
            || recursiveSelect.HavingExpression is not null
            || recursiveSelect.OrderByExpression is not null
            || recursiveSelect.LimitExpression is not null
            || recursiveSelect.WhereExpression is not null
            || recursiveSelect.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive recursive CTE recursive arm must be a single INNER JOIN of the CTE to one base table.");
        }

        JoinDetailNode join = recursiveSelect.Joins[0];
        if (!JoinTypes.INNER.Equals(join.JoinType, StringComparison.OrdinalIgnoreCase) || join.Condition is null)
        {
            throw new NotSupportedException("Reactive recursive CTE recursive arm requires an INNER JOIN with an equality ON condition.");
        }

        string fromTable = recursiveSelect.FromTable.Name;
        string fromAlias = recursiveSelect.FromAlias?.Name ?? fromTable;
        string joinTable = join.TableName.Name;
        string joinAlias = join.Alias?.Name ?? joinTable;

        // Exactly one side of the join must be the CTE; the other is the recursive base table. Linear
        // recursion: the recursive arm references the CTE once. (Mutual/non-linear recursion is rejected:
        // a second CTE reference would appear as a self-join to the same name, which has identical aliases
        // resolved to the CTE on both sides — disallowed below.)
        bool fromIsCte = fromTable.Equals(cteName, StringComparison.OrdinalIgnoreCase);
        bool joinIsCte = joinTable.Equals(cteName, StringComparison.OrdinalIgnoreCase);
        if (fromIsCte == joinIsCte)
        {
            throw new NotSupportedException("Reactive recursive CTE recursive arm must join the CTE to exactly one base table (linear recursion).");
        }

        string cteAlias = fromIsCte ? fromAlias : joinAlias;
        string recursiveBaseTable = fromIsCte ? joinTable : fromTable;
        string recursiveBaseAlias = fromIsCte ? joinAlias : fromAlias;

        if (recursiveBaseTable.Equals(cteName, StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException("Reactive recursive CTE does not support mutual recursion.");
        }

        // Resolve the equi-join key into (cteColumn, baseColumn).
        if (!ResolveJoinKey(join.Condition, cteAlias, recursiveBaseAlias, recursiveBaseTable, out string cteJoinColumn, out string baseJoinColumn))
        {
            throw new NotSupportedException("Reactive recursive CTE join must equate one CTE column to one base-table column.");
        }

        // Map each recursive-arm projected column to its source side (CTE vs base table).
        if (recursiveSelect.Columns.Count != outputColumns.Count)
        {
            throw new NotSupportedException("Reactive recursive CTE arms must project the same number of columns.");
        }

        var recursiveColumns = new List<RecursiveColumnSource>();
        foreach (SelectColumnNode column in recursiveSelect.Columns)
        {
            if (!TryColumn(column.Expression, out string? qualifier, out string source) || qualifier is null)
            {
                throw new NotSupportedException("Reactive recursive CTE recursive arm must project qualified plain columns (e.g. r.Src, e.Dst).");
            }

            bool fromCte = QualifierIs(qualifier, cteAlias, cteName);
            bool fromBase = QualifierIs(qualifier, recursiveBaseAlias, recursiveBaseTable);
            if (fromCte == fromBase)
            {
                throw new NotSupportedException($"Reactive recursive CTE projection '{column.RawExpression}' is ambiguous between the CTE and base table.");
            }

            recursiveColumns.Add(new RecursiveColumnSource(fromCte, source));
        }

        // Final SELECT: must read from the CTE only, no joins/aggregates/grouping. Optional WHERE.
        if (finalSelect.FromTable is null
            || !finalSelect.FromTable.Name.Equals(cteName, StringComparison.OrdinalIgnoreCase)
            || finalSelect.Joins.Count > 0
            || finalSelect.GroupByExpression is not null
            || finalSelect.HavingExpression is not null
            || finalSelect.OrderByExpression is not null
            || finalSelect.LimitExpression is not null
            || finalSelect.Ctes.Count > 0)
        {
            throw new NotSupportedException("Reactive recursive CTE final SELECT must read the CTE only (no joins/aggregates).");
        }

        return new RecursiveCteShape(
            cteName,
            outputColumns,
            baseSelect.FromTable.Name,
            baseSourceColumns,
            recursiveBaseTable,
            cteJoinColumn,
            baseJoinColumn,
            recursiveColumns,
            finalSelect.Columns,
            finalSelect.WhereExpression);
    }

    private static bool ResolveJoinKey(
        JoinConditionNode condition,
        string cteAlias,
        string baseAlias,
        string baseTable,
        out string cteColumn,
        out string baseColumn)
    {
        cteColumn = string.Empty;
        baseColumn = string.Empty;

        ColumnRefNode a = condition.Left;
        ColumnRefNode b = condition.Right;

        bool aCte = QualifierIs(a.TableOrAlias, cteAlias, cteAlias);
        bool aBase = QualifierIs(a.TableOrAlias, baseAlias, baseTable);
        bool bCte = QualifierIs(b.TableOrAlias, cteAlias, cteAlias);
        bool bBase = QualifierIs(b.TableOrAlias, baseAlias, baseTable);

        if (aCte && bBase && !aBase && !bCte)
        {
            cteColumn = a.Column;
            baseColumn = b.Column;
            return true;
        }

        if (bCte && aBase && !bBase && !aCte)
        {
            cteColumn = b.Column;
            baseColumn = a.Column;
            return true;
        }

        return false;
    }

    private static bool QualifierIs(string? qualifier, string alias, string table)
    {
        return qualifier is not null
            && (qualifier.Equals(alias, StringComparison.OrdinalIgnoreCase)
                || qualifier.Equals(table, StringComparison.OrdinalIgnoreCase));
    }

    private static bool TryColumn(ExpressionNode? expression, out string? qualifier, out string column)
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

    private static SelectStatement ParseSelect(List<SqlToken> tokens, string context)
    {
        if (tokens.Count == 0 || !(tokens[0].Type == SqlTokenType.Keyword && tokens[0].Value.Equals("SELECT", StringComparison.OrdinalIgnoreCase)))
        {
            throw new NotSupportedException($"Reactive {context} must be a SELECT.");
        }

        var withEof = new List<SqlToken>(tokens) { new SqlToken(SqlTokenType.EOF, string.Empty) };
        List<SqlStatement> statements;
        try
        {
            statements = new SqlParser(withEof).Parse();
        }
        catch (Exception ex)
        {
            throw new NotSupportedException($"Reactive {context} could not be parsed: {ex.Message}", ex);
        }

        if (statements.Count != 1 || statements[0] is not SelectStatement select)
        {
            throw new NotSupportedException($"Reactive {context} must be a single SELECT.");
        }

        return select;
    }

    private static List<SqlToken> Slice(List<SqlToken> tokens, int start, int endExclusive)
    {
        var slice = new List<SqlToken>();
        for (int j = start; j < endExclusive; j++)
        {
            slice.Add(tokens[j]);
        }

        return slice;
    }

    private static int TrailingEof(List<SqlToken> tokens) =>
        tokens.Count > 0 && tokens[^1].Type == SqlTokenType.EOF ? 1 : 0;

    private static int SkipTrivia(List<SqlToken> tokens, int index)
    {
        // The lexer emits no whitespace tokens, so this is effectively a bounds-safe identity used to keep
        // the structural scan readable.
        return index < tokens.Count ? index : tokens.Count;
    }

    private static bool IsValue(List<SqlToken> tokens, int index, string value)
    {
        return index >= 0 && index < tokens.Count
            && (tokens[index].Type == SqlTokenType.Keyword || tokens[index].Type == SqlTokenType.Identifier)
            && tokens[index].Value.Equals(value, StringComparison.OrdinalIgnoreCase);
    }

    private static void Expect(List<SqlToken> tokens, ref int index, string value)
    {
        if (!IsValue(tokens, index, value))
        {
            throw new NotSupportedException($"Reactive recursive CTE: expected '{value}'.");
        }

        index++;
    }

    private static void ExpectPunctuation(List<SqlToken> tokens, ref int index, string value)
    {
        if (index >= tokens.Count || tokens[index].Type != SqlTokenType.Punctuation || tokens[index].Value != value)
        {
            throw new NotSupportedException($"Reactive recursive CTE: expected '{value}'.");
        }

        index++;
    }
}
