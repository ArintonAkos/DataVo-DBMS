using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;
using DataVo.Core.Parser.Types;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Services;
using DataVo.Core.Utils;
using DataVo.Core.Parser.Utils;
using System.Security;

namespace DataVo.Core.Parser.Statements.Mechanism;

/// <summary>
/// Evaluates WHERE clause expressions against table records in the context of a JOIN operation.
/// <para>
/// This evaluator resolves predicates by choosing the most efficient access path available:
/// secondary B-Tree index lookup, primary key index lookup, or a full table scan.
/// After filtering from the base table, results are automatically passed through the
/// configured <see cref="Join"/> strategy to produce a fully joined <see cref="HashedTable"/>.
/// </para>
/// </summary>
/// <remarks>
/// Inherits from <see cref="ExpressionEvaluatorCore{TResult}"/> with <c>TResult</c> = <see cref="HashedTable"/>.
/// Logical operators (<c>AND</c>, <c>OR</c>) are handled by the base class, which delegates
/// leaf-node evaluation to the overridden methods in this class.
/// </remarks>
public class StatementEvaluator : ExpressionEvaluatorCore<HashedTable>
{
    private TableService TableService { get; set; }
    private Join? Join { get; set; }
    private TableDetail? FromTable { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="StatementEvaluator"/> class.
    /// </summary>
    /// <param name="tableService">The service that provides access to <see cref="TableDetail"/> metadata for all tables involved in the query.</param>
    /// <param name="joinStatements">The <see cref="Join"/> instance encapsulating the JOIN strategy (INNER, LEFT, etc.) used to combine table results.</param>
    /// <param name="fromTable">The primary (FROM) table whose rows are evaluated first before being joined with other tables.</param>
    public StatementEvaluator(TableService tableService, Join joinStatements, TableDetail fromTable)
    {
        TableService = tableService;
        Join = joinStatements;
        FromTable = fromTable;
    }

    /// <summary>
    /// Returns all rows from the base table when the WHERE clause resolves to an unconditional <c>TRUE</c>.
    /// All rows are passed through the configured JOIN strategy before being returned.
    /// </summary>
    /// <returns>A <see cref="HashedTable"/> containing every row from the base table, fully joined.</returns>
    protected override HashedTable EvaluateTrueLiteral()
    {
        return GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);
    }

    /// <summary>
    /// Returns an empty result set when the WHERE clause resolves to an unconditional <c>FALSE</c>.
    /// </summary>
    /// <returns>An empty <see cref="HashedTable"/>.</returns>
    protected override HashedTable EvaluateFalseLiteral() => [];

    /// <summary>
    /// Handles an equality comparison (<c>column = literal</c>) by selecting the fastest available access path:
    /// <list type="number">
    ///   <item><description>Secondary index lookup — if the column has a B-Tree index defined.</description></item>
    ///   <item><description>Primary key index lookup — if the column is part of the table's primary key.</description></item>
    ///   <item><description>Full table scan — as a fallback when no index covers the column.</description></item>
    /// </list>
    /// String literal values are trimmed of surrounding single quotes before comparison.
    /// </summary>
    /// <param name="root">A <see cref="BinaryExpressionNode"/> whose left operand is a <see cref="ResolvedColumnRefNode"/> and right operand is a <see cref="LiteralNode"/>.</param>
    /// <returns>A <see cref="HashedTable"/> containing the matching rows, fully joined via the configured JOIN strategy.</returns>
    protected override HashedTable HandleIndexableStatement(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        string tableName = leftCol.TableName;
        string leftValue = leftCol.Column;
        string rightValue = rightLit.Value?.ToString() ?? string.Empty;

        if (rightValue.StartsWith('\'') && rightValue.EndsWith('\''))
        {
            rightValue = rightValue.Trim('\'');
        }

        var table = TableService.GetTableDetailByAliasOrName(tableName);

        if (table.IndexedColumns!.TryGetValue(leftValue, out string? indexFile))
        {
            return EvaluateUsingSecondaryIndex(table, rightValue, indexFile);
        }

        int columnIndex = table.PrimaryKeys!.IndexOf(leftValue);
        if (columnIndex > -1)
        {
            return EvaluateUsingPrimaryKey(table, rightValue);
        }

        return EvaluateUsingFullScan(table, leftValue, rightValue);
    }

    /// <summary>
    /// Retrieves matching rows by performing a B-Tree lookup against a secondary index.
    /// Row IDs returned by the index are then loaded from the storage engine and joined.
    /// </summary>
    /// <param name="table">The <see cref="TableDetail"/> describing the table to query.</param>
    /// <param name="rightValue">The value to search for in the secondary index.</param>
    /// <param name="indexFile">The filename of the secondary index B-Tree file.</param>
    /// <returns>A <see cref="HashedTable"/> containing the matched and joined rows.</returns>
    private HashedTable EvaluateUsingSecondaryIndex(TableDetail table, string rightValue, string indexFile)
    {
        List<long> ids = [.. Runtime.DataVoEngine.Current().IndexManager.FilterUsingIndex(rightValue, indexFile, table.TableName, table.DatabaseName!)];
        return LoadJoinedRowsFromContext(table, ids);
    }

    /// <summary>
    /// Retrieves matching rows by performing a B-Tree lookup against the table's primary key index (<c>_PK_{TableName}</c>).
    /// Row IDs returned by the index are then loaded from the storage engine and joined.
    /// </summary>
    /// <param name="table">The <see cref="TableDetail"/> describing the table to query.</param>
    /// <param name="rightValue">The primary key value to search for.</param>
    /// <returns>A <see cref="HashedTable"/> containing the matched and joined rows.</returns>
    private HashedTable EvaluateUsingPrimaryKey(TableDetail table, string rightValue)
    {
        List<long> ids = [.. Runtime.DataVoEngine.Current().IndexManager.FilterUsingIndex(rightValue, $"_PK_{table.TableName}", table.TableName, table.DatabaseName!)];
        return LoadJoinedRowsFromContext(table, ids);
    }

    /// <summary>
    /// Loads the specified rows from the storage engine and passes them through the
    /// configured JOIN strategy to produce a <see cref="HashedTable"/>.
    /// </summary>
    /// <param name="table">The <see cref="TableDetail"/> identifying the source table and database.</param>
    /// <param name="ids">The list of row IDs to retrieve from the storage engine.</param>
    /// <returns>A <see cref="HashedTable"/> containing the loaded rows, fully joined.</returns>
    private HashedTable LoadJoinedRowsFromContext(TableDetail table, List<long> ids)
    {
        var internalRows = Runtime.DataVoEngine.Current().StorageContext.SelectFromTable(ids, [], table.TableName, table.DatabaseName!);

        TableData tableRows = [];
        foreach (var kvp in internalRows)
        {
            tableRows[kvp.Key] = new Record(kvp.Key, kvp.Value);
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Falls back to a full table scan, iterating over every row and comparing the specified
    /// column value to the expected value using string equality. Matching rows are then joined.
    /// </summary>
    /// <param name="table">The <see cref="TableDetail"/> containing the in-memory table content to scan.</param>
    /// <param name="leftValue">The name of the column to evaluate.</param>
    /// <param name="rightValue">The expected value to match against (string comparison).</param>
    /// <returns>A <see cref="HashedTable"/> containing all matching rows, fully joined.</returns>
    private HashedTable EvaluateUsingFullScan(TableDetail table, string leftValue, string rightValue)
    {
        TableData tableRows = [];
        foreach (var entry in table.TableContent!.Where(entry => entry.Value[leftValue].ToString() == rightValue))
        {
            tableRows.Add(entry.Key, entry.Value);
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Handles a non-equality comparison between a column and a literal (e.g., <c>&gt;</c>, <c>&lt;</c>, <c>!=</c>, <c>IS NULL</c>).
    /// Because these operators cannot leverage B-Tree index lookups, a full table scan is performed
    /// and each row is tested with the appropriate predicate. Matching rows are then joined.
    /// </summary>
    /// <param name="root">A <see cref="BinaryExpressionNode"/> whose left operand is a <see cref="ResolvedColumnRefNode"/> and right operand is a <see cref="LiteralNode"/>.</param>
    /// <returns>A <see cref="HashedTable"/> containing the filtered and joined rows.</returns>
    protected override HashedTable HandleNonIndexableStatement(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        string tableName = leftCol.TableName;
        string leftValue = leftCol.Column;
        var rightVal = rightLit.Value;

        var table = TableService.GetTableDetailByAliasOrName(tableName);

        Func<KeyValuePair<long, Record>, bool> pred = DeterminePredicate(root.Operator, leftValue, rightVal);

        TableData tableRows = [];
        foreach (var t in table.TableContent!.Where(pred))
        {
            tableRows.Add(t.Key, t.Value);
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Builds a row-filtering predicate based on the specified relational operator.
    /// The predicate reads the value of <paramref name="leftValue"/> from each row and compares it to <paramref name="rightVal"/>.
    /// </summary>
    /// <param name="op">The relational operator (e.g., <c>=</c>, <c>!=</c>, <c>&lt;</c>, <c>&gt;</c>, <c>IS NULL</c>).</param>
    /// <param name="leftValue">The column name whose value is extracted from each row for comparison.</param>
    /// <param name="rightVal">The literal value to compare against.</param>
    /// <returns>A predicate function that returns <c>true</c> for rows satisfying the condition.</returns>
    /// <exception cref="SecurityException">Thrown when <paramref name="op"/> is not a recognized operator.</exception>
    private Func<KeyValuePair<long, Record>, bool> DeterminePredicate(string op, string leftValue, object? rightVal)
    {
        return op switch
        {
            Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], rightVal),
            Operators.NOT_EQUALS => entry => !EvaluateEquality(entry.Value[leftValue], rightVal),
            Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) < 0,
            Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) >= 0,
            Operators.LIKE => entry => ExpressionValueComparer.MatchesLike(entry.Value[leftValue], rightVal, trimQuotedStrings: true),
            Operators.IS_NULL => entry => entry.Value[leftValue] == null,
            Operators.IS_NOT_NULL => entry => entry.Value[leftValue] != null,
            _ => throw new SecurityException("Invalid operator")
        };
    }

    /// <summary>
    /// Handles a comparison between two column references within the same table (e.g., <c>col_a = col_b</c>).
    /// Both columns must belong to the same table; cross-table column comparisons in the WHERE clause
    /// are not supported and will throw an exception.
    /// </summary>
    /// <param name="root">A <see cref="BinaryExpressionNode"/> whose left and right operands are both <see cref="ResolvedColumnRefNode"/> instances.</param>
    /// <returns>A <see cref="HashedTable"/> containing the matching and joined rows.</returns>
    /// <exception cref="SecurityException">Thrown when the two columns reference different tables.</exception>
    protected override HashedTable HandleTwoColumnExpression(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightCol = (ResolvedColumnRefNode)root.Right;

        string tableName = leftCol.TableName;
        string rightTableName = rightCol.TableName;

        if (tableName != rightTableName)
        {
            throw new SecurityException("Join like statement not permitted in where clause!");
        }

        string leftValue = leftCol.Column;
        string rightValue = rightCol.Column;

        var table = TableService.GetTableDetailByAliasOrName(tableName);

        Func<KeyValuePair<long, Record>, bool> pred = DetermineTwoColumnPredicate(root.Operator, leftValue, rightValue);

        TableData tableRows = [];
        foreach (var t in table.TableContent!.Where(pred))
        {
            tableRows.Add(t.Key, t.Value);
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Builds a row-filtering predicate that compares two column values within the same row.
    /// </summary>
    /// <param name="op">The relational operator (e.g., <c>=</c>, <c>!=</c>, <c>&lt;</c>, <c>&gt;</c>).</param>
    /// <param name="leftValue">The name of the first column.</param>
    /// <param name="rightValue">The name of the second column.</param>
    /// <returns>A predicate function that returns <c>true</c> for rows where the two column values satisfy the operator.</returns>
    /// <exception cref="SecurityException">Thrown when <paramref name="op"/> is not a recognized operator.</exception>
    private Func<KeyValuePair<long, Record>, bool> DetermineTwoColumnPredicate(string op, string leftValue, string rightValue)
    {
        return op switch
        {
            Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
            Operators.NOT_EQUALS => entry => !EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
            Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) < 0,
            Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) >= 0,
            Operators.LIKE => entry => ExpressionValueComparer.MatchesLike(entry.Value[leftValue], entry.Value[rightValue], trimQuotedStrings: true),
            _ => throw new SecurityException("Invalid operator")
        };
    }

    /// <summary>
    /// Evaluates constant expressions that do not depend on any column values, 
    /// returning either the full table or an empty set based on the condition's truth value.
    /// </summary>
    /// <param name="root">The binary expression node containing two literals and an operator.</param>
    /// <returns><see cref="HashedTable"/> containing all rows if the condition is true, or empty if false.</returns>
    protected override HashedTable HandleConstantExpression(BinaryExpressionNode root)
    {
        var leftLit = (LiteralNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        object? leftVal = leftLit.Value;
        object? rightVal = rightLit.Value;

        bool isCondTrue = DetermineConstantCondition(root.Operator, leftVal, rightVal);

        if (isCondTrue)
        {
            return GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);
        }

        return [];
    }

    /// <summary>
    /// Determines the truth value of a constant condition by evaluating the operator against the literal values.
    /// </summary>
    /// <param name="op">The specific relational operator to evaluate.</param>
    /// <param name="leftVal">The left literal value to compare.</param>
    /// <param name="rightVal">The right literal value to compare.</param>
    /// <returns>>True if the condition holds based on the operator and values; otherwise, false.</returns>
    /// <example>
    /// For example, for the expression '5 > 3', this method would evaluate the
    /// operator '>' against the left value '5' and the right value '3', returning true.
    /// </example>
    /// <exception cref="SecurityException">Thrown when an invalid operator is provided.</exception>
    private static bool DetermineConstantCondition(string op, object? leftVal, object? rightVal)
    {
        return op switch
        {
            Operators.EQUALS => EvaluateEquality(leftVal, rightVal),
            Operators.NOT_EQUALS => !EvaluateEquality(leftVal, rightVal),
            Operators.LESS_THAN => CompareDynamics(leftVal, rightVal) < 0,
            Operators.GREATER_THAN => CompareDynamics(leftVal, rightVal) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) >= 0,
            Operators.LIKE => ExpressionValueComparer.MatchesLike(leftVal, rightVal, trimQuotedStrings: true),
            Operators.IS_NULL => leftVal == null,
            Operators.IS_NOT_NULL => leftVal != null,
            _ => throw new SecurityException("Invalid operator")
        };
    }

    /// <summary>
    /// Wraps the given table rows into a <see cref="HashedTable"/> keyed by <see cref="JoinedRowId"/>,
    /// then passes them through the configured <see cref="Join"/> strategy to incorporate data from
    /// any joined tables.
    /// </summary>
    /// <param name="tableRows">The filtered rows from the source table.</param>
    /// <param name="tableName">The name of the source table (used as the key in each <see cref="JoinedRow"/>).</param>
    /// <returns>A <see cref="HashedTable"/> containing the fully joined result set.</returns>
    private HashedTable GetJoinedTableContent(TableData tableRows, string tableName)
    {
        HashedTable groupedInitialTable = [];

        foreach (var row in tableRows)
        {
            groupedInitialTable.Add(new JoinedRowId(row.Key), new JoinedRow(tableName, row.Value.ToRow()));
        }

        return Join!.Evaluate(groupedInitialTable, tableName);
    }


    /// <summary>
    /// Combines two result sets using logical AND by intersecting their keys.
    /// Only rows present in both <paramref name="leftResult"/> and <paramref name="rightResult"/> are retained.
    /// </summary>
    /// <param name="leftResult">The result set from the left sub-expression.</param>
    /// <param name="rightResult">The result set from the right sub-expression.</param>
    /// <returns>A <see cref="HashedTable"/> containing only the rows that appear in both inputs.</returns>
    protected override HashedTable And(HashedTable leftResult, HashedTable rightResult)
    {
        var result = leftResult.Keys.Intersect(rightResult.Keys)
               .ToDictionary(t => t, t => leftResult[t]);

        return new HashedTable(result);
    }


    /// <summary>
    /// Combines two result sets using logical OR by computing their union.
    /// If a row exists in both sets, the entry from <paramref name="leftResult"/> takes precedence.
    /// </summary>
    /// <param name="leftResult">The result set from the left sub-expression.</param>
    /// <param name="rightResult">The result set from the right sub-expression.</param>
    /// <returns>A <see cref="HashedTable"/> containing all rows from either input.</returns>
    protected override HashedTable Or(HashedTable leftResult, HashedTable rightResult)
    {
        HashSet<JoinedRowId> leftHashes = [.. leftResult.Keys];
        HashSet<JoinedRowId> rightHashes = [.. rightResult.Keys];

        HashSet<JoinedRowId> unionResult = [.. leftHashes.Union(rightHashes)];

        HashedTable result = [];
        foreach (JoinedRowId hash in unionResult)
        {
            if (leftResult.ContainsKey(hash))
            {
                result.Add(hash, leftResult[hash]);
                continue;
            }

            result.Add(hash, rightResult[hash]);
        }

        return result;
    }


    /// <summary>
    /// Compares two dynamically typed values for equality. Quoted strings are trimmed before comparison.
    /// Returns <c>false</c> if either value is <c>null</c>.
    /// </summary>
    /// <param name="leftVal">The left-hand value.</param>
    /// <param name="rightVal">The right-hand value.</param>
    /// <returns><c>true</c> if the values are considered equal; otherwise, <c>false</c>.</returns>
    private static bool EvaluateEquality(dynamic? leftVal, dynamic? rightVal)
    {
        if (leftVal == null || rightVal == null) return false;
        return ExpressionValueComparer.AreEqual(leftVal, rightVal, trimQuotedStrings: true);
    }

    /// <summary>
    /// Performs an ordered comparison between two dynamically typed values.
    /// Quoted strings are trimmed before comparison. Returns <c>null</c> if either value is <c>null</c>.
    /// </summary>
    /// <param name="left">The left-hand value.</param>
    /// <param name="right">The right-hand value.</param>
    /// <returns>
    /// A negative integer if <paramref name="left"/> is less than <paramref name="right"/>,
    /// zero if they are equal, a positive integer if <paramref name="left"/> is greater,
    /// or <c>null</c> if either operand is <c>null</c>.
    /// </returns>
    private static int? CompareDynamics(dynamic? left, dynamic? right)
    {
        if (left == null || right == null) return null;
        return ExpressionValueComparer.Compare(left, right, trimQuotedStrings: true);
    }
}