using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;
using DataVo.Core.Parser.Types;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Services;
using DataVo.Core.Utils;
using DataVo.Core.Parser.Utils;
using System.Globalization;
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
    private HashedTable? _fullJoin;

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

        // SQL three-valued logic: "column = NULL" is always unknown/false in WHERE filtering.
        if (rightLit.Value == null)
        {
            return [];
        }

        string tableName = leftCol.TableName;
        string leftValue = leftCol.Column;
        string rightValue = rightLit.Value?.ToString() ?? string.Empty;

        if (rightLit is not BoundValueLiteralNode && rightValue.StartsWith('\'') && rightValue.EndsWith('\''))
        {
            rightValue = rightValue[1..^1];
        }

        var table = TableService.GetTableDetailByAliasOrName(tableName);

        if (!CanFilterBeforeJoin(table))
        {
            return FilterFullJoin(table.TableName, row => row[leftValue] is { } candidate
                && ExpressionValueComparer.AreEqual(candidate, rightValue, trimQuotedStrings: true));
        }

        if (MVCC.MvccCoordinator.SnapshotRequiresScan(Runtime.DataVoEngine.Current(), table.DatabaseName!, table.TableName))
        {
            return EvaluateUsingFullScan(table, leftValue, rightValue);
        }

        if (table.IndexedColumns!.TryGetValue(leftValue, out string? indexFile))
        {
            if (SqlIndexLookupKey.TryBuildAuthoritative(table, leftValue, indexFile, rightLit.Value, out string secondaryKey, isBoundValue: rightLit is BoundValueLiteralNode))
            {
                return EvaluateUsingSecondaryIndex(table, secondaryKey, indexFile);
            }

            HashedTable indexed = EvaluateUsingSecondaryIndex(table, rightValue, indexFile);
            if (indexed.Count > 0)
            {
                return indexed;
            }

            return EvaluateUsingFullScan(table, leftValue, rightValue);
        }

        int columnIndex = table.PrimaryKeys!.IndexOf(leftValue);
        if (columnIndex > -1)
        {
            if (SqlIndexLookupKey.TryBuildAuthoritative(table, leftValue, $"_PK_{table.TableName}", rightLit.Value, out string primaryKey, isBoundValue: rightLit is BoundValueLiteralNode))
            {
                return EvaluateUsingPrimaryKey(table, primaryKey);
            }

            HashedTable indexed = EvaluateUsingPrimaryKey(table, rightValue);
            if (indexed.Count > 0)
            {
                return indexed;
            }

            return EvaluateUsingFullScan(table, leftValue, rightValue);
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
        List<RowKey> ids = [.. Runtime.DataVoEngine.Current().IndexManager.FilterUsingIndex(rightValue, indexFile, table.TableName, table.DatabaseName!)];
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
        List<RowKey> ids = [.. Runtime.DataVoEngine.Current().IndexManager.FilterUsingIndex(rightValue, $"_PK_{table.TableName}", table.TableName, table.DatabaseName!)];
        return LoadJoinedRowsFromContext(table, ids);
    }

    /// <summary>
    /// Loads the specified rows from the storage engine and passes them through the
    /// configured JOIN strategy to produce a <see cref="HashedTable"/>.
    /// </summary>
    /// <param name="table">The <see cref="TableDetail"/> identifying the source table and database.</param>
    /// <param name="ids">The list of row IDs to retrieve from the storage engine.</param>
    /// <returns>A <see cref="HashedTable"/> containing the loaded rows, fully joined.</returns>
    private HashedTable LoadJoinedRowsFromContext(TableDetail table, List<RowKey> ids)
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

        foreach (var entry in table.TableContent!)
        {
            if (!entry.Value.Values.TryGetValue(leftValue, out object? candidate) || candidate == null)
            {
                continue;
            }

            if (ExpressionValueComparer.AreEqual(candidate, rightValue, trimQuotedStrings: true))
            {
                tableRows.Add(entry.Key, entry.Value);
            }
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

        Func<object?, bool> pred = DeterminePredicate(root.Operator, rightVal);
        if (!CanFilterBeforeJoin(table))
        {
            return FilterFullJoin(table.TableName, row => pred(row[leftValue]));
        }

        TableData tableRows = [];
        foreach (var t in table.TableContent!)
        {
            if (pred(t.Value[leftValue]))
            {
                tableRows.Add(t.Key, t.Value);
            }
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Handles predicates of the form <c>distance(vector_column, literal_vector) &lt;op&gt; threshold</c>.
    /// </summary>
    protected override HashedTable HandleDistanceThresholdExpression(BinaryExpressionNode distanceExpression, string comparisonOperator, object? thresholdLiteral)
    {
        if (distanceExpression.Left is not ResolvedColumnRefNode vectorColumn
            || distanceExpression.Right is not LiteralNode vectorLiteral)
        {
            return [];
        }

        if (!VectorParser.TryCoerceToVector(vectorLiteral.Value, out float[] queryVector))
        {
            return [];
        }

        double threshold;
        if (!TryConvertThreshold(thresholdLiteral, out threshold))
        {
            return [];
        }

        var table = TableService.GetTableDetailByAliasOrName(vectorColumn.TableName);

        bool Matches(object? cell)
        {
            if (!VectorParser.TryCoerceToVector(cell, out float[] rowVector) || rowVector.Length != queryVector.Length)
            {
                return false;
            }

            double distance = distanceExpression.Operator == Operators.VECTOR_DISTANCE_COSINE
                ? ComputeCosineDistance(queryVector, rowVector)
                : ComputeEuclideanDistance(queryVector, rowVector);

            return EvaluateDistanceComparison(distance, threshold, comparisonOperator);
        }

        if (!CanFilterBeforeJoin(table))
        {
            return FilterFullJoin(table.TableName, row => Matches(row[vectorColumn.Column]));
        }

        TableData tableRows = [];
        foreach (var entry in table.TableContent!)
        {
            if (Matches(entry.Value[vectorColumn.Column]))
            {
                tableRows[entry.Key] = entry.Value;
            }
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Builds a predicate over one column value for the specified relational operator, comparing it to
    /// <paramref name="rightVal"/>.
    /// </summary>
    /// <param name="op">The relational operator (e.g., <c>=</c>, <c>!=</c>, <c>&lt;</c>, <c>&gt;</c>, <c>IS NULL</c>).</param>
    /// <param name="rightVal">The literal value to compare against.</param>
    /// <returns>A predicate that returns <c>true</c> for column values satisfying the condition.</returns>
    /// <exception cref="SecurityException">Thrown when <paramref name="op"/> is not a recognized operator.</exception>
    private static Func<object?, bool> DeterminePredicate(string op, object? rightVal)
    {
        return op switch
        {
            Operators.EQUALS => value => EvaluateEquality(value, rightVal),
            Operators.NOT_EQUALS => value => ExpressionValueComparer.SqlNotEquals(value, rightVal, trimQuotedStrings: true),
            Operators.LESS_THAN => value => CompareDynamics(value, rightVal) < 0,
            Operators.GREATER_THAN => value => CompareDynamics(value, rightVal) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => value => CompareDynamics(value, rightVal) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => value => CompareDynamics(value, rightVal) >= 0,
            Operators.LIKE => value => ExpressionValueComparer.MatchesLike(value, rightVal, trimQuotedStrings: true),
            Operators.NOT_LIKE => value => ExpressionValueComparer.MatchesNotLike(value, rightVal, trimQuotedStrings: true),
            Operators.IS_NULL => value => value == null,
            Operators.IS_NOT_NULL => value => value != null,
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

        Func<object?, object?, bool> pred = DetermineTwoColumnPredicate(root.Operator);
        if (!CanFilterBeforeJoin(table))
        {
            return FilterFullJoin(table.TableName, row => pred(row[leftValue], row[rightValue]));
        }

        TableData tableRows = [];
        foreach (var t in table.TableContent!)
        {
            if (pred(t.Value[leftValue], t.Value[rightValue]))
            {
                tableRows.Add(t.Key, t.Value);
            }
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Builds a predicate that compares two column values of the same row.
    /// </summary>
    /// <param name="op">The relational operator (e.g., <c>=</c>, <c>!=</c>, <c>&lt;</c>, <c>&gt;</c>).</param>
    /// <returns>A predicate over (left value, right value) that returns <c>true</c> when the operator holds.</returns>
    /// <exception cref="SecurityException">Thrown when <paramref name="op"/> is not a recognized operator.</exception>
    private static Func<object?, object?, bool> DetermineTwoColumnPredicate(string op)
    {
        return op switch
        {
            Operators.EQUALS => (left, right) => EvaluateEquality(left, right),
            Operators.NOT_EQUALS => (left, right) => ExpressionValueComparer.SqlNotEquals(left, right, trimQuotedStrings: true),
            Operators.LESS_THAN => (left, right) => CompareDynamics(left, right) < 0,
            Operators.GREATER_THAN => (left, right) => CompareDynamics(left, right) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => (left, right) => CompareDynamics(left, right) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => (left, right) => CompareDynamics(left, right) >= 0,
            Operators.LIKE => (left, right) => ExpressionValueComparer.MatchesLike(left, right, trimQuotedStrings: true),
            Operators.NOT_LIKE => (left, right) => ExpressionValueComparer.MatchesNotLike(left, right, trimQuotedStrings: true),
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
            Operators.NOT_EQUALS => ExpressionValueComparer.SqlNotEquals(leftVal, rightVal, trimQuotedStrings: true),
            Operators.LESS_THAN => CompareDynamics(leftVal, rightVal) < 0,
            Operators.GREATER_THAN => CompareDynamics(leftVal, rightVal) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) >= 0,
            Operators.LIKE => ExpressionValueComparer.MatchesLike(leftVal, rightVal, trimQuotedStrings: true),
            Operators.NOT_LIKE => ExpressionValueComparer.MatchesNotLike(leftVal, rightVal, trimQuotedStrings: true),
            Operators.IS_NULL => leftVal == null,
            Operators.IS_NOT_NULL => leftVal != null,
            _ => throw new SecurityException("Invalid operator")
        };
    }

    /// <summary>
    /// Whether a predicate on <paramref name="table"/> may filter that table's rows before the join, which is
    /// how leaves are normally evaluated. It is equivalent to filtering the joined rows only when no outer join
    /// can pad the table with NULLs or keep rows the filter would remove: all joins are INNER/CROSS, or the
    /// table is the FROM table and every join preserves it (INNER, LEFT, CROSS).
    /// </summary>
    private bool CanFilterBeforeJoin(TableDetail table)
    {
        List<JoinModel.JoinCondition> conditions = Join!.Model.JoinConditions;
        if (conditions.All(condition => IsJoinType(condition, JoinTypes.INNER) || IsJoinType(condition, JoinTypes.CROSS)))
        {
            return true;
        }

        return table.TableName == FromTable!.TableName
            && conditions.All(condition => IsJoinType(condition, JoinTypes.INNER)
                || IsJoinType(condition, JoinTypes.LEFT)
                || IsJoinType(condition, JoinTypes.CROSS));
    }

    private static bool IsJoinType(JoinModel.JoinCondition condition, string joinType) =>
        condition.JoinType.Equals(joinType, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Evaluates a predicate on <paramref name="tableName"/> after the join: the full join is computed once per
    /// evaluator and its rows are kept when the table's part of the row (NULL-padded for outer joins) matches.
    /// Keys are the same canonical keys filter-before-join leaves produce, so AND/OR combine them.
    /// </summary>
    private HashedTable FilterFullJoin(string tableName, Func<Row, bool> predicate)
    {
        _fullJoin ??= GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);

        HashedTable result = [];
        foreach (KeyValuePair<JoinedRowId, JoinedRow> entry in _fullJoin)
        {
            if (entry.Value.ContainsKey(tableName) && predicate(entry.Value[tableName]))
            {
                result.Add(entry.Key, entry.Value);
            }
        }

        return result;
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
    private static bool EvaluateEquality(object? leftVal, object? rightVal)
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
    private static int? CompareDynamics(object? left, object? right)
    {
        if (left == null || right == null) return null;
        return ExpressionValueComparer.Compare(left, right, trimQuotedStrings: true);
    }

    private static bool TryConvertThreshold(object? rawValue, out double threshold)
    {
        threshold = 0d;
        if (rawValue is null)
        {
            return false;
        }

        return rawValue switch
        {
            byte v => Assign(v, out threshold),
            sbyte v => Assign(v, out threshold),
            short v => Assign(v, out threshold),
            ushort v => Assign(v, out threshold),
            int v => Assign(v, out threshold),
            uint v => Assign(v, out threshold),
            long v => Assign(v, out threshold),
            ulong v => Assign(v, out threshold),
            float v => Assign(v, out threshold),
            double v => Assign(v, out threshold),
            decimal v => Assign((double)v, out threshold),
            string s => TryParseInvariantDouble(s, out threshold),
            _ => false
        };

        static bool Assign(double value, out double output)
        {
            output = value;
            return true;
        }

        static bool TryParseInvariantDouble(string value, out double output)
        {
            string candidate = value.Trim();
            if ((candidate.StartsWith("'", StringComparison.Ordinal) && candidate.EndsWith("'", StringComparison.Ordinal))
                || (candidate.StartsWith("\"", StringComparison.Ordinal) && candidate.EndsWith("\"", StringComparison.Ordinal)))
            {
                candidate = candidate[1..^1].Trim();
            }

            if (double.TryParse(
                candidate.AsSpan(),
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out output))
            {
                return true;
            }

            return double.TryParse(
                candidate.AsSpan(),
                NumberStyles.Float,
                CultureInfo.CurrentCulture,
                out output);
        }
    }

    private static bool EvaluateDistanceComparison(double distance, double threshold, string comparisonOperator)
    {
        return comparisonOperator switch
        {
            Operators.LESS_THAN => distance < threshold,
            Operators.LESS_THAN_OR_EQUAL_TO => distance <= threshold,
            Operators.GREATER_THAN => distance > threshold,
            Operators.GREATER_THAN_OR_EQUAL_TO => distance >= threshold,
            _ => false
        };
    }

    private static double ComputeEuclideanDistance(float[] left, float[] right)
    {
        double sum = 0d;
        for (int i = 0; i < left.Length; i++)
        {
            double diff = left[i] - right[i];
            sum += diff * diff;
        }

        return Math.Sqrt(sum);
    }

    private static double ComputeCosineDistance(float[] left, float[] right)
    {
        double dot = 0d;
        double magLeft = 0d;
        double magRight = 0d;

        for (int i = 0; i < left.Length; i++)
        {
            dot += left[i] * right[i];
            magLeft += left[i] * left[i];
            magRight += right[i] * right[i];
        }

        if (magLeft == 0d || magRight == 0d)
        {
            return 1d;
        }

        double similarity = dot / (Math.Sqrt(magLeft) * Math.Sqrt(magRight));
        return 1d - similarity;
    }
}