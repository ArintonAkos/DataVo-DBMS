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
/// Evaluates a WHERE clause over the rows of a JOIN, running the join once.
/// <para>
/// The WHERE is split into its top-level AND conjuncts. A conjunct that reads one table, and may filter that
/// table before the join without changing the result (<see cref="CanFilterBeforeJoin"/>), narrows that table's
/// rows, using an index for an equality where it can. The join then runs once, from the FROM table, over the
/// narrowed tables. The other conjuncts (several tables, or the NULL-supplied side of an outer join) are
/// tested on the joined rows. Every conjunct is tested with the same predicates either way.
/// </para>
/// </summary>
/// <remarks>
/// Logical operators (<c>AND</c>, <c>OR</c>) inside a conjunct are handled by the base class, which delegates
/// leaf-node evaluation to the overridden methods in this class; after the join, leaves filter the joined rows
/// and AND/OR intersect or unite their keys, which all come from the one join.
/// </remarks>
public class StatementEvaluator : ExpressionEvaluatorCore<HashedTable>
{
    private TableService TableService { get; set; }
    private Join Join { get; set; }
    private TableDetail FromTable { get; set; }

    /// <summary>The joined rows the leaves of a post-join conjunct filter.</summary>
    private HashedTable _joined = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="StatementEvaluator"/> class.
    /// </summary>
    /// <param name="tableService">The service that provides access to <see cref="TableDetail"/> metadata for all tables involved in the query.</param>
    /// <param name="joinStatements">The <see cref="Join"/> instance encapsulating the JOIN strategy (INNER, LEFT, etc.) used to combine table results.</param>
    /// <param name="fromTable">The primary (FROM) table the join starts from.</param>
    public StatementEvaluator(TableService tableService, Join joinStatements, TableDetail fromTable)
    {
        TableService = tableService;
        Join = joinStatements;
        FromTable = fromTable;
    }

    /// <summary>
    /// Returns the joined rows that satisfy <paramref name="root"/>, joining the tables once.
    /// </summary>
    /// <param name="root">The bound WHERE expression.</param>
    /// <returns>The matching joined rows, keyed by canonical joined-row keys.</returns>
    public HashedTable EvaluateWhere(ExpressionNode root)
    {
        var filtered = new Dictionary<string, TableData>(StringComparer.Ordinal);
        var residual = new List<ExpressionNode>();
        foreach (ExpressionNode conjunct in SplitConjuncts(root))
        {
            if (SingleTableOf(conjunct) is { } table && CanFilterBeforeJoin(table))
            {
                filtered.TryGetValue(table.TableName, out TableData? rows);
                filtered[table.TableName] = new TableFilterEvaluator(this, table, rows).Evaluate(conjunct);
            }
            else
            {
                residual.Add(conjunct);
            }
        }

        HashedTable joined = JoinOnce(filtered);
        foreach (ExpressionNode conjunct in residual)
        {
            if (joined.Count == 0)
            {
                break;
            }

            _joined = joined;
            joined = Evaluate(conjunct);
        }

        return joined;
    }

    /// <summary>The top-level AND conjuncts of <paramref name="node"/>, left to right.</summary>
    private static List<ExpressionNode> SplitConjuncts(ExpressionNode node)
    {
        var conjuncts = new List<ExpressionNode>();
        var pending = new Stack<ExpressionNode>();
        pending.Push(node);
        while (pending.Count > 0)
        {
            ExpressionNode current = pending.Pop();
            if (current is BinaryExpressionNode { Operator: Operators.AND } and)
            {
                pending.Push(and.Right);
                pending.Push(and.Left);
            }
            else
            {
                conjuncts.Add(current);
            }
        }

        return conjuncts;
    }

    /// <summary>
    /// The one table every column of <paramref name="conjunct"/> belongs to, or <see langword="null"/> when it
    /// reads several tables, none, or contains a node the leaves do not take apart.
    /// </summary>
    private TableDetail? SingleTableOf(ExpressionNode conjunct)
    {
        TableDetail? table = null;
        bool single = true;
        Visit(conjunct);
        return single ? table : null;

        void Visit(ExpressionNode node)
        {
            switch (node)
            {
                case ResolvedColumnRefNode column:
                    TableDetail detail = TableService.GetTableDetailByAliasOrName(column.TableName);
                    if (table is null)
                    {
                        table = detail;
                    }
                    else if (!string.Equals(table.TableName, detail.TableName, StringComparison.Ordinal))
                    {
                        single = false;
                    }

                    break;
                case BinaryExpressionNode binary:
                    Visit(binary.Left);
                    Visit(binary.Right);
                    break;
                case LiteralNode:
                    break;
                default:
                    single = false;
                    break;
            }
        }
    }

    /// <summary>
    /// Joins the tables once over the rows each filter left. An all-INNER join starts from the filtered table with
    /// the fewest rows, so only the rows that can match are wrapped as joined rows; row keys are canonical, so the
    /// result does not depend on the start. Any outer join starts from the FROM table, as its semantics require.
    /// </summary>
    private HashedTable JoinOnce(Dictionary<string, TableData> filtered)
    {
        string start = FromTable.TableName;
        if (Join.Model.JoinConditions.All(condition => IsJoinType(condition, JoinTypes.INNER)))
        {
            int fewest = filtered.TryGetValue(start, out TableData? fromRows) ? fromRows.Count : int.MaxValue;
            foreach ((string table, TableData rows) in filtered)
            {
                if (rows.Count < fewest)
                {
                    (start, fewest) = (table, rows.Count);
                }
            }
        }

        TableData startRows = filtered.Remove(start, out TableData? narrowed)
            ? narrowed
            : TableService.GetTableDetailByAliasOrName(start).TableContent!;
        return GetJoinedTableContent(startRows, start, filtered);
    }

    /// <summary>All joined rows: a WHERE that is TRUE keeps every row of the join.</summary>
    protected override HashedTable EvaluateTrueLiteral() => _joined;

    /// <summary>
    /// Returns an empty result set when the WHERE clause resolves to an unconditional <c>FALSE</c>.
    /// </summary>
    /// <returns>An empty <see cref="HashedTable"/>.</returns>
    protected override HashedTable EvaluateFalseLiteral() => [];

    /// <summary>Keeps the joined rows whose part from the compared table equals the literal.</summary>
    protected override HashedTable HandleIndexableStatement(BinaryExpressionNode root)
    {
        (TableDetail table, Func<Row, bool> test) = EqualityLeaf(root);
        return FilterJoined(table.TableName, test);
    }

    /// <summary>Keeps the joined rows whose part from the compared table meets a non-equality comparison.</summary>
    protected override HashedTable HandleNonIndexableStatement(BinaryExpressionNode root)
    {
        (TableDetail table, Func<Row, bool> test) = ComparisonLeaf(root);
        return FilterJoined(table.TableName, test);
    }

    /// <summary>
    /// Handles predicates of the form <c>distance(vector_column, literal_vector) &lt;op&gt; threshold</c>.
    /// </summary>
    protected override HashedTable HandleDistanceThresholdExpression(BinaryExpressionNode distanceExpression, string comparisonOperator, object? thresholdLiteral) =>
        DistanceLeaf(distanceExpression, comparisonOperator, thresholdLiteral) is { } leaf ? FilterJoined(leaf.Table.TableName, leaf.Test) : [];

    /// <summary>
    /// Handles a comparison between two columns of the same table (e.g., <c>col_a = col_b</c>); a comparison
    /// of columns of two tables is not supported in WHERE.
    /// </summary>
    /// <exception cref="SecurityException">Thrown when the two columns reference different tables.</exception>
    protected override HashedTable HandleTwoColumnExpression(BinaryExpressionNode root)
    {
        (TableDetail table, Func<Row, bool> test) = TwoColumnLeaf(root);
        return FilterJoined(table.TableName, test);
    }

    /// <summary>
    /// Evaluates constant expressions that do not depend on any column values, returning either every joined
    /// row or none.
    /// </summary>
    protected override HashedTable HandleConstantExpression(BinaryExpressionNode root) =>
        ConstantLeaf(root) ? _joined : [];

    /// <summary>Keeps the joined rows whose part from <paramref name="tableName"/> (NULL-padded for outer joins) passes.</summary>
    private HashedTable FilterJoined(string tableName, Func<Row, bool> test)
    {
        HashedTable result = [];
        foreach (KeyValuePair<JoinedRowId, JoinedRow> entry in _joined)
        {
            if (entry.Value.ContainsKey(tableName) && test(entry.Value[tableName]))
            {
                result.Add(entry.Key, entry.Value);
            }
        }

        return result;
    }

    // The leaves: each resolves its table and returns the test one row of that table must pass. The same test
    // filters a table before the join and the joined rows after it.

    private (TableDetail Table, Func<Row, bool> Test) EqualityLeaf(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightLit = (LiteralNode)root.Right;
        TableDetail table = TableService.GetTableDetailByAliasOrName(leftCol.TableName);
        string column = leftCol.Column;

        // SQL three-valued logic: "column = NULL" is always unknown/false in WHERE filtering.
        if (rightLit.Value == null)
        {
            return (table, static _ => false);
        }

        string rightValue = UnquotedLiteral(rightLit);
        return (table, row => row.ContainsKey(column) && row[column] is { } candidate
            && ExpressionValueComparer.AreEqual(candidate, rightValue, trimQuotedStrings: true));
    }

    private (TableDetail Table, Func<Row, bool> Test) ComparisonLeaf(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightLit = (LiteralNode)root.Right;
        TableDetail table = TableService.GetTableDetailByAliasOrName(leftCol.TableName);
        string column = leftCol.Column;
        Func<object?, bool> predicate = DeterminePredicate(root.Operator, rightLit.Value);
        return (table, row => predicate(row[column]));
    }

    private (TableDetail Table, Func<Row, bool> Test) TwoColumnLeaf(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightCol = (ResolvedColumnRefNode)root.Right;
        if (leftCol.TableName != rightCol.TableName)
        {
            throw new SecurityException("Join like statement not permitted in where clause!");
        }

        TableDetail table = TableService.GetTableDetailByAliasOrName(leftCol.TableName);
        string left = leftCol.Column;
        string right = rightCol.Column;
        Func<object?, object?, bool> predicate = DetermineTwoColumnPredicate(root.Operator);
        return (table, row => predicate(row[left], row[right]));
    }

    private (TableDetail Table, Func<Row, bool> Test)? DistanceLeaf(BinaryExpressionNode distanceExpression, string comparisonOperator, object? thresholdLiteral)
    {
        if (distanceExpression.Left is not ResolvedColumnRefNode vectorColumn
            || distanceExpression.Right is not LiteralNode vectorLiteral
            || !VectorParser.TryCoerceToVector(vectorLiteral.Value, out float[] queryVector)
            || !TryConvertThreshold(thresholdLiteral, out double threshold))
        {
            return null;
        }

        TableDetail table = TableService.GetTableDetailByAliasOrName(vectorColumn.TableName);
        string column = vectorColumn.Column;
        bool cosine = distanceExpression.Operator == Operators.VECTOR_DISTANCE_COSINE;
        return (table, row =>
        {
            if (!VectorParser.TryCoerceToVector(row[column], out float[] rowVector) || rowVector.Length != queryVector.Length)
            {
                return false;
            }

            double distance = cosine ? ComputeCosineDistance(queryVector, rowVector) : ComputeEuclideanDistance(queryVector, rowVector);
            return EvaluateDistanceComparison(distance, threshold, comparisonOperator);
        });
    }

    private static bool ConstantLeaf(BinaryExpressionNode root) =>
        DetermineConstantCondition(root.Operator, ((LiteralNode)root.Left).Value, ((LiteralNode)root.Right).Value);

    /// <summary>The literal's text without the single quotes an unbound string literal carries.</summary>
    private static string UnquotedLiteral(LiteralNode literal)
    {
        string value = literal.Value?.ToString() ?? string.Empty;
        return literal is not BoundValueLiteralNode && value.StartsWith('\'') && value.EndsWith('\'')
            ? value[1..^1]
            : value;
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
    /// Determines the truth value of a constant condition by evaluating the operator against the literal values.
    /// </summary>
    /// <param name="op">The specific relational operator to evaluate.</param>
    /// <param name="leftVal">The left literal value to compare.</param>
    /// <param name="rightVal">The right literal value to compare.</param>
    /// <returns>True if the condition holds based on the operator and values; otherwise, false.</returns>
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
    /// Whether a predicate on <paramref name="table"/> may filter that table's rows before the join. It is
    /// equivalent to filtering the joined rows only when no outer join can pad the table with NULLs or keep rows
    /// the filter would remove: all joins are INNER/CROSS, or the table is the FROM table and every join
    /// preserves it (INNER, LEFT, CROSS).
    /// </summary>
    private bool CanFilterBeforeJoin(TableDetail table)
    {
        List<JoinModel.JoinCondition> conditions = Join.Model.JoinConditions;
        if (conditions.All(condition => IsJoinType(condition, JoinTypes.INNER) || IsJoinType(condition, JoinTypes.CROSS)))
        {
            return true;
        }

        return table.TableName == FromTable.TableName
            && conditions.All(condition => IsJoinType(condition, JoinTypes.INNER)
                || IsJoinType(condition, JoinTypes.LEFT)
                || IsJoinType(condition, JoinTypes.CROSS));
    }

    private static bool IsJoinType(JoinModel.JoinCondition condition, string joinType) =>
        condition.JoinType.Equals(joinType, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Wraps the start table's rows into a <see cref="HashedTable"/> keyed by <see cref="JoinedRowId"/>, then
    /// passes them through the configured <see cref="Join"/> strategy, which reads the other tables' rows from
    /// <paramref name="tableRowsByName"/> where a filter narrowed them.
    /// </summary>
    private HashedTable GetJoinedTableContent(TableData tableRows, string tableName, IReadOnlyDictionary<string, TableData> tableRowsByName)
    {
        HashedTable groupedInitialTable = [];

        foreach (var row in tableRows)
        {
            groupedInitialTable.Add(new JoinedRowId(row.Key), new JoinedRow(tableName, row.Value.ToRow()));
        }

        return Join.Evaluate(groupedInitialTable, tableName, tableRowsByName);
    }

    /// <summary>
    /// Combines two result sets using logical AND by intersecting their keys.
    /// Only rows present in both <paramref name="leftResult"/> and <paramref name="rightResult"/> are retained.
    /// </summary>
    protected override HashedTable And(HashedTable leftResult, HashedTable rightResult)
    {
        HashedTable result = [];
        foreach (KeyValuePair<JoinedRowId, JoinedRow> entry in leftResult)
        {
            if (rightResult.ContainsKey(entry.Key))
            {
                result.Add(entry.Key, entry.Value);
            }
        }

        return result;
    }

    /// <summary>
    /// Combines two result sets using logical OR by computing their union.
    /// If a row exists in both sets, the entry from <paramref name="leftResult"/> takes precedence.
    /// </summary>
    protected override HashedTable Or(HashedTable leftResult, HashedTable rightResult)
    {
        HashedTable result = [];
        foreach (KeyValuePair<JoinedRowId, JoinedRow> entry in leftResult)
        {
            result.Add(entry.Key, entry.Value);
        }

        foreach (KeyValuePair<JoinedRowId, JoinedRow> entry in rightResult)
        {
            if (!result.ContainsKey(entry.Key))
            {
                result.Add(entry.Key, entry.Value);
            }
        }

        return result;
    }

    /// <summary>
    /// Narrows one table's rows by a conjunct that reads only that table, before the join. Leaves use the
    /// evaluator's tests; an equality on an indexed or key column reads the index while the table is unfiltered.
    /// </summary>
    private sealed class TableFilterEvaluator(StatementEvaluator owner, TableDetail table, TableData? source) : ExpressionEvaluatorCore<TableData>
    {
        /// <summary>The rows to narrow: the rows an earlier conjunct left, or the whole table.</summary>
        private TableData Source => source ?? table.TableContent!;

        protected override TableData EvaluateTrueLiteral() => Source;

        protected override TableData EvaluateFalseLiteral() => [];

        protected override TableData HandleIndexableStatement(BinaryExpressionNode root)
        {
            (_, Func<Row, bool> test) = owner.EqualityLeaf(root);
            var literal = (LiteralNode)root.Right;
            if (source is not null || literal.Value is null || TryIndexLookup((ResolvedColumnRefNode)root.Left, literal) is not { } indexed)
            {
                return Scan(test);
            }

            return indexed;
        }

        protected override TableData HandleNonIndexableStatement(BinaryExpressionNode root) => Scan(owner.ComparisonLeaf(root).Test);

        protected override TableData HandleDistanceThresholdExpression(BinaryExpressionNode distanceExpression, string comparisonOperator, object? thresholdLiteral) =>
            owner.DistanceLeaf(distanceExpression, comparisonOperator, thresholdLiteral) is { } leaf ? Scan(leaf.Test) : [];

        protected override TableData HandleTwoColumnExpression(BinaryExpressionNode root) => Scan(owner.TwoColumnLeaf(root).Test);

        protected override TableData HandleConstantExpression(BinaryExpressionNode root) => ConstantLeaf(root) ? Source : [];

        protected override TableData And(TableData leftResult, TableData rightResult)
        {
            TableData result = [];
            foreach (KeyValuePair<RowKey, Record> entry in leftResult)
            {
                if (rightResult.ContainsKey(entry.Key))
                {
                    result[entry.Key] = entry.Value;
                }
            }

            return result;
        }

        protected override TableData Or(TableData leftResult, TableData rightResult)
        {
            TableData result = [];
            foreach (KeyValuePair<RowKey, Record> entry in leftResult)
            {
                result[entry.Key] = entry.Value;
            }

            foreach (KeyValuePair<RowKey, Record> entry in rightResult)
            {
                result.TryAdd(entry.Key, entry.Value);
            }

            return result;
        }

        private TableData Scan(Func<Row, bool> test)
        {
            TableData rows = [];
            foreach (KeyValuePair<RowKey, Record> entry in Source)
            {
                if (test(entry.Value.ToRow()))
                {
                    rows.Add(entry.Key, entry.Value);
                }
            }

            return rows;
        }

        /// <summary>
        /// The rows an index names for <c>column = literal</c>, or <see langword="null"/> when no index covers the
        /// column, a snapshot needs a scan, or the index key cannot be built exactly and the raw lookup finds nothing.
        /// </summary>
        private TableData? TryIndexLookup(ResolvedColumnRefNode column, LiteralNode literal)
        {
            if (MVCC.MvccCoordinator.SnapshotRequiresScan(Runtime.DataVoEngine.Current(), table.DatabaseName!, table.TableName))
            {
                return null;
            }

            string? indexName = table.IndexedColumns!.TryGetValue(column.Column, out string? indexFile)
                ? indexFile
                : table.PrimaryKeys!.Contains(column.Column) ? $"_PK_{table.TableName}" : null;
            if (indexName is null)
            {
                return null;
            }

            bool bound = literal is BoundValueLiteralNode;
            if (SqlIndexLookupKey.TryBuildAuthoritative(table, column.Column, indexName, literal.Value, out string key, isBoundValue: bound))
            {
                return Load(key, indexName);
            }

            TableData raw = Load(UnquotedLiteral(literal), indexName);
            return raw.Count > 0 ? raw : null;
        }

        private TableData Load(string key, string indexName)
        {
            List<RowKey> ids = [.. Runtime.DataVoEngine.Current().IndexManager.FilterUsingIndex(key, indexName, table.TableName, table.DatabaseName!)];
            var internalRows = Runtime.DataVoEngine.Current().StorageContext.SelectFromTable(ids, [], table.TableName, table.DatabaseName!);

            TableData rows = [];
            foreach (var kvp in internalRows)
            {
                rows[kvp.Key] = new Record(kvp.Key, kvp.Value);
            }

            return rows;
        }
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