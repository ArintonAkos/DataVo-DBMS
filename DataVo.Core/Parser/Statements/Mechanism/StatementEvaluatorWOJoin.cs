using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Utils;
using System.Globalization;
using System.Security;

namespace DataVo.Core.Parser.Statements.Mechanism
{
    /// <summary>
    /// Evaluates WHERE clause expressions against a single table when no JOIN is present.
    /// <para>
    /// Unlike <see cref="StatementEvaluator"/>, this evaluator returns a <see cref="HashSet{T}"/>
    /// of row IDs rather than fully materialized joined rows, making it more lightweight for
    /// single-table operations such as <c>DELETE</c> and <c>UPDATE</c>.
    /// </para>
    /// <para>
    /// The evaluator selects the most efficient access path for equality conditions
    /// (secondary index, primary key index, or full scan) and falls back to a full scan
    /// for all other operators.
    /// </para>
    /// </summary>
    /// <remarks>
    /// Inherits from <see cref="ExpressionEvaluatorCore{TResult}"/> with <c>TResult</c> = <see cref="HashSet{T}"/> of <see cref="long"/>.
    /// </remarks>
    internal class StatementEvaluatorWOJoin : ExpressionEvaluatorCore<HashSet<RowKey>>
    {
        private readonly TableDetail _table;

        /// <summary>
        /// Initializes a new instance of the <see cref="StatementEvaluatorWOJoin"/> class
        /// and loads the table metadata (columns, primary keys, indexed columns, and in-memory content)
        /// via <see cref="TableDetail"/>.
        /// </summary>
        /// <param name="databaseName">The name of the database containing the target table.</param>
        /// <param name="tableName">The name of the table to evaluate conditions against.</param>
        public StatementEvaluatorWOJoin(string databaseName, string tableName)
        {
            _table = new TableDetail(tableName, null)
            {
                DatabaseName = databaseName
            };
        }

        /// <summary>
        /// Returns all row IDs when the WHERE clause resolves to an unconditional <c>TRUE</c>.
        /// </summary>
        /// <returns>A <see cref="HashSet{T}"/> containing the IDs of every row in the table.</returns>
        protected override HashSet<RowKey> EvaluateTrueLiteral()
        {
            return [.. _table.TableContent!.Select(row => row.Key)];
        }

        /// <summary>
        /// Returns an empty set when the WHERE clause resolves to an unconditional <c>FALSE</c>.
        /// </summary>
        /// <returns>An empty <see cref="HashSet{T}"/>.</returns>
        protected override HashSet<RowKey> EvaluateFalseLiteral() => [];

        /// <summary>
        /// Handles an equality comparison (<c>column = literal</c>) by selecting the fastest available access path:
        /// <list type="number">
        ///   <item><description>Secondary index lookup — if the column has a B-Tree index.</description></item>
        ///   <item><description>Primary key index lookup — if the column is part of the primary key.</description></item>
        ///   <item><description>Full table scan — as a fallback.</description></item>
        /// </list>
        /// String literal values are trimmed of surrounding single quotes before comparison.
        /// </summary>
        /// <param name="root">A <see cref="BinaryExpressionNode"/> with a column reference on the left and a literal on the right.</param>
        /// <returns>A <see cref="HashSet{T}"/> of row IDs that satisfy the equality condition.</returns>
        protected override HashSet<RowKey> HandleIndexableStatement(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            if (rightLit.Value == null)
            {
                return [];
            }

            string leftValue = leftCol.Column;
            // Index keys are written in the invariant culture; a culture-formatted literal (0,1) would miss them.
            string rightValue = Convert.ToString(rightLit.Value, CultureInfo.InvariantCulture) ?? string.Empty;

            if (rightLit is not BoundValueLiteralNode && rightValue.StartsWith('\'') && rightValue.EndsWith('\''))
            {
                rightValue = rightValue[1..^1];
            }

            if (_table.IndexedColumns!.TryGetValue(leftValue, out string? indexFile))
            {
                if (SqlIndexLookupKey.TryBuildAuthoritative(_table, leftValue, indexFile, rightLit.Value, out string secondaryKey, isBoundValue: rightLit is BoundValueLiteralNode))
                {
                    return EvaluateUsingAuthoritativeIndex(secondaryKey, indexFile, leftValue);
                }

                if (MVCC.MvccCoordinator.SnapshotRequiresScan(Runtime.DataVoEngine.Current(), _table.DatabaseName!, _table.TableName))
                {
                    return EvaluateUsingFullScan(leftValue, rightLit.Value);
                }

                HashSet<RowKey> indexed = EvaluateUsingSecondaryIndex(rightValue, indexFile);
                return indexed.Count > 0
                    ? indexed
                    : EvaluateUsingFullScan(leftValue, rightLit.Value);
            }

            int columnIndex = _table.PrimaryKeys!.IndexOf(leftValue);
            if (columnIndex > -1)
            {
                string primaryKeyIndex = $"_PK_{_table.TableName}";
                if (SqlIndexLookupKey.TryBuildAuthoritative(_table, leftValue, primaryKeyIndex, rightLit.Value, out string primaryKey, isBoundValue: rightLit is BoundValueLiteralNode))
                {
                    return EvaluateUsingAuthoritativeIndex(primaryKey, primaryKeyIndex, leftValue);
                }

                if (MVCC.MvccCoordinator.SnapshotRequiresScan(Runtime.DataVoEngine.Current(), _table.DatabaseName!, _table.TableName))
                {
                    return EvaluateUsingFullScan(leftValue, rightLit.Value);
                }

                HashSet<RowKey> indexed = EvaluateUsingPrimaryKey(rightValue);
                return indexed.Count > 0
                    ? indexed
                    : EvaluateUsingFullScan(leftValue, rightLit.Value);
            }

            return EvaluateUsingFullScan(leftValue, rightLit.Value);
        }

        /// <summary>
        /// Performs an MVCC-aware B-Tree lookup against an authoritative single-column index, combining committed
        /// index entries with visible retained images and the current transaction's buffered writes.
        /// </summary>
        private HashSet<RowKey> EvaluateUsingAuthoritativeIndex(string key, string indexFile, string columnName)
        {
            return [.. MVCC.MvccCoordinator.FilterUsingIndex(Runtime.DataVoEngine.Current(), _table.DatabaseName!, _table.TableName, indexFile, columnName, key)];
        }

        /// <summary>
        /// Performs a B-Tree lookup against a secondary index to retrieve matching row IDs.
        /// </summary>
        /// <param name="rightValue">The value to search for in the index.</param>
        /// <param name="indexFile">The filename of the secondary index B-Tree file.</param>
        /// <returns>A <see cref="HashSet{T}"/> of row IDs that match the index lookup.</returns>
        private HashSet<RowKey> EvaluateUsingSecondaryIndex(string rightValue, string indexFile)
        {
            return [.. Runtime.DataVoEngine.Current().IndexManager.FilterUsingIndex(rightValue, indexFile, _table.TableName, _table.DatabaseName!)];
        }

        /// <summary>
        /// Performs a B-Tree lookup against the table's primary key index (<c>_PK_{TableName}</c>)
        /// to retrieve matching row IDs.
        /// </summary>
        /// <param name="rightValue">The primary key value to search for.</param>
        /// <returns>A <see cref="HashSet{T}"/> of row IDs that match.</returns>
        private HashSet<RowKey> EvaluateUsingPrimaryKey(string rightValue)
        {
            return [.. Runtime.DataVoEngine.Current().IndexManager.FilterUsingIndex(rightValue, $"_PK_{_table.TableName}", _table.TableName, _table.DatabaseName!)];
        }

        /// <summary>
        /// Performs a full table scan, comparing the value of <paramref name="leftValue"/> in each row
        /// to <paramref name="rightVal"/> using equality semantics. Returns the IDs of all matching rows.
        /// </summary>
        /// <param name="leftValue">The column name to evaluate in each row.</param>
        /// <param name="rightVal">The expected value to compare against.</param>
        /// <returns>A <see cref="HashSet{T}"/> of matching row IDs.</returns>
        private HashSet<RowKey> EvaluateUsingFullScan(string leftValue, object? rightVal)
        {
            return [.. _table.TableContent!
                .Where(entry => EvaluateEquality(entry.Value[leftValue], rightVal))
                .Select(entry => entry.Key)];
        }

        /// <summary>
        /// Handles a non-equality comparison between a column and a literal (e.g., <c>&gt;</c>, <c>&lt;</c>, <c>!=</c>, <c>IS NULL</c>).
        /// Performs a full table scan since these operators cannot leverage B-Tree index lookups.
        /// </summary>
        /// <param name="root">A <see cref="BinaryExpressionNode"/> with a column reference on the left and a literal on the right.</param>
        /// <returns>A <see cref="HashSet{T}"/> of row IDs that satisfy the condition.</returns>
        protected override HashSet<RowKey> HandleNonIndexableStatement(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            string leftValue = leftCol.Column;
            var rightVal = rightLit.Value;

            Func<KeyValuePair<RowKey, Record>, bool> pred = DeterminePredicate(root.Operator, leftValue, rightVal);

            return [.. _table.TableContent!
                .Where(pred)
                .Select(entry => entry.Key)];
        }

        /// <summary>
        /// Handles predicates of the form <c>distance(vector_column, literal_vector) &lt;op&gt; threshold</c>.
        /// </summary>
        protected override HashSet<RowKey> HandleDistanceThresholdExpression(BinaryExpressionNode distanceExpression, string comparisonOperator, object? thresholdLiteral)
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

            return [.. _table.TableContent!
                .Where(entry =>
                {
                    if (!VectorParser.TryCoerceToVector(entry.Value[vectorColumn.Column], out float[] rowVector))
                    {
                        return false;
                    }

                    if (rowVector.Length != queryVector.Length)
                    {
                        return false;
                    }

                    double distance = distanceExpression.Operator == Operators.VECTOR_DISTANCE_COSINE
                        ? SimdDistanceKernels.CosineDistanceDouble(queryVector, rowVector)
                        : SimdDistanceKernels.EuclideanDistanceDouble(queryVector, rowVector);

                    return EvaluateDistanceComparison(distance, threshold, comparisonOperator);
                })
                .Select(entry => entry.Key)];
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
        private Func<KeyValuePair<RowKey, Record>, bool> DeterminePredicate(string op, string leftValue, object? rightVal)
        {
            return op switch
            {
                Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], rightVal),
                Operators.NOT_EQUALS => entry => ExpressionValueComparer.SqlNotEquals(entry.Value[leftValue], rightVal, trimQuotedStrings: true),
                Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) < 0,
                Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) > 0,
                Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) <= 0,
                Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) >= 0,
                Operators.LIKE => entry => ExpressionValueComparer.MatchesLike(entry.Value[leftValue], rightVal, trimQuotedStrings: true),
                Operators.NOT_LIKE => entry => ExpressionValueComparer.MatchesNotLike(entry.Value[leftValue], rightVal, trimQuotedStrings: true),
                Operators.IS_NULL => entry => entry.Value[leftValue] == null,
                Operators.IS_NOT_NULL => entry => entry.Value[leftValue] != null,
                _ => throw new SecurityException("Invalid operator")
            };
        }

        /// <summary>
        /// Handles a comparison between two column references within the same table (e.g., <c>col_a &gt; col_b</c>).
        /// Performs a full table scan, evaluating each row with the appropriate predicate.
        /// </summary>
        /// <param name="root">A <see cref="BinaryExpressionNode"/> whose left and right operands are both <see cref="ResolvedColumnRefNode"/> instances.</param>
        /// <returns>A <see cref="HashSet{T}"/> of row IDs where the condition holds.</returns>
        protected override HashSet<RowKey> HandleTwoColumnExpression(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightCol = (ResolvedColumnRefNode)root.Right;

            string leftValue = leftCol.Column;
            string rightValue = rightCol.Column;

            Func<KeyValuePair<RowKey, Record>, bool> pred = DetermineTwoColumnPredicate(root.Operator, leftValue, rightValue);

            return [.. _table.TableContent!
                .Where(pred)
                .Select(entry => entry.Key)];
        }

        /// <summary>
        /// Builds a row-filtering predicate that compares two column values within the same row.
        /// </summary>
        /// <param name="op">The relational operator (e.g., <c>=</c>, <c>!=</c>, <c>&lt;</c>, <c>&gt;</c>).</param>
        /// <param name="leftValue">The name of the first column.</param>
        /// <param name="rightValue">The name of the second column.</param>
        /// <returns>A predicate function that returns <c>true</c> for rows where the two column values satisfy the operator.</returns>
        /// <exception cref="SecurityException">Thrown when <paramref name="op"/> is not a recognized operator.</exception>
        private Func<KeyValuePair<RowKey, Record>, bool> DetermineTwoColumnPredicate(string op, string leftValue, string rightValue)
        {
            return op switch
            {
                Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
                Operators.NOT_EQUALS => entry => ExpressionValueComparer.SqlNotEquals(entry.Value[leftValue], entry.Value[rightValue], trimQuotedStrings: true),
                Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) < 0,
                Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) > 0,
                Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) <= 0,
                Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) >= 0,
                Operators.LIKE => entry => ExpressionValueComparer.MatchesLike(entry.Value[leftValue], entry.Value[rightValue], trimQuotedStrings: true),
                Operators.NOT_LIKE => entry => ExpressionValueComparer.MatchesNotLike(entry.Value[leftValue], entry.Value[rightValue], trimQuotedStrings: true),
                _ => throw new SecurityException("Invalid operator")
            };
        }

        /// <summary>
        /// Evaluates a comparison between two constant literal values (e.g., <c>5 &gt; 3</c>).
        /// If the condition is true, returns all row IDs from the table; otherwise, returns an empty set.
        /// </summary>
        /// <param name="root">A <see cref="BinaryExpressionNode"/> whose left and right operands are both <see cref="LiteralNode"/> instances.</param>
        /// <returns>All row IDs if the condition is true; an empty <see cref="HashSet{T}"/> otherwise.</returns>
        protected override HashSet<RowKey> HandleConstantExpression(BinaryExpressionNode root)
        {
            var leftLit = (LiteralNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            object? leftVal = leftLit.Value;
            object? rightVal = rightLit.Value;

            bool isCondTrue = DetermineConstantCondition(root.Operator, leftVal, rightVal);

            return isCondTrue
                ? [.. _table.TableContent!.Select(row => row.Key)]
                : [];
        }

        /// <summary>
        /// Combines two result sets using logical AND by intersecting them.
        /// Only row IDs present in both sets are retained.
        /// </summary>
        /// <param name="leftResult">Row IDs from the left sub-expression.</param>
        /// <param name="rightResult">Row IDs from the right sub-expression.</param>
        /// <returns>A <see cref="HashSet{T}"/> containing only row IDs present in both inputs.</returns>
        protected override HashSet<RowKey> And(HashSet<RowKey> leftResult, HashSet<RowKey> rightResult)
        {
            return [.. leftResult.Intersect(rightResult)];
        }

        /// <summary>
        /// Combines two result sets using logical OR by computing their union.
        /// All row IDs from either set are included.
        /// </summary>
        /// <param name="leftResult">Row IDs from the left sub-expression.</param>
        /// <param name="rightResult">Row IDs from the right sub-expression.</param>
        /// <returns>A <see cref="HashSet{T}"/> containing all row IDs from both inputs.</returns>
        protected override HashSet<RowKey> Or(HashSet<RowKey> leftResult, HashSet<RowKey> rightResult)
        {
            return [.. leftResult.Union(rightResult)];
        }
    }
}
