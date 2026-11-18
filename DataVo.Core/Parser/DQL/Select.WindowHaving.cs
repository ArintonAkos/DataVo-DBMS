using DataVo.Core.Constants;
using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Execution.Volcano;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Utils;
using System.Globalization;

namespace DataVo.Core.Parser.DQL;

internal partial class Select
{
    private void ComputeWindowFunctionValues(ListedTable rows)
    {
        _windowValues.Clear();
        List<SelectColumnNode> windowColumns = _model.GetWindowFunctionColumns();
        if (windowColumns.Count == 0 || rows.Count == 0)
        {
            return;
        }

        Dictionary<JoinedRow, TypedExecutionRow> typedRows = BuildTypedWindowRows(rows);

        foreach (var col in windowColumns)
        {
            if (col.Expression is not WindowFunctionExpressionNode windowExpr)
            {
                continue;
            }

            if (!windowExpr.FunctionName.Equals("RANK", StringComparison.OrdinalIgnoreCase))
            {
                throw new EvaluationException($"Unsupported window function: {windowExpr.FunctionName}");
            }

            string outputName = col.Alias ?? col.RawExpression;

            var partitions = rows
                .GroupBy(row => BuildPartitionSignature(typedRows[row], windowExpr.PartitionByColumns))
                .ToList();

            foreach (var partition in partitions)
            {
                List<JoinedRow> ordered = windowExpr.IsOrderAscending
                    ? [.. partition.OrderBy(r => ResolveWindowOrderValue(typedRows[r], windowExpr.OrderByColumn), DynamicObjectComparer.Instance)]
                    : [.. partition.OrderByDescending(r => ResolveWindowOrderValue(typedRows[r], windowExpr.OrderByColumn), DynamicObjectComparer.Instance)];

                object? previousOrderValue = null;
                long currentRank = 1;

                for (int i = 0; i < ordered.Count; i++)
                {
                    JoinedRow row = ordered[i];
                    object? currentOrderValue = ResolveWindowOrderValue(typedRows[row], windowExpr.OrderByColumn);

                    if (i == 0)
                    {
                        currentRank = 1;
                    }
                    else if (DynamicObjectComparer.Instance.Compare(previousOrderValue, currentOrderValue) != 0)
                    {
                        currentRank = i + 1;
                    }

                    if (!_windowValues.TryGetValue(row, out Dictionary<string, object?>? rowValues))
                    {
                        rowValues = [];
                        _windowValues[row] = rowValues;
                    }

                    rowValues[outputName] = currentRank;
                    previousOrderValue = currentOrderValue;
                }
            }
        }
    }

    private static Dictionary<JoinedRow, TypedExecutionRow> BuildTypedWindowRows(ListedTable rows)
    {
        var typed = new Dictionary<JoinedRow, TypedExecutionRow>();
        long rowId = 1; // synthetic ids for window rows

        foreach (JoinedRow row in rows)
        {
            var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            bool singleTable = row.Keys.Count() == 1;

            foreach (string tableName in row.Keys)
            {
                Row tableRow = row[tableName];
                foreach (string column in tableRow.Keys)
                {
                    object? value = tableRow[column];
                    values[$"{tableName}.{column}"] = value;
                    if (singleTable && !values.ContainsKey(column))
                    {
                        values[column] = value;
                    }
                }
            }

            typed[row] = new TypedExecutionRow(rowId++, values);
        }

        return typed;
    }

    private string BuildPartitionSignature(TypedExecutionRow row, List<ColumnRefNode> partitionColumns)
    {
        if (partitionColumns.Count == 0)
        {
            return "__ALL__";
        }

        IEnumerable<string> parts = partitionColumns
            .Select(col => ResolveWindowOrderValue(row, col))
            .Select(BuildWindowValueSignature);

        return string.Join("|", parts);
    }

    private static string BuildWindowValueSignature(object? value)
    {
        if (value == null)
        {
            return "NULL";
        }

        return value switch
        {
            string s => $"System.String:{s}",
            char c => $"System.Char:{c}",
            bool b => $"System.Boolean:{b}",
            DataVoDate d => $"DataVo.Core.DataVoDate:{d:O}",
            DateTime dt => $"System.DateTime:{dt:O}",
            IFormattable formattable => $"{value.GetType().FullName}:{formattable.ToString(null, CultureInfo.InvariantCulture)}",
            _ => $"{value.GetType().FullName}:{value}"
        };
    }

    private object? ResolveWindowOrderValue(TypedExecutionRow row, ColumnRefNode column)
    {
        string reference = string.IsNullOrWhiteSpace(column.TableOrAlias)
            ? column.Column
            : $"{column.TableOrAlias}.{column.Column}";

        return ResolveTypedColumnValue(row, reference);
    }

    private object? ResolveTypedColumnValue(TypedExecutionRow row, string columnReference)
    {
        string[] referenceParts = columnReference.Split('.');

        if (referenceParts.Length == 1)
        {
            if (row.Values.TryGetValue(columnReference, out object? directValue))
            {
                return directValue;
            }

            List<string> matchedKeys = [.. row.Values.Keys.Where(k =>
                k.Equals(columnReference, StringComparison.OrdinalIgnoreCase)
                || k.EndsWith($".{columnReference}", StringComparison.OrdinalIgnoreCase))];

            if (matchedKeys.Count == 0)
            {
                throw new BindingException($"Column '{columnReference}' not found.");
            }

            if (matchedKeys.Count > 1)
            {
                throw new BindingException($"Column '{columnReference}' is ambiguous.");
            }

            return row.Values[matchedKeys[0]];
        }

        string tableOrAlias = referenceParts[0];
        string colName = referenceParts[1];
        string resolvedTableName = NormalizeTableIdentifier(tableOrAlias);

        string normalizedKey = $"{resolvedTableName}.{colName}";
        if (row.Values.TryGetValue(normalizedKey, out object? normalizedValue))
        {
            return normalizedValue;
        }

        string aliasKey = $"{tableOrAlias}.{colName}";
        if (row.Values.TryGetValue(aliasKey, out object? aliasValue))
        {
            return aliasValue;
        }

        throw new BindingException($"Column '{columnReference}' not found in typed window row.");
    }

    private object? ResolveWindowValue(JoinedRow row, string outputField)
    {
        if (_windowValues.TryGetValue(row, out Dictionary<string, object?>? values)
            && values.TryGetValue(outputField, out object? value))
        {
            return value;
        }

        return null;
    }

    private bool EvaluatePredicate(ExpressionNode node, JoinedRow row)
    {
        if (node is LiteralNode literalNode)
        {
            return EvaluateLiteralNode(literalNode);
        }

        if (node is not BinaryExpressionNode binNode)
        {
            throw new EvaluationException($"Unsupported HAVING predicate node type: {node.GetType().Name}");
        }

        return EvaluateBinaryNode(binNode, row);
    }

    private bool EvaluateLiteralNode(LiteralNode literalNode)
    {
        if (literalNode.Value is bool b)
        {
            return b;
        }

        if (literalNode.Value is string s && s == SqlLiterals.TrueExpression)
        {
            return true;
        }

        return false;
    }

    private bool EvaluateBinaryNode(BinaryExpressionNode binNode, JoinedRow row)
    {
        if (binNode.Operator == Operators.AND)
        {
            return EvaluatePredicate(binNode.Left, row) && EvaluatePredicate(binNode.Right, row);
        }

        if (binNode.Operator == Operators.OR)
        {
            return EvaluatePredicate(binNode.Left, row) || EvaluatePredicate(binNode.Right, row);
        }

        return EvaluateComparisonOperator(binNode, row);
    }

    private bool EvaluateComparisonOperator(BinaryExpressionNode binNode, JoinedRow row)
    {
        object? leftValue = ResolveNodeValue(binNode.Left, row);
        object? rightValue = ResolveNodeValue(binNode.Right, row);
        string op = binNode.Operator;

        return op switch
        {
            Operators.EQUALS => EvaluateEquality(leftValue, rightValue),
            Operators.NOT_EQUALS => ExpressionValueComparer.SqlNotEquals(leftValue, rightValue, trimQuotedStrings: true, useNumericTolerance: true),
            Operators.LESS_THAN => CompareDynamics(leftValue, rightValue) < 0,
            Operators.GREATER_THAN => CompareDynamics(leftValue, rightValue) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => CompareDynamics(leftValue, rightValue) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => CompareDynamics(leftValue, rightValue) >= 0,
            Operators.LIKE => ExpressionValueComparer.MatchesLike(leftValue, rightValue, trimQuotedStrings: true),
            Operators.NOT_LIKE => ExpressionValueComparer.MatchesNotLike(leftValue, rightValue, trimQuotedStrings: true),
            Operators.IS_NULL => leftValue == null,
            Operators.IS_NOT_NULL => leftValue != null,
            _ => throw new EvaluationException($"Unsupported HAVING operator: {op}")
        };
    }

    private static bool EvaluateEquality(object? val1, object? val2)
    {
        if (val1 == null || val2 == null)
        {
            return false;
        }

        return ExpressionValueComparer.AreEqual(val1, val2, trimQuotedStrings: true, useNumericTolerance: true);
    }

    private static int? CompareDynamics(object? leftVal, object? rightVal)
    {
        if (leftVal == null || rightVal == null)
        {
            return null;
        }

        return ExpressionValueComparer.Compare(leftVal, rightVal, trimQuotedStrings: true);
    }

    private object? ResolveNodeValue(ExpressionNode node, JoinedRow row)
    {
        // HAVING and ORDER BY evaluate this once per group: the resolvers are made once per statement.
        _resolveColumnValue ??= (colRef, r) =>
        {
            string reference = string.IsNullOrEmpty(colRef.TableOrAlias) ? colRef.Column : $"{colRef.TableOrAlias}.{colRef.Column}";
            return ResolveColumnValue(r, reference);
        };
        _resolveAggregateValue ??= ResolveAggregateValue;
        return ExpressionEvaluator.Evaluate(node, row, _resolveColumnValue, _resolveAggregateValue);
    }

    private Func<ColumnRefNode, JoinedRow, object?>? _resolveColumnValue;
    private Func<AggregateExpressionNode, JoinedRow, object?>? _resolveAggregateValue;
    private Dictionary<AggregateExpressionNode, string>? _aggregateHeaders;

    /// <summary>The value of an aggregate in a grouped row, found under its canonical header.</summary>
    private object? ResolveAggregateValue(AggregateExpressionNode aggNode, JoinedRow r)
    {
        if (!r.ContainsKey(GroupBy.HASH_VALUE))
        {
            throw new EvaluationException("Aggregate expression used outside grouped/aggregated context.");
        }

        Row aggMap = r[GroupBy.HASH_VALUE];

        // The header depends only on the node, so it is built once per node (AST nodes compare by reference).
        _aggregateHeaders ??= [];
        if (!_aggregateHeaders.TryGetValue(aggNode, out string? canonicalKey))
        {
            canonicalKey = AggregateExpressionFormatter.BuildHeader(aggNode);
            _aggregateHeaders[aggNode] = canonicalKey;
        }

        if (aggMap.ContainsKey(canonicalKey))
        {
            return aggMap[canonicalKey];
        }

        string resolvedKey = ResolveAggregateKey(aggNode, r);
        return aggMap[resolvedKey];
    }

    /// <summary>
    /// Finds the grouped row's entry for an aggregate whose canonical header is not present. Only an entry that is
    /// certainly the same aggregate qualifies (the star form, or the same function over the same column, written
    /// with a qualifier): reading another aggregate of the same function would silently give a wrong value.
    /// </summary>
    private string ResolveAggregateKey(AggregateExpressionNode aggNode, JoinedRow row)
    {
        Row aggMap = row[GroupBy.HASH_VALUE];
        string funcName = aggNode.FunctionName.ToUpperInvariant();
        string? argument = aggNode.IsStar
            ? "*"
            : aggNode.Argument switch
            {
                ColumnRefNode column => column.Column,
                ResolvedColumnRefNode resolved => resolved.Column,
                _ => null,
            };

        if (argument is not null)
        {
            string distinct = aggNode.IsDistinct ? "DISTINCT " : string.Empty;
            foreach (string key in aggMap.Keys)
            {
                if (key.StartsWith(funcName + "(" + distinct, StringComparison.OrdinalIgnoreCase)
                    && key.EndsWith(")", StringComparison.Ordinal)
                    && (key.EndsWith("(" + distinct + argument + ")", StringComparison.OrdinalIgnoreCase)
                        || key.EndsWith("." + argument + ")", StringComparison.OrdinalIgnoreCase)))
                {
                    return key;
                }
            }
        }

        throw new EvaluationException($"Aggregate result {AggregateExpressionFormatter.BuildHeader(aggNode)} not found in grouped row.");
    }

    private object? ResolveColumnValue(JoinedRow row, string columnReference)
    {
        string[] referenceParts = columnReference.Split('.');

        if (referenceParts.Length == 1)
        {
            List<string> matchedTables = [.. row.Keys.Where(t => row[t].ContainsKey(columnReference))];

            if (matchedTables.Count == 0)
            {
                throw new BindingException($"Column '{columnReference}' not found.");
            }

            if (matchedTables.Count > 1)
            {
                throw new BindingException($"Column '{columnReference}' is ambiguous.");
            }

            return row[matchedTables[0]][columnReference];
        }

        string tableOrAlias = referenceParts[0];
        string colName = referenceParts[1];
        string resolvedTableName = _model.TableService!.GetTableDetailByAliasOrName(tableOrAlias).TableName;

        if (row.ContainsKey(resolvedTableName) && row[resolvedTableName].ContainsKey(colName))
        {
            return row[resolvedTableName][colName];
        }

        throw new BindingException($"Column '{columnReference}' not found in the currently resolved JOIN results.");
    }
}
