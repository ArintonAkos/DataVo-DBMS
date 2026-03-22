using DataVo.Core.Parser.AST;
using DataVo.Core.Services;
using DataVo.Core.Enums;
using DataVo.Core.Constants;

namespace DataVo.Core.Parser.Statements;

/// <summary>
/// Extracts WHERE predicates that can be safely evaluated on a single table early in query execution.
/// </summary>
/// <remarks>
/// Used by the query planner to determine which WHERE filters can be applied to seed rows
/// after HNSW fetch but before join evaluation.
///
/// Conservative approach: Only extracts predicates that clearly reference a single table,
/// avoiding false positives that could cause incorrect filtering.
/// Supports:
/// - AND chains (partial extraction allowed)
/// - OR chains (only when BOTH branches are table-specific)
/// </remarks>
internal static class ExpressionExtractor
{
    /// <summary>
    /// Attempts to extract WHERE predicates that reference only the specified table.
    /// </summary>
    /// <param name="expression">The WHERE expression tree to analyze.</param>
    /// <param name="tableName">The table that predicates must reference exclusively.</param>
    /// <param name="tableService">Service for resolving column-to-table mappings.</param>
    /// <returns>
    /// An expression containing only TABLE-specific predicates.
    /// Returns null if:
    /// - No safe predicates can be extracted
    /// - An OR branch references non-target-table columns
    /// - The expression references columns from other tables
    /// </returns>
    public static ExpressionNode? TryExtractTableSpecificPredicates(
        ExpressionNode? expression,
        string tableName,
        TableService tableService)
    {
        if (expression == null)
        {
            return null;
        }

        // Recursively extract safe predicates
        var extracted = TryExtractInternal(expression, tableName, tableService);
        return extracted;
    }

    private static ExpressionNode? TryExtractInternal(
        ExpressionNode expression,
        string tableName,
        TableService tableService)
    {
        if (expression is not BinaryExpressionNode binary)
        {
            return null;
        }

        if (binary.Operator.Equals(Operators.AND, StringComparison.OrdinalIgnoreCase))
        {
            var left = TryExtractInternal(binary.Left, tableName, tableService);
            var right = TryExtractInternal(binary.Right, tableName, tableService);

            if (left is not null && right is not null)
            {
                return new BinaryExpressionNode
                {
                    Operator = Operators.AND,
                    Left = left,
                    Right = right
                };
            }

            return left ?? right;
        }

        if (binary.Operator.Equals(Operators.OR, StringComparison.OrdinalIgnoreCase))
        {
            var left = TryExtractInternal(binary.Left, tableName, tableService);
            var right = TryExtractInternal(binary.Right, tableName, tableService);

            if (left is not null && right is not null)
            {
                return new BinaryExpressionNode
                {
                    Operator = Operators.OR,
                    Left = left,
                    Right = right
                };
            }

            return null;
        }

        if (!IsComparisonOperator(binary.Operator))
        {
            return null;
        }

        if (ReferencesOnlyTable(binary.Left, tableName, tableService)
            && ReferencesOnlyTable(binary.Right, tableName, tableService))
        {
            return binary;
        }

        return null;
    }

    /// <summary>
    /// Checks if an expression tree references columns from only the specified table.
    /// </summary>
    /// <remarks>
    /// Literals, nulls, and other non-column nodes are considered "table agnostic"
    /// and pass through (they don't reference other tables).
    /// </remarks>
    private static bool ReferencesOnlyTable(ExpressionNode expression, string tableName, TableService tableService)
    {
        // Literals and null references are OK (don't reference any table)
        if (expression is LiteralNode or NullLiteralNode)
        {
            return true;
        }

        // Resolved column reference
        if (expression is ResolvedColumnRefNode resolved)
        {
            return resolved.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase);
        }

        // Unresolved column reference - need to resolve it
        if (expression is ColumnRefNode colRef)
        {
            // If explicitly qualified, check directly
            if (!string.IsNullOrWhiteSpace(colRef.TableOrAlias))
            {
                return colRef.TableOrAlias.Equals(tableName, StringComparison.OrdinalIgnoreCase);
            }

            // Otherwise, resolve the column through the table service
            try
            {
                (string resolvedTable, _) = tableService.ParseAndFindTableNameByColumn(colRef.Column);
                return resolvedTable.Equals(tableName, StringComparison.OrdinalIgnoreCase);
            }
            catch
            {
                // If resolution fails, assume the column belongs to another table (conservative)
                return false;
            }
        }

        // Binary expressions: check both sides
        if (expression is BinaryExpressionNode binaryExpr)
        {
            return ReferencesOnlyTable(binaryExpr.Left, tableName, tableService)
                && ReferencesOnlyTable(binaryExpr.Right, tableName, tableService);
        }

        // Scalar functions: check all arguments
        if (expression is ScalarFunctionExpressionNode scalarFunc)
        {
            return scalarFunc.Arguments.All(arg => ReferencesOnlyTable(arg, tableName, tableService));
        }

        // Other expression types (window functions, aggregates) are not table-specific
        return false;
    }

    /// <summary>
    /// Checks if an operator is a comparison operator that can be pushed down.
    /// </summary>
    private static bool IsComparisonOperator(string op)
    {
        return op switch
        {
            // Logical comparisons
            Operators.EQUALS => true,
            Operators.NOT_EQUALS => true,
            Operators.GREATER_THAN => true,
            Operators.LESS_THAN => true,
            Operators.GREATER_THAN_OR_EQUAL_TO => true,
            Operators.LESS_THAN_OR_EQUAL_TO => true,
            // Pattern matching
            Operators.LIKE => true,
            // Null checks
            Operators.IS_NULL => true,
            Operators.IS_NOT_NULL => true,
            // Arithmetic (for expressions like column + value > b)
            Operators.ADD => true,
            Operators.SUBTRACT => true,
            Operators.MUL => true,
            Operators.DIVIDE => true,
            // Vector distances (though typically used in ORDER BY, not WHERE)
            Operators.VECTOR_DISTANCE_L2 => true,
            Operators.VECTOR_DISTANCE_COSINE => true,
            _ => false
        };
    }
}
