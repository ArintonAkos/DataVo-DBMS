using DataVo.Core.Exceptions;
using DataVo.Core.Enums;
using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.Binding;

internal static class SelectBinder
{
    public static JoinModel BindJoins(SelectStatement ast, TableService tableService)
    {
        var model = new JoinModel();

        foreach (var join in ast.Joins)
        {
            ValidateJoinShape(join);

            var tableName = join.TableName.Name;
            var alias = join.Alias?.Name;

            var tableDetail = new TableDetail(tableName, alias);

            try
            {
                tableService.AddTableDetail(tableDetail);
            }
            catch (Exception ex)
            {
                throw new BindingException($"Binding Error: cannot add joined table '{tableName}'{(alias != null ? $" (alias '{alias}')" : "")}. {ex.Message}");
            }

            model.JoinTableDetails[tableDetail.GetTableNameInUse()] = tableDetail;

            if (join.Condition == null)
            {
                if (join.JoinType.Equals(JoinTypes.CROSS, StringComparison.OrdinalIgnoreCase))
                {
                    string leftTableName = ast.FromAlias?.Name ?? ast.FromTable?.Name ?? string.Empty;

                    model.JoinConditions.Add(new JoinModel.JoinCondition(
                        leftTableName,
                        "__dummy__",
                        tableDetail.GetTableNameInUse(),
                        "__dummy__",
                        join.JoinType
                    ));
                }
                continue;
            }

            var left = ResolveColumnRef(join.Condition.Left, tableService);
            var right = ResolveColumnRef(join.Condition.Right, tableService);

            model.JoinConditions.Add(new JoinModel.JoinCondition(
                left.TableName,
                left.Column,
                right.TableName,
                right.Column,
                join.JoinType
            ));
        }

        return model;
    }

    private static void ValidateJoinShape(JoinDetailNode join)
    {
        if (!JoinTypes.All.Contains(join.JoinType))
        {
            throw new BindingException($"Binding Error: unsupported JOIN type '{join.JoinType}'.");
        }

        if (join.JoinType.Equals(JoinTypes.CROSS, StringComparison.OrdinalIgnoreCase))
        {
            if (join.Condition != null)
            {
                throw new BindingException("Binding Error: CROSS JOIN must not have an ON condition.");
            }

            return;
        }

        if (join.Condition == null)
        {
            throw new BindingException($"Binding Error: {join.JoinType} JOIN requires an ON condition.");
        }
    }

    private static ResolvedColumnRefNode ResolveColumnRef(ColumnRefNode reference, TableService tableService)
    {
        if (string.IsNullOrWhiteSpace(reference.Column))
        {
            throw new BindingException("Binding Error: empty column reference in JOIN ON.");
        }

        if (!string.IsNullOrWhiteSpace(reference.TableOrAlias))
        {
            TableDetail table;

            try
            {
                table = tableService.GetTableDetailByAliasOrName(reference.TableOrAlias);
            }
            catch
            {
                throw new BindingException($"Binding Error: table or alias '{reference.TableOrAlias}' not found.");
            }

            if (table.Columns == null || !table.Columns.Contains(reference.Column))
            {
                throw new BindingException($"Binding Error: column '{reference.Column}' not found in table '{table.TableName}'.");
            }

            return new ResolvedColumnRefNode
            {
                TableName = table.TableName,
                Column = reference.Column
            };
        }

        try
        {
            var resolved = tableService.ParseAndFindTableNameByColumn(reference.Column);

            return new ResolvedColumnRefNode
            {
                TableName = resolved.Item1,
                Column = resolved.Item2
            };
        }
        catch (Exception ex)
        {
            throw new BindingException($"Binding Error: cannot resolve column '{reference.Column}'. {ex.Message}");
        }
    }

    private static readonly HashSet<string> ComparisonOperators =
        new(StringComparer.OrdinalIgnoreCase) { "=", "==", "EQ", "!=", "<>", "NEQ", "<", ">", "<=", ">=" };

    /// <summary>
    /// <c>WHERE Id = 'abc'</c> on a numeric column can never match; report it instead of silently
    /// returning nothing. Numeric text such as <c>'150'</c> is rewritten to the number, so ordering
    /// comparisons (<c>Code &gt;= '150'</c>) compare numerically rather than as text.
    /// </summary>
    private static void RejectTextComparedWithNumericColumn(BinaryExpressionNode binary, TableService tableService)
    {
        if (!ComparisonOperators.Contains(binary.Operator))
        {
            return;
        }

        (ResolvedColumnRefNode? column, LiteralNode? literal) = (binary.Left, binary.Right) switch
        {
            (ResolvedColumnRefNode c, LiteralNode l) => (c, l),
            (LiteralNode l, ResolvedColumnRefNode c) => (c, l),
            _ => (null, null),
        };

        if (column is null || literal?.Value is not string text)
        {
            return;
        }

        string value;
        if (literal is BoundValueLiteralNode)
        {
            value = text;
        }
        else if (text.Length >= 2 && text[0] == '\'' && text[^1] == '\'')
        {
            value = text[1..^1];
        }
        else
        {
            return;
        }

        string? type = tableService.TryGetColumnType(column.TableName, column.Column);
        if (type is not ("INT" or "FLOAT"))
        {
            return;
        }

        if (!double.TryParse(value.Trim(), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out _))
        {
            throw new BindingException($"Cannot compare {type} column {column.Column} with the text value '{value}'.")
            {
                ErrorCode = Contracts.Results.DataVoErrorCodes.TypeMismatch,
            };
        }

        literal.Value = value.Trim(); // same shape as a number literal
    }

    public static ExpressionNode? BindWhere(ExpressionNode? node, TableService tableService)
    {
        if (node == null) return null;

        if (node is ParameterNode parameter)
        {
            throw new BindingException($"Bind Error: Parameter '@{parameter.Name}' was not provided.");
        }

        if (node is BinaryExpressionNode binary)
        {
            binary.Left = BindWhere(binary.Left, tableService)!;
            binary.Right = BindWhere(binary.Right, tableService)!;
            RejectTextComparedWithNumericColumn(binary, tableService);
            return binary;
        }

        if (node is ScalarFunctionExpressionNode scalarFunction)
        {
            for (int index = 0; index < scalarFunction.Arguments.Count; index++)
            {
                scalarFunction.Arguments[index] = BindWhere(scalarFunction.Arguments[index], tableService)!;
            }

            return scalarFunction;
        }

        if (node is ColumnRefNode columnRef)
        {
            if (string.IsNullOrWhiteSpace(columnRef.Column))
            {
                throw new BindingException("Binding Error: empty column reference in WHERE clause.");
            }

            try
            {
                string rawColumn = columnRef.Column;
                if (!string.IsNullOrWhiteSpace(columnRef.TableOrAlias))
                {
                    rawColumn = $"{columnRef.TableOrAlias}.{rawColumn}";
                }

                var resolved = tableService.ParseAndFindTableNameByColumn(rawColumn);
                return new ResolvedColumnRefNode
                {
                    TableName = resolved.Item1,
                    Column = resolved.Item2
                };
            }
            catch (Exception ex)
            {
                var refName = string.IsNullOrWhiteSpace(columnRef.TableOrAlias)
                    ? columnRef.Column
                    : $"{columnRef.TableOrAlias}.{columnRef.Column}";

                throw new BindingException($"Binding Error: cannot resolve column '{refName}'. {ex.Message}");
            }
        }

        // Return LiteralNode as is
        return node;
    }
}