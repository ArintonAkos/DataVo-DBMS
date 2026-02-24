using DataVo.Core.Exceptions;
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
                continue;
            }

            var left = ResolveColumnRef(join.Condition.Left, tableService);
            var right = ResolveColumnRef(join.Condition.Right, tableService);

            model.JoinConditions.Add(new JoinModel.JoinCondition(
                left.TableName,
                left.Column,
                right.TableName,
                right.Column
            ));
        }

        return model;
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
}