using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Binding;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Services;

namespace DataVo.Core.Models.DQL;

internal class SelectModel
{
    public Dictionary<string, List<string>>? TableColumnsInUse { get; set; }
    public string? Database { get; set; }
    public TableService? TableService { get; set; }
    public Where WhereStatement { get; set; } = null!;
    public Join JoinStatement { get; set; } = null!;
    public GroupBy GroupByStatement { get; set; } = null!;
    public Aggregate AggregateStatement { get; set; } = null!;
    public TableDetail FromTable { get; set; } = null!;

    private SelectStatement Ast { get; set; } = null!;

    public static SelectModel FromAst(SelectStatement ast)
    {
        string rawTableName = ast.FromTable?.Name ?? string.Empty;
        var tableNameWithAlias = TableParserService.ParseTableWithAlias(rawTableName);
        string tableName = tableNameWithAlias.Item1;
        string? tableAlias = tableNameWithAlias.Item2;
        TableDetail fromTable = new(tableName, tableAlias);

        Where whereStatement = new Where(string.Empty, fromTable);
        if (ast.WhereExpression != null)
        {
            whereStatement = new Where(ast.WhereExpression, fromTable);
        }
        else
        {
            whereStatement = new Where(string.Empty, fromTable);
        }

        return new SelectModel
        {
            WhereStatement = whereStatement,
            FromTable = fromTable,
            Ast = ast
        };
    }

    public List<string> GetSelectedColumns()
    {
        if (TableColumnsInUse is null)
        {
            return GetAllColumns();
        }

        return TableColumnsInUse.SelectMany(c => c.Value).ToList();
    }

    public Node? GetHavingExpression() => Ast.HavingExpression;

    public OrderByNode? GetOrderByExpression() => Ast.OrderByExpression;

    private List<string> GetAllColumns()
    {
        List<string> columns = [];

        foreach (var table in TableService!.TableDetails)
        {
            columns.AddRange(table.Value.Columns!.Select(c => $"{table.Value.TableName}.{c}"));
        }

        return columns;
    }

    public bool Validate(string databaseName)
    {
        Database = databaseName;
        TableService = new TableService(databaseName);
        TableService.AddTableDetail(FromTable);

        var boundJoinModel = SelectBinder.BindJoins(Ast, TableService);
        JoinStatement = new Join(boundJoinModel, TableService);
        GroupByStatement = new GroupBy(Ast.GroupByExpression, databaseName, TableService);
        AggregateStatement = new Aggregate(Ast.Columns, databaseName, TableService);

        TableColumnsInUse = ParseSelectColumnsFromAst(Ast.Columns, TableService);

        return false;
    }

    private static Dictionary<string, List<string>>? ParseSelectColumnsFromAst(List<SqlNode> columns, TableService tableService)
    {
        Dictionary<string, List<string>> selectedColumns = [];

        foreach (var node in columns)
        {
            if (node is not IdentifierNode identifierNode)
            {
                continue;
            }

            string name = identifierNode.Name.Trim();

            if (name == "*")
            {
                return null;
            }

            if (name.EndsWith(".*", StringComparison.Ordinal))
            {
                string tableOrAlias = name[..^2];
                var tableDetail = tableService.GetTableDetailByAliasOrName(tableOrAlias);
                string tableName = tableDetail.TableName;

                if (!selectedColumns.ContainsKey(tableName))
                {
                    selectedColumns[tableName] = [];
                }

                foreach (var col in tableDetail.Columns ?? [])
                {
                    string qualified = $"{tableName}.{col}";
                    if (!selectedColumns[tableName].Contains(qualified))
                    {
                        selectedColumns[tableName].Add(qualified);
                    }
                }

                continue;
            }

            if (name.Contains('(') && name.Contains(')'))
            {
                continue;
            }

            var parseResult = tableService.ParseAndFindTableNameByColumn(name);
            string resolvedTableName = parseResult.Item1;
            string resolvedColumnName = parseResult.Item2;

            if (!selectedColumns.ContainsKey(resolvedTableName))
            {
                selectedColumns[resolvedTableName] = [];
            }

            string resolvedQualified = $"{resolvedTableName}.{resolvedColumnName}";
            if (!selectedColumns[resolvedTableName].Contains(resolvedQualified))
            {
                selectedColumns[resolvedTableName].Add(resolvedQualified);
            }
        }

        return selectedColumns;
    }
}