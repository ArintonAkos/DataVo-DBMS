using System.Text.RegularExpressions;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
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

    private Group RawJoinStatement { get; set; } = null!;
    private string RawGroupByStatement { get; set; } = null!;
    private string RawColumns { get; set; } = null!;
    public string? RawHavingStatement { get; set; }
    public string? RawOrderByStatement { get; set; }

    public static SelectModel FromMatch(Match match)
    {
        var tableNameWithAlias = TableParserService.ParseTableWithAlias(match.Groups["TableName"].Value);
        string tableName = tableNameWithAlias.Item1;
        string? tableAlias = tableNameWithAlias.Item2;
        TableDetail fromTable = new(tableName, tableAlias);

        var whereStatement = new Where(match.Groups["WhereStatement"].Value, fromTable);

        return new SelectModel
        {
            WhereStatement = whereStatement,
            RawJoinStatement = match.Groups["Joins"],
            RawGroupByStatement = match.Groups["ColumnNames"].Value,
            RawColumns = match.Groups["Columns"].Value,
            FromTable = fromTable
        };
    }

    public static SelectModel FromAst(SelectStatement ast)
    {
        string rawTableName = ast.FromTable?.Name ?? string.Empty;
        var tableNameWithAlias = TableParserService.ParseTableWithAlias(rawTableName);
        string tableName = tableNameWithAlias.Item1;
        string? tableAlias = tableNameWithAlias.Item2;
        TableDetail fromTable = new(tableName, tableAlias);

        Where whereStatement;
        if (ast.WhereExpression != null)
        {
            whereStatement = new Where(ast.WhereExpression, fromTable);
        }
        else
        {
            whereStatement = new Where(string.Empty, fromTable);
        }

        string rawColumns = string.Join(", ", ast.Columns.Cast<IdentifierNode>().Select(c => c.Name));

        // Reconstruct RawJoinStatement for legacy backend compatibility
        string rawJoinString = string.Empty;
        if (ast.Joins.Count != 0)
        {
            var joinParts = ast.Joins.Select(j =>
            {
                string aliasPart = j.Alias != null ? $" AS {j.Alias.Name}" : "";
                string onPart = j.Condition != null ? $" ON {j.Condition.LeftTable.Name}.{j.Condition.LeftColumn.Name} = {j.Condition.RightTable.Name}.{j.Condition.RightColumn.Name}" : "";
                return $"{j.JoinType} {j.TableName.Name}{aliasPart}{onPart}";
            });
            rawJoinString = string.Join(" ", joinParts);
        }

        // Reconstruct RawGroupByStatement for legacy backend compatibility
        string rawGroupByString = string.Empty;
        if (ast.GroupByExpression != null && ast.GroupByExpression.Columns.Count != 0)
        {
            rawGroupByString = string.Join(", ", ast.GroupByExpression.Columns.Select(c => c.Name));
        }

        string rawHavingString = string.Empty;
        if (ast.HavingExpression != null)
        {
            rawHavingString = "True"; // Placeholder, as full expression reconstruction is complex and not yet supported by backend.
        }

        string rawOrderByString = string.Empty;
        if (ast.OrderByExpression != null && ast.OrderByExpression.Columns.Count != 0)
        {
            rawOrderByString = string.Join(", ", ast.OrderByExpression.Columns.Select(c => $"{c.Column.Name}{(c.IsAscending ? "" : " DESC")}"));
        }

        return new SelectModel
        {
            WhereStatement = whereStatement,
            RawJoinStatement = Regex.Match(rawJoinString, rawJoinString == string.Empty ? "" : ".*").Groups[0],
            RawGroupByStatement = rawGroupByString,
            RawHavingStatement = rawHavingString,
            RawOrderByStatement = rawOrderByString,
            RawColumns = rawColumns,
            FromTable = fromTable
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

        JoinStatement = new Join(RawJoinStatement, TableService);
        GroupByStatement = new GroupBy(RawGroupByStatement, databaseName, TableService);
        AggregateStatement = new Aggregate(RawColumns, databaseName, TableService);

        TableColumnsInUse = TableParserService.ParseSelectColumns(RawColumns, TableService);

        return false;
    }
}