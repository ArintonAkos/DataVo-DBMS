using System.Text.RegularExpressions;
using Server.Models.Statement.Utils;
using Server.Parser.DDL;
using Server.Parser.Statements;
using Server.Services;

namespace Server.Models.DQL;

internal class SelectModel
{
    public Dictionary<string, List<string>>? TableColumnsInUse { get; set; }
    public string? Database { get; set; }
    public TableService? TableService { get; set; }
    public Where WhereStatement { get; set; }
    public Join JoinStatement { get; set; }
    public TableDetail FromTable { get; set; }

    private Group RawJoinStatement { get; set; }
    private string RawColumns { get; set; }

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
            RawColumns = match.Groups["Columns"].Value,
            FromTable = fromTable
        };
    }

    // returns column is "tablename.columnname" format
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
        List<string> columns = new();

        foreach (var table in TableService!.TableDetails)
        {
            columns.AddRange(Catalog.Catalog.GetTableColumns(table.Value.TableName, Database!)
                 .Select(c => $"{table.Value.TableName}.{c.Name}"));
        }

        return columns;
    }

    public bool Validate(string databaseName)
    {
        Database = databaseName;
        TableService = new TableService(databaseName);
        TableService.AddTableDetail(FromTable);

        JoinStatement = new Join(RawJoinStatement, TableService);
        foreach (var tableDetail in JoinStatement.Model.JoinTableDetails)
        {
            TableService.AddTableDetail(tableDetail.Value);
        }

        Dictionary<string, List<string>> tableColumns = new();
        foreach (var table in TableService.TableDetails)
        {
            tableColumns[table.Key] = Catalog.Catalog.GetTableColumns(table.Value.TableName, databaseName)
                .Select(c => c.Name)
                .ToList();
        }

        TableColumnsInUse = TableParserService.ParseColumns(RawColumns, tableColumns);

        return false;
    }
}