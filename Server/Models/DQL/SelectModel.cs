using System.Text.RegularExpressions;
using Server.Models.Statement.Utils;
using Server.Parser.Statements;
using Server.Services;

namespace Server.Models.DQL;

internal class SelectModel
{
    private Dictionary<string, List<string>>? TableColumnsInUse { get; set; }
    public string Database { get; set; }
    public TableService TableService { get; set; }
    public Where WhereStatement { get; set; }
    public Join JoinStatement { get; set; }

    public static SelectModel FromMatch(Match match, string databaseName)
    {
        TableService tableService = new(databaseName);

        var tableNameWithAlias = TableParserService.ParseTableWithAlias(match.Groups["TableName"].Value);
        string tableName = tableNameWithAlias.Item1;
        string? tableAlias = tableNameWithAlias.Item2;

        tableService.AddTableDetail(new TableDetail(tableName, tableAlias));

        var joinStatement = new Join(match.Groups["Joins"]);
        foreach (var tableDetail in joinStatement.Model.JoinTableDetails)
        {
            tableService.AddTableDetail(tableDetail.Value);
        }

        Dictionary<string, List<string>> tableColumns = new();

        foreach (var table in tableService.TableDetails)
        {
            tableColumns[table.Key] = Catalog.Catalog.GetTableColumns(table.Value.TableName, databaseName)
                .Select(c => c.Name)
                .ToList();
        }

        Dictionary<string, List<string>>? tableColumnsInUse = TableParserService.ParseColumns(match.Groups["Columns"].Value, tableColumns);

        // It means that * was as the column, we add every column name to the table
        /*if (tableColumnsInUse is null)
        {
            foreach (var table in tableColumns)
            {
                tableService.TableDetails[table.Key].Columns = table.Value;
            }
        }
        else
        {
            foreach (var table in tableColumnsInUse)
            {
                tableService.TableDetails[table.Key].Columns = table.Value;
            }
        }*/

        var whereStatement = new Where(match.Groups["WhereStatement"].Value);

        return new SelectModel
        {
            WhereStatement = whereStatement,
            JoinStatement = joinStatement,
            Database = databaseName,
            TableService = tableService,
            TableColumnsInUse = tableColumnsInUse,
        };
    }

    public bool Validate(string databaseName)
    {
        //List<Column> columns = Catalog.Catalog.GetTableColumns(TableName, databaseName);
        //bool hasMissingColumnsSpecified = false;

        //for (int i = 0; i < Columns.Count; i++)
        //{
        //    if (Columns[i].Contains(value: '.'))
        //    {
        //        string[] splitColumn = Columns[i].Split(separator: '.');
        //        string columnPrefix = splitColumn[0].Trim();
        //        string columnName = splitColumn[1].Trim();

        //        // Validate the table alias
        //        if (TableAlias != null && columnPrefix != TableAlias)
        //        {
        //            throw new Exception($"Invalid table alias: {columnPrefix}");
        //        }

        //        Columns[i] = columnName;
        //    }

        //    if (columns.All(c => c.Name != Columns[i]))
        //    {
        //        hasMissingColumnsSpecified = true;
        //    }
        //}

        return false;
    }
}