using System.Text.RegularExpressions;
using Server.Models.Catalog;
using Server.Parser.Statements;

namespace Server.Models.DQL;

internal class SelectModel
{
    public string TableName { get; set; }
    public string? TableAlias { get; set; }
    public List<string> Columns { get; set; }
    public Where WhereStatement { get; set; }
    public Join JoinStatement { get; set; }

    public static SelectModel FromMatch(Match match)
    {
        string tableName = match.Groups["TableName"].Value;
        string? tableAlias = null;

        if (tableName.ToLower().Contains("as"))
        {
            var splitTableName = tableName
                .Split("as")
                .Select(r => r.Replace(" ", ""))
                .ToList();

            tableName = splitTableName[index: 0];
            tableAlias = splitTableName[index: 1];
        }

        string columns = match.Groups["Columns"].Value;
        var columnsList = columns.Split(separator: ',')
            .Select(c => c.Trim())
            .ToList();
        var whereStatement = new Where(match.Groups["WhereStatement"].Value);
        var joinStatement = new Join(match.Groups["Joins"].Value);

        return new SelectModel
        {
            TableName = tableName,
            TableAlias = tableAlias,
            Columns = columnsList,
            WhereStatement = whereStatement,
            JoinStatement = joinStatement,
        };
    }

    public bool Validate(string databaseName)
    {
        List<Column> columns = Catalog.Catalog.GetTableColumns(TableName, databaseName);
        bool hasMissingColumnsSpecified = false;

        foreach (string selectedColumn in Columns)
        {
            string columnName;

            if (selectedColumn.Contains(value: '.'))
            {
                string[] splitColumn = selectedColumn.Split(separator: '.');
                string columnPrefix = splitColumn[0].Trim();
                columnName = splitColumn[1].Trim();

                // Validate the table alias
                if (TableAlias != null && columnPrefix != TableAlias)
                {
                    throw new Exception($"Invalid table alias: {columnPrefix}");
                }
            }
            else
            {
                columnName = selectedColumn;
            }

            if (columns.All(c => c.Name != columnName))
            {
                hasMissingColumnsSpecified = true;
            }
        }

        return hasMissingColumnsSpecified;
    }
}