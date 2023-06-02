using Server.Models.Statement;
using Server.Parser.Commands;
using Server.Services;
using System.Collections.Generic;
using System.Collections.Generic;

namespace Server.Parser.Statements;
using TableRows = List<Dictionary<string, Dictionary<string, dynamic>>>;

internal class GroupBy
{
    public GroupByModel GroupByModel { get; private set; }
    public TableService TableService { get; private set; }

    public GroupBy(string match, TableService tableService)
    {
        GroupByModel = GroupByModel.FromString(match, tableService);
        TableService = tableService;
    }
        
    public bool ContainsGroupBy() => GroupByModel.Columns.Count > 0;

    public TableRows Evaluate(TableRows tableData)
    {
        if (!ContainsGroupBy())
        {
            return tableData;
        }

        var groupedTableData = new TableRows();

        foreach (var row in tableData)
        {
            var groupedRow = new Dictionary<string, Dictionary<string, dynamic>>();
            foreach (var column in GroupByModel.Columns)
            {
                var tableName = column.TableName;
                var columnName = column.ColumnName;
                if (!row.ContainsKey(tableName))
                {
                    throw new Exception($"Column {columnName} does not exist in table {tableName}.");
                }
                if (!groupedRow.ContainsKey(tableName))
                {
                    groupedRow[tableName] = new Dictionary<string, dynamic>();
                }
                groupedRow[tableName][columnName] = row[tableName][columnName];
            }
            groupedTableData.Add(groupedRow);
        }
        return groupedTableData;
    }
}