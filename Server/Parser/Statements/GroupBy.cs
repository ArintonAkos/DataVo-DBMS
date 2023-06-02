using Server.Models.Statement;
using Server.Parser.Commands;
using Server.Parser.Types;
using Server.Services;
using System.Collections.Generic;

namespace Server.Parser.Statements;

internal class GroupBy
{
    public GroupByModel Model { get; private set; }
    public TableService TableService { get; private set; }

    public GroupBy(string match, TableService tableService)
    {
        Model = GroupByModel.FromString(match, tableService);
        TableService = tableService;
    }
        
    public bool ContainsGroupBy() => Model.Columns.Count > 0;

    public ListedTable Evaluate(ListedTable tableData)
    {
        if (!ContainsGroupBy())
        {
            return tableData;
        }

        HashedTable groupedTableData = new();

        foreach (JoinedRow row in tableData)
        {
            string rowHash = CreateHashForRow(row);

            groupedTableData.Add(rowHash, row);
        }

        return groupedTableData.ToListedTable();
    }

    private string CreateHashForRow(JoinedRow row)
    {
        List<string> columnValues = new();

        Model.Columns.ForEach(column =>
        {
            if (!row.ContainsKey(column.TableName) || row[column.TableName] == null)
            {
                throw new Exception("Trying to join on inexistent table!");
            }

            if (row[column.TableName].ContainsKey(column.ColumnName)) 
            {
                throw new Exception("Trying to join on inexistent column!");
            }

            if (row[column.TableName][column.ColumnName] == null)
            {
                columnValues.Add(string.Empty);
            }
            else
            {
                var columnValue = (string)row[column.TableName][column.ColumnName];
                string hashCode = columnValue.GetHashCode().ToString();

                columnValues.Add(hashCode);
            }
        });

        return string.Join("##", columnValues);
    }
}