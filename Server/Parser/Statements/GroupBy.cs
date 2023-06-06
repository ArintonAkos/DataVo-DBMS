using Server.Models.Statement;
using Server.Parser.Types;
using Server.Services;

namespace Server.Parser.Statements;

internal class GroupBy
{
    public static string HASH_VALUE
    {
        get
        {
            return string.Empty;
        }
    }

    public GroupByModel Model { get; private set; }
    public TableService TableService { get; private set; }

    public GroupBy(string match, string databaseName, TableService tableService)
    {
        Model = GroupByModel.FromString(match, databaseName, tableService);
        TableService = tableService;
    }

    public bool ContainsGroupBy() => Model.Columns.Count > 0;

    public GroupedTable Evaluate(ListedTable tableData)
    {
        if (!ContainsGroupBy())
        {
            return tableData.ToGroupedTable();
        }

        GroupedTable groupedTableData = new();

        foreach (JoinedRow row in tableData)
        {
            string rowHash = CreateHashForRow(row);

            if (!groupedTableData.ContainsKey(rowHash))
            {
                groupedTableData.Add(rowHash, new());
            }

            groupedTableData[rowHash].Add(row);
        }

        return groupedTableData;
    }

    private string CreateHashForRow(JoinedRow row)
    {
        List<string> columnValues = new();

        Model.Columns.ForEach(column =>
        {
            if (!row.ContainsKey(column.TableName) || row[column.TableName] == null)
            {
                throw new Exception("Trying to group by inexistent table!");
            }

            if (!row[column.TableName].ContainsKey(column.ColumnName)) 
            {
                throw new Exception("Trying to group by inexistent column!");
            }

            if (row[column.TableName][column.ColumnName] == null)
            {
                columnValues.Add(string.Empty);
            }
            else
            {
                var columnValue = row[column.TableName][column.ColumnName].ToString();
                string hashCode = columnValue.GetHashCode().ToString();

                columnValues.Add(hashCode);
            }
        });

        return string.Join("##", columnValues);
    }
}