using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.Statements;

internal class GroupBy(string match, string databaseName, TableService tableService)
{
    public static string HASH_VALUE
    {
        get
        {
            return string.Empty;
        }
    }

    public GroupByModel Model { get; private set; } = GroupByModel.FromString(match, databaseName, tableService);
    public TableService TableService { get; private set; } = tableService;

    public GroupBy(GroupByNode? groupByNode, string databaseName, TableService tableService)
        : this(string.Empty, databaseName, tableService)
    {
        Model = GroupByModel.FromAst(groupByNode, databaseName, tableService);
        TableService = tableService;
    }

    public bool ContainsGroupBy() => Model.Columns.Count > 0;

    public GroupedTable Evaluate(ListedTable tableData)
    {
        if (!ContainsGroupBy())
        {
            return tableData.ToGroupedTable();
        }

        GroupedTable groupedTableData = [];

        foreach (JoinedRow row in tableData)
        {
            string rowHash = CreateHashForRow(row);

            if (!groupedTableData.ContainsKey(rowHash))
            {
                groupedTableData.Add(rowHash, []);
            }

            groupedTableData[rowHash].Add(row);
        }

        return groupedTableData;
    }

    private string CreateHashForRow(JoinedRow row)
    {
        List<string> columnValues = [];

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