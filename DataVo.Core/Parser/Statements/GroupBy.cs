using System.Globalization;
using System.Text;
using DataVo.Core.Models.Statement;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.Statements;

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

    public GroupBy(GroupByNode? groupByNode, string databaseName, TableService tableService)
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

    /// <summary>Builds the group key of a row from its grouping column values.</summary>
    private string CreateHashForRow(JoinedRow row)
    {
        var values = new object?[Model.Columns.Count];
        for (int i = 0; i < values.Length; i++)
        {
            var column = Model.Columns[i];
            if (!row.ContainsKey(column.TableName) || row[column.TableName] == null)
            {
                throw new BindingException("Trying to group by inexistent table!");
            }

            if (!row[column.TableName].ContainsKey(column.ColumnName))
            {
                throw new BindingException("Trying to group by inexistent column!");
            }

            values[i] = row[column.TableName][column.ColumnName];
        }

        return BuildGroupKey(values);
    }

    /// <summary>
    /// Builds a group key from the grouping values themselves, never from hash codes, so distinct groups can
    /// never collide. Each value is length-prefixed (NULL has its own marker), so no combination of values can
    /// spell another.
    /// </summary>
    /// <param name="values">The row's grouping values, in GROUP BY order.</param>
    /// <returns>A key equal for two rows exactly when their grouping values are equal.</returns>
    internal static string BuildGroupKey(IReadOnlyList<object?> values)
    {
        var key = new StringBuilder();
        foreach (object? value in values)
        {
            if (value is null)
            {
                key.Append('N');
                continue;
            }

            string text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
            key.Append('V').Append(text.Length.ToString(CultureInfo.InvariantCulture)).Append(':').Append(text);
        }

        return key.ToString();
    }
}
