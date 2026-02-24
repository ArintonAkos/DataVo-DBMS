using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Services;

namespace DataVo.Core.Models.Statement
{
    internal class GroupByModel(List<Column> columns)
    {
        public List<Column> Columns { get; set; } = columns;

        public static GroupByModel FromAst(GroupByNode? groupByNode, string databaseName, TableService tableService)
        {
            if (groupByNode == null || groupByNode.Columns.Count == 0)
            {
                return new GroupByModel([]);
            }

            List<Column> columns = [];

            foreach (var colNode in groupByNode.Columns)
            {
                var parseResult = tableService.ParseAndFindTableNameByColumn(colNode.Name);
                Column column = new(databaseName, parseResult.Item1, parseResult.Item2);

                if (!columns.Any(c => c.ColumnName == column.ColumnName && c.TableName == column.TableName))
                {
                    columns.Add(column);
                }
            }

            return new GroupByModel(columns);
        }
    }
}
