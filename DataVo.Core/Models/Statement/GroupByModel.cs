using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Services;

namespace DataVo.Core.Models.Statement
{
    internal class GroupByModel(List<Column> columns)
    {
        public List<Column> Columns { get; set; } = columns;

        public static GroupByModel FromString(string columnNamesString, string databaseName, TableService tableService)
        {
            List<Column> columns = TableParserService.ParseGroupByColumns(columnNamesString, databaseName, tableService);

            return new GroupByModel(columns);
        }
    }
}
