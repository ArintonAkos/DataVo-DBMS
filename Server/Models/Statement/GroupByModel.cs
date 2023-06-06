using Server.Models.Statement.Utils;
using Server.Services;

namespace Server.Models.Statement
{
    internal class GroupByModel
    {
        public List<Column> Columns { get; set; }

        public GroupByModel(List<Column> columns)
        {
            Columns = columns;
        }

        public static GroupByModel FromString(string columnNamesString, string databaseName, TableService tableService)
        {
            List<Column> columns = TableParserService.ParseGroupByColumns(columnNamesString, databaseName, tableService);

            return new GroupByModel(columns);
        }
    }
}
