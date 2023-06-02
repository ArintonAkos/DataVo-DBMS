using Server.Models.Statement.Utils;
using Server.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.Statement
{
    internal class GroupByModel
    {
        public List<Column> Columns { get; set; }

        public GroupByModel(List<Column> columns)
        {
            Columns = columns;
        }

        public static GroupByModel FromString(string columnNamesString, TableService tableService)
        {
            List<Column> columns = TableParserService.ParseGroupByColumns(columnNamesString, tableService);

            return new GroupByModel(columns);
        }
    }
}
