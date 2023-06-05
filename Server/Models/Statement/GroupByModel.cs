using Server.Models.Statement.Utils;
using Server.Parser.Aggregations;
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
        public List<Aggregation> Aggregations { get; set; }

        public GroupByModel(List<Column> columns, List<Aggregation> aggregations)
        {
            Columns = columns;
            Aggregations = aggregations;
        }

        public static GroupByModel FromString(string columnNamesString, TableService tableService)
        {
            List<Column> columns = TableParserService.ParseGroupByColumns(columnNamesString, tableService);
            List<Aggregation> aggregations = new();

            return new GroupByModel(columns, aggregations);
        }
    }
}
