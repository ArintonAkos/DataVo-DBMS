using Server.Models.Statement.Utils;
using Server.Parser.Aggregations;
using Server.Services;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.Statement
{
    internal class AggregateModel
    {
        public List<Aggregation> Functions { get; set; } = new();

        public static AggregateModel FromString(string match, string databaseName, TableService tableService)
        {
            var aggregations = TableParserService.ParseAggregationColumns(match, databaseName, tableService);

            return new()
            {
                Functions = aggregations
            };
        }
    }
}
