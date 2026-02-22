using DataVo.Core.Parser.Aggregations;
using DataVo.Core.Services;

namespace DataVo.Core.Models.Statement
{
    internal class AggregateModel(List<Aggregation> aggregations)
    {
        public List<Aggregation> Functions { get; set; } = aggregations;

        public static AggregateModel FromString(string match, string databaseName, TableService tableService)
        {
            var aggregations = TableParserService.ParseAggregationColumns(match, databaseName, tableService);
            return new AggregateModel(aggregations);
        }
    }
}
