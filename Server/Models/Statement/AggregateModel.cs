using Server.Parser.Aggregations;
using Server.Services;

namespace Server.Models.Statement
{
    internal class AggregateModel
    {
        public List<Aggregation> Functions { get; set; } = new();

        public AggregateModel(List<Aggregation> aggregations)
        {
            Functions = aggregations;
        }

        public static AggregateModel FromString(string match, string databaseName, TableService tableService)
        {
            var aggregations = TableParserService.ParseAggregationColumns(match, databaseName, tableService);
            return new AggregateModel(aggregations);
        }
    }
}
