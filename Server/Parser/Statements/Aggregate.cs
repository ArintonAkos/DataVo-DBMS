using Server.Models.Statement;
using Server.Parser.Aggregations;
using Server.Parser.Types;
using Server.Services;
using Server.Utils;

namespace Server.Parser.Statements
{
    internal class Aggregate
    {
        public AggregateModel Model { get; private set; }
        public TableService TableService { get; private set; }

        public Aggregate(string match, string databaseName, TableService tableService)
        {
            Model = AggregateModel.FromString(match, databaseName, tableService);
            TableService = tableService;
        }

        public bool ContainsAggregate() => Model.Functions.Count > 0;

        public ListedTable Perform(GroupedTable tableData)
        {
            if (!ContainsAggregate())
            {
                if (tableData.ContainsKey(GroupBy.HASH_VALUE))
                {
                    return tableData[GroupBy.HASH_VALUE];
                }

                return tableData
                    .Select(g => g.Value.First())
                    .ToListedTable();
            }

            ListedTable resultTable = new();

            foreach (var groupedRow in tableData)
            {
                JoinedRow row = groupedRow.Value.First();
                row.Add(Aggregation.HASH_VALUE, new());

                foreach (var aggregateFunc in Model.Functions)
                {
                    var result = aggregateFunc.Execute(groupedRow.Value);

                    row[Aggregation.HASH_VALUE].Add(aggregateFunc.GetHeaderName(), result);
                }

                resultTable.Add(row);
            }

            return resultTable;
        }
    }
}
