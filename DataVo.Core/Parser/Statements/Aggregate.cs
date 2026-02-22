using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Aggregations;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.Statements
{
    internal class Aggregate(string match, string databaseName, TableService tableService)
    {
        public AggregateModel Model { get; private set; } = AggregateModel.FromString(match, databaseName, tableService);
        public TableService TableService { get; private set; } = tableService;

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

            ListedTable resultTable = [];

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
