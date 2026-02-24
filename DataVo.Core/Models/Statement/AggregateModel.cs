using DataVo.Core.Parser.Aggregations;
using DataVo.Core.Parser.AST;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Services;

namespace DataVo.Core.Models.Statement
{
    internal class AggregateModel(List<Aggregation> aggregations)
    {
        public List<Aggregation> Functions { get; set; } = aggregations;

        public static AggregateModel FromAstColumns(List<SqlNode> columns, string databaseName, TableService tableService)
        {
            List<Aggregation> aggregations = [];

            foreach (var node in columns)
            {
                if (node is not IdentifierNode identifierNode)
                {
                    continue;
                }

                string token = identifierNode.Name.Trim();
                int openParen = token.IndexOf('(');
                int closeParen = token.LastIndexOf(')');

                if (openParen <= 0 || closeParen <= openParen)
                {
                    continue;
                }

                string functionName = token[..openParen].Trim();
                string rawColumnName = token[(openParen + 1)..closeParen].Trim();

                Column column;
                if (functionName.Equals("count", StringComparison.OrdinalIgnoreCase) && rawColumnName == "*")
                {
                    column = new(databaseName, "*", "");
                }
                else
                {
                    var parseResult = tableService.ParseAndFindTableNameByColumn(rawColumnName);
                    column = new(databaseName, parseResult.Item1, parseResult.Item2);
                }

                Aggregation aggregation = AggregationService.CreateInstance(functionName, column);
                aggregations.Add(aggregation);
            }

            return new AggregateModel(aggregations);
        }
    }
}
