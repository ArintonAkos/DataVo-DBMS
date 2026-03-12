using DataVo.Core.Parser.Aggregations;
using DataVo.Core.Parser.AST;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Services;

namespace DataVo.Core.Models.Statement
{
    internal class AggregateModel(List<Aggregation> aggregations)
    {
        public List<Aggregation> Functions { get; set; } = aggregations;

        public static AggregateModel FromAstColumns(List<SelectColumnNode> columns, string databaseName, TableService tableService)
        {
            List<Aggregation> aggregations = [];

            foreach (var node in columns)
            {
                // Prefer structured AST if available
                if (node.Expression is AggregateExpressionNode aggNode)
                {
                    string functionName = aggNode.FunctionName;
                    Column? column = ResolveAggregateColumn(databaseName, tableService, aggNode);

                    Func<JoinedRow, object?> selector = row =>
                    {
                        if (aggNode.IsStar)
                        {
                            return 1;
                        }

                        if (aggNode.Argument == null)
                        {
                            return null;
                        }

                        return ExpressionEvaluator.Evaluate(
                            aggNode.Argument,
                            row,
                            (colRef, joinedRow) => ResolveColumnValue(joinedRow, colRef, tableService),
                            (_, _) => throw new Exception("Nested aggregate expressions are not supported.")
                        );
                    };

                    string headerName = AggregateExpressionFormatter.BuildHeader(aggNode);
                    Aggregation aggregation = AggregationService.CreateInstance(functionName, aggNode.Argument, selector, headerName, column);
                    aggregations.Add(aggregation);
                    continue;
                }

                // Fallback for legacy raw-expression strings
                string token = (node.RawExpression ?? string.Empty).Trim();
                if (token.Contains("OVER", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                int openParen2 = token.IndexOf('(');
                int closeParen2 = token.LastIndexOf(')');

                if (openParen2 <= 0 || closeParen2 <= openParen2)
                {
                    continue;
                }

                string functionName2 = token[..openParen2].Trim();
                string rawColumnName2 = token[(openParen2 + 1)..closeParen2].Trim();

                Column column2;
                if (functionName2.Equals("count", StringComparison.OrdinalIgnoreCase) && rawColumnName2 == "*")
                {
                    column2 = new(databaseName, "*", "");
                }
                else
                {
                    var parseResult = tableService.ParseAndFindTableNameByColumn(rawColumnName2);
                    column2 = new(databaseName, parseResult.Item1, parseResult.Item2);
                }

                Aggregation aggregation2 = AggregationService.CreateInstance(functionName2, column2);
                aggregations.Add(aggregation2);
            }

            return new AggregateModel(aggregations);
        }

        private static Column? ResolveAggregateColumn(string databaseName, TableService tableService, AggregateExpressionNode aggNode)
        {
            if (aggNode.IsStar)
            {
                return new Column(databaseName, "*", string.Empty);
            }

            if (aggNode.Argument is not ColumnRefNode colRef)
            {
                return null;
            }

            var resolved = tableService.ParseAndFindTableNameByColumn(
                string.IsNullOrEmpty(colRef.TableOrAlias)
                    ? colRef.Column
                    : $"{colRef.TableOrAlias}.{colRef.Column}"
            );

            return new Column(databaseName, resolved.Item1, resolved.Item2);
        }

        private static object? ResolveColumnValue(JoinedRow row, ColumnRefNode colRef, TableService tableService)
        {
            string lookup = string.IsNullOrEmpty(colRef.TableOrAlias)
                ? colRef.Column
                : $"{colRef.TableOrAlias}.{colRef.Column}";

            var parsed = tableService.ParseAndFindTableNameByColumn(lookup);
            string tableName = parsed.Item1;
            string columnName = parsed.Item2;

            if (!row.ContainsKey(tableName) || !row[tableName].ContainsKey(columnName))
            {
                throw new Exception($"Column '{lookup}' not found in aggregate source row.");
            }

            return row[tableName][columnName];
        }
    }
}
