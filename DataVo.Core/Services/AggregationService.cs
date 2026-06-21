using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Aggregations;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Services
{
    internal class AggregationService
    {
        public static Aggregation CreateInstance(string functionName, Column column)
        {
            return CreateInstance(
                functionName,
                column,
                null,
                row => row[column.TableName][column.ColumnName],
                null
            );
        }

        public static Aggregation CreateInstance(string functionName, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string headerName, Column? column = null)
        {
            return CreateInstance(functionName, column, expression, valueSelector, headerName);
        }

        private static Aggregation CreateInstance(string functionName, Column? column, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName)
        {
            // Explicit factory (no Activator/reflection) so the aggregation types' constructors are
            // statically referenced and the path is Native-AOT safe.
            return functionName.ToUpperInvariant() switch
            {
                "AVG" => new Avg(column, expression, valueSelector, headerName),
                "COUNT" => new Count(column, expression, valueSelector, headerName),
                "MAX" => new Max(column, expression, valueSelector, headerName),
                "MIN" => new Min(column, expression, valueSelector, headerName),
                "SUM" => new Sum(column, expression, valueSelector, headerName),
                _ => throw new ArgumentException($"Unknown aggregation function: {functionName}"),
            };
        }
    }
}
