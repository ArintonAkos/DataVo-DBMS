using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Aggregations;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Services
{
    internal class AggregationService
    {
        private static readonly Dictionary<string, Type> _aggregationFunctions = new(StringComparer.OrdinalIgnoreCase)
        {
            { "avg", typeof(Avg) },
            { "count", typeof(Count) },
            { "max", typeof(Max) },
            { "min", typeof(Min) },
            { "sum", typeof(Sum) },
        };

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
            if (!_aggregationFunctions.TryGetValue(functionName, out var type))
            {
                throw new ArgumentException($"Unknown aggregation function: {functionName}");
            }

            return (Aggregation)Activator.CreateInstance(type, column, expression, valueSelector, headerName)!;
        }
    }
}
