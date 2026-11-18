using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    /// <summary>SQL <c>COUNT</c>: a 64-bit count (<see cref="long"/>) on every execution path.</summary>
    internal class Count(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName = null)
        : Aggregation(field, expression, valueSelector, headerName)
    {
        protected override object? Apply(ListedTable rows)
        {
            if (_field?.TableName == "*" && _expression == null)
            {
                return (long)rows.Count;
            }

            return rows.Select(SelectColumn)
                .Where(c => c != null)
                .LongCount();
        }
    }
}
