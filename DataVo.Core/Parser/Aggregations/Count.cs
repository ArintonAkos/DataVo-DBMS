using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Count(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName = null)
        : Aggregation(field, expression, valueSelector, headerName)
    {
        protected override dynamic? Apply(ListedTable rows)
        {
            if (_field?.TableName == "*" && _expression == null)
            {
                return rows.Count;
            }

            return rows.Select(SelectColumn)
                .Where(c => c != null)
                .Count();
        }
    }
}
