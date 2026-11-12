using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Max(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName = null)
        : Aggregation(field, expression, valueSelector, headerName)
    {
        protected override object? Apply(ListedTable rows)
        {
            return Extreme(rows, smallest: false);
        }

        protected override void Validate()
        {
            ValidateOrderableColumn();
        }
    }

}
