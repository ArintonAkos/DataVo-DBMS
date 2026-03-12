using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Min(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName = null)
        : Aggregation(field, expression, valueSelector, headerName)
    {
        protected override dynamic? Apply(ListedTable rows)
        {
            return rows.Min(SelectColumn);
        }

        protected override void Validate()
        {
            ValidateNumericColumn();
            ValidateStringColumn();
            ValidateDateColumn();
        }
    }
}
