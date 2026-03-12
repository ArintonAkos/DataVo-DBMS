using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Sum(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName = null)
        : Aggregation(field, expression, valueSelector, headerName)
    {
        protected override dynamic? Apply(ListedTable rows)
        {
            double sum = 0;

            foreach (var row in rows)
            {
                var value = SelectColumn(row);
                if (value == null)
                {
                    continue;
                }

                sum += Convert.ToDouble(value);
            }

            return sum;
        }

        protected override void Validate()
        {
            ValidateNumericColumn();
        }
    }
}
