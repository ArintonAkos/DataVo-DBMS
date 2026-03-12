using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Avg(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName = null)
        : Aggregation(field, expression, valueSelector, headerName)
    {
        protected override dynamic? Apply(ListedTable rows)
        {
            double sum = 0;
            int count = 0;

            foreach (var row in rows)
            {
                var value = SelectColumn(row);
                if (value == null)
                {
                    continue;
                }

                sum += Convert.ToDouble(value);
                count++;
            }

            return count == 0 ? null : sum / count;
        }

        protected override void Validate()
        {
            ValidateNumericColumn();
        }
    }
}
