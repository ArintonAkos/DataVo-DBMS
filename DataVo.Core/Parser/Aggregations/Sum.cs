using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Sum(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName = null)
        : Aggregation(field, expression, valueSelector, headerName)
    {
        protected override object? Apply(ListedTable rows)
        {
            double sum = 0;
            bool any = false;

            foreach (var row in rows)
            {
                var value = SelectColumn(row);
                if (value == null)
                {
                    continue;
                }

                sum += Convert.ToDouble(value);
                any = true;
            }

            // SQL: the sum of no values (an empty group, or only NULLs) is NULL, not 0.
            return any ? sum : null;
        }

        protected override void Validate()
        {
            ValidateNumericColumn();
        }
    }
}
