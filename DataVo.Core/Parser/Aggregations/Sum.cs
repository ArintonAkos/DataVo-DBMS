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
            // SQL: an integer sum stays an integer; NULL when there are no values (an empty group, or only NULLs).
            var sum = new DataVo.Core.Execution.SqlSumAccumulator();
            foreach (var row in rows)
            {
                var value = SelectColumn(row);
                if (value != null)
                {
                    sum.Add(value);
                }
            }

            return sum.Result;
        }

        protected override void Validate()
        {
            ValidateNumericColumn();
        }
    }
}
