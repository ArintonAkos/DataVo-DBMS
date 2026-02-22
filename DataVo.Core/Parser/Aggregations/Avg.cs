using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Avg(Column field) : Aggregation(field)
    {
        protected override dynamic? Apply(ListedTable rows)
        {
            return rows.Average(SelectColumn<double?>);
        }

        protected override void Validate()
        {
            ValidateNumericColumn();
        }
    }
}
