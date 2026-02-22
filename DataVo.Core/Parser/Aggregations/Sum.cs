using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Sum(Column field) : Aggregation(field)
    {
        protected override dynamic? Apply(ListedTable rows)
        {
            return rows.Sum(SelectColumn<double?>);
        }

        protected override void Validate()
        {
            ValidateNumericColumn();
        }
    }
}
