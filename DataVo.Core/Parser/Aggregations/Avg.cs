using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Avg : Aggregation
    {
        public Avg(Column field) : base(field) { }

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
