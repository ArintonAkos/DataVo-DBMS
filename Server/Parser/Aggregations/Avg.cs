using Server.Models.Statement.Utils;
using Server.Parser.Types;

namespace Server.Parser.Aggregations
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
