using Server.Models.Statement.Utils;
using Server.Parser.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal class Sum : Aggregation
    {
        public Sum(Column field) : base(field) { }

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
