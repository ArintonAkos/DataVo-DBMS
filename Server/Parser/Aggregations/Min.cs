using Server.Models.Statement.Utils;
using Server.Parser.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal class Min : Aggregation
    {
        public Min(Column field) : base(field) { }

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
