using Server.Models.Statement.Utils;
using Server.Parser.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal class Max : Aggregation
    {
        public Max(Column field) : base(field) { }

        protected override dynamic? Apply(ListedTable rows)
        {
            return rows.Max(SelectColumn);
        }

        protected override void Validate()
        {
            ValidateNumericColumn();
            ValidateStringColumn();
            ValidateDateColumn();
        }
    }

}
