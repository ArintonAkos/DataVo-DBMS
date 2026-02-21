using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataVo.Core.Parser.Aggregations
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
