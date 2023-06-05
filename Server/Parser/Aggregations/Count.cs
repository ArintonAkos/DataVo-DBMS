using Server.Models.Statement.Utils;
using Server.Parser.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal class Count : Aggregation
    {
        public Count(Column field) : base(field) { }

        protected override dynamic? Apply(ListedTable rows)
        {
            if (_field.TableName == "*")
            {
                return rows.Count;
            }

            return rows.Select(SelectColumn)
                .Where(c => c != null)
                .Count();
        }
    }
}
