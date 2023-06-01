using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal class Min : Aggregation
    {
        public Min(string field) : base(field) { }

        public override dynamic Apply(List<Dictionary<string, dynamic>> rows)
        {
            return rows.Min(row => row[_field]) ?? 0;
        }
    }
}
