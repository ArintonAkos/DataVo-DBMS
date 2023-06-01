using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal class Avg : Aggregation
    {
        public Avg(string field) : base(field) { }

        public override dynamic Apply(List<Dictionary<string, dynamic>> rows)
        {
            return rows.Average(row => (double)row[_field]);
        }
    }
}
