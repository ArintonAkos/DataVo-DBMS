using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal class Sum : Aggregation
    {
        public Sum(string field) : base(field) { }

        public override dynamic Apply(List<Dictionary<string, dynamic>> rows)
        {
            return rows.Sum(row => (double)row[_field]);
        }
    }
}
