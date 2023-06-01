using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal class Max : Aggregation
    {
        public Max(string field) : base(field) { }

        public override dynamic Apply(List<Dictionary<string, dynamic>> rows)
        {
            return rows.Max(row => row[_field]);
        }
    }

}
