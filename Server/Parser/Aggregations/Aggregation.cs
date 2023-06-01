using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Aggregations
{
    internal abstract class Aggregation
    {
        protected readonly string _field;

        protected Aggregation(string field)
        {
            _field = field;
        }

        public abstract dynamic Apply(List<Dictionary<string, dynamic>> rows);
    }
}
