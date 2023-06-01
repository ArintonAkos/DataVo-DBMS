using Server.Models.Statement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Statements
{
    internal class GroupBy
    {
        public GroupByModel? GroupByModel { get; private set; }

        public GroupBy(string match)
        {
        }
    }
}
