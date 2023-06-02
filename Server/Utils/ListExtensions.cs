using Server.Parser.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Utils
{
    public static class ListExtensions
    {
        public static ListedTable ToListedTable(this IEnumerable<JoinedRow> list)
        {
            return new ListedTable(list.ToList());
        }

        public static ListedTable ToListedTable(this List<JoinedRow> list)
        {
            return new ListedTable(list);
        }
    }
}
