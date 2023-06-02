using Server.Parser.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Utils
{
    public static class DictionaryExtensions
    {
        public static Row ToRow(this Dictionary<string, dynamic> dictionary)
        {
            return new Row(dictionary);
        }

        public static HashedTable ToHashedTable(this Dictionary<string, JoinedRow> dictionary)
        {
            return new HashedTable(dictionary);
        }
    }
}
