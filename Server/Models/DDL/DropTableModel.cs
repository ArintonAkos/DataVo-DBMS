using Server.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Models.DDL
{
    internal class DropTableModel
    {
        public String TableName { get; set; }

        public DropTableModel(String databaseName)
        {
            this.TableName = databaseName;
        }

        public static DropTableModel FromMatch(Match match)
        {
            return new DropTableModel(match.NthGroup(1).Value);
        }
    }
}
