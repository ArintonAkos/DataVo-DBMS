using Server.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Models.DDL
{
    internal class DropDatabaseModel
    {
        public String DatabaseName { get; set; }

        public DropDatabaseModel(String databaseName)
        {
            this.DatabaseName = databaseName;
        }

        public static DropDatabaseModel FromMatch(Match match)
        {
            return new DropDatabaseModel(match.NthGroup(1).Value);
        }
    }
}
