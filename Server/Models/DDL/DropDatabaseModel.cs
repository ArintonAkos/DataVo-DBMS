using Server.Utils;
using System.Text.RegularExpressions;

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
