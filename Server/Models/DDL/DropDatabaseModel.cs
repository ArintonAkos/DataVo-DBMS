using System.Text.RegularExpressions;
using Server.Utils;

namespace Server.Models.DDL;

internal class DropDatabaseModel
{
    public DropDatabaseModel(string databaseName)
    {
        DatabaseName = databaseName;
    }

    public string DatabaseName { get; set; }

    public static DropDatabaseModel FromMatch(Match match)
    {
        return new DropDatabaseModel(match.NthGroup(1).Value);
    }
}