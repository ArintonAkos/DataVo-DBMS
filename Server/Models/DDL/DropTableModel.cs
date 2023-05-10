using System.Text.RegularExpressions;
using Server.Utils;

namespace Server.Models.DDL;

internal class DropTableModel
{
    public DropTableModel(string databaseName)
    {
        TableName = databaseName;
    }

    public string TableName { get; set; }

    public static DropTableModel FromMatch(Match match)
    {
        return new DropTableModel(match.NthGroup(1).Value);
    }
}