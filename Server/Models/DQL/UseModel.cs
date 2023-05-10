using System.Text.RegularExpressions;

namespace Server.Models.DQL;

internal class UseModel
{
    public UseModel(string databaseName)
    {
        DatabaseName = databaseName;
    }

    public string DatabaseName { get; set; }

    public static UseModel FromMatch(Match match)
    {
        return new UseModel(match.Groups["DatabaseName"].Value);
    }
}