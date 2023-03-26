using System.Text.RegularExpressions;

namespace Server.Models.DQL
{
    internal class UseModel
    {
        public String DatabaseName { get; set; }

        public UseModel(String databaseName)
        {
            this.DatabaseName = databaseName;
        }

        public static UseModel FromMatch(Match match)
        {
            return new UseModel(match.Groups["DatabaseName"].Value);
        }
    }
}
