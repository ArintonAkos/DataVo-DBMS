using Server.Utils;
using System.Text.RegularExpressions;

namespace Server.Models.DDL
{
    public class CreateDatabaseModel
    {
        public String DatabaseName { get; set; }

        public CreateDatabaseModel(String databaseName)
        {
            this.DatabaseName = databaseName;
        }

        public static CreateDatabaseModel FromMatch(Match match)
        {
            return new CreateDatabaseModel(match.NthGroup(1).Value); ;
        }

        public Database ToDatabase()
        {
            return new Database()
            {
                DatabaseName = this.DatabaseName,
                Tables = new()
            };
        }
    }
}
