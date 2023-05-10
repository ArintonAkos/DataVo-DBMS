using System.Text.RegularExpressions;
using Server.Models.Catalog;
using Server.Utils;

namespace Server.Models.DDL;

public class CreateDatabaseModel
{
    public CreateDatabaseModel(string databaseName) => DatabaseName = databaseName;

    public string DatabaseName { get; set; }

    public static CreateDatabaseModel FromMatch(Match match) => new(match.NthGroup(n: 1).Value);

    public Database ToDatabase() =>
        new()
        {
            DatabaseName = DatabaseName,
            Tables = new List<Table>(),
        };
}