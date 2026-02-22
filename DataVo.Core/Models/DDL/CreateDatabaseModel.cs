using System.Text.RegularExpressions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

public class CreateDatabaseModel
{
    public CreateDatabaseModel(string databaseName) => DatabaseName = databaseName;

    public string DatabaseName { get; set; }

    public static CreateDatabaseModel FromMatch(Match match) => new(match.NthGroup(n: 1).Value);
    public static CreateDatabaseModel FromAst(CreateDatabaseStatement ast) => new(ast.DatabaseName.Name);

    public Database ToDatabase() =>
        new()
        {
            DatabaseName = DatabaseName,
            Tables = new List<Table>(),
        };
}