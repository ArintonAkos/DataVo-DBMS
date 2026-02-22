using System.Text.RegularExpressions;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

internal class DropDatabaseModel
{
    public DropDatabaseModel(string databaseName) => DatabaseName = databaseName;

    public string DatabaseName { get; set; }

    public static DropDatabaseModel FromMatch(Match match) => new(match.NthGroup(n: 1).Value);
    public static DropDatabaseModel FromAst(DropDatabaseStatement ast) => new(ast.DatabaseName.Name);
}