using System.Text.RegularExpressions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DQL;

internal class UseModel
{
    public UseModel(string databaseName) => DatabaseName = databaseName;

    public string DatabaseName { get; set; }

    public static UseModel FromMatch(Match match) => new(match.Groups["DatabaseName"].Value);
    public static UseModel FromAst(UseStatement ast) => new(ast.DatabaseName.Name);
}