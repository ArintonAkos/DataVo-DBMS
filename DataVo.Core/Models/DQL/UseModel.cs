using System.Text.RegularExpressions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DQL;

internal class UseModel(string databaseName)
{
    public string DatabaseName { get; set; } = databaseName;

    public static UseModel FromMatch(Match match) => new(match.Groups["DatabaseName"].Value);
    public static UseModel FromAst(UseStatement ast) => new(ast.DatabaseName.Name);
}