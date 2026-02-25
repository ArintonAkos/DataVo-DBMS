using System.Text.RegularExpressions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DQL;

internal class UseModel(string databaseName)
{
    public string DatabaseName { get; set; } = databaseName;

    public static UseModel FromAst(UseStatement ast) => new(ast.DatabaseName.Name);
}