using System.Text.RegularExpressions;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DQL;

internal class DescribeModel(string tableName)
{
    public string TableName { get; set; } = tableName;

    public static DescribeModel FromMatch(Match match) => new(match.NthGroup(n: 1).Value);
    public static DescribeModel FromAst(DescribeStatement ast) => new(ast.TableName.Name);
}