using System.Text.RegularExpressions;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DQL;

internal class DescribeModel
{
    public DescribeModel(string tableName) => TableName = tableName;

    public string TableName { get; set; }

    public static DescribeModel FromMatch(Match match) => new(match.NthGroup(n: 1).Value);
    public static DescribeModel FromAst(DescribeStatement ast) => new(ast.TableName.Name);
}