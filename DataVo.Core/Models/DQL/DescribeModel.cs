using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DQL;

internal class DescribeModel(string tableName)
{
    public string TableName { get; set; } = tableName;

    public static DescribeModel FromAst(DescribeStatement ast) => new(ast.TableName.Name);
}