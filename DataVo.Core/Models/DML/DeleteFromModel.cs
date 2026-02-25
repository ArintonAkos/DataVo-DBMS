using System.Text.RegularExpressions;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.AST;
using DataVo.Core.Constants;

namespace DataVo.Core.Models.DML;

internal class DeleteFromModel
{
    public string TableName { get; set; } = null!;
    public Where WhereStatement { get; set; } = null!;

    public static DeleteFromModel FromAst(DeleteFromStatement ast)
    {
        ExpressionNode whereNode = ast.WhereExpression ?? new LiteralNode { Value = SqlLiterals.TrueExpression };

        return new DeleteFromModel()
        {
            TableName = ast.TableName.Name,
            WhereStatement = new Where(whereNode)
        };
    }
}