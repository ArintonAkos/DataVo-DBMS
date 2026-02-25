using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.Statement;

internal class WhereModel
{
    public required ExpressionNode Statement { get; set; }

    public static WhereModel FromExpression(ExpressionNode node)
    {
        return new WhereModel
        {
            Statement = node
        };
    }
}