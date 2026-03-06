using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DML;

public class UpdateModel
{
    public required string TableName { get; init; }
    public required Dictionary<string, ExpressionNode> SetExpressions { get; init; }
    public required ExpressionNode WhereExpression { get; init; }

    public static UpdateModel FromAst(UpdateStatement statement)
    {
        return new UpdateModel
        {
            TableName = statement.TableName.Name,
            SetExpressions = statement.SetClauses.ToDictionary(k => k.ColumnName.Name, v => v.Value),
            WhereExpression = statement.WhereClause ?? new LiteralNode() { Value = "true" }
        };
    }
}
