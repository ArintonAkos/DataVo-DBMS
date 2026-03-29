using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DML;

/// <summary>
/// Represents the normalized model for an UPDATE statement.
/// </summary>
public class UpdateModel
{
    /// <summary>
    /// Gets the target table name.
    /// </summary>
    public required string TableName { get; init; }

    /// <summary>
    /// Gets mapped SET expressions by column name.
    /// </summary>
    public required Dictionary<string, ExpressionNode> SetExpressions { get; init; }

    /// <summary>
    /// Gets the WHERE expression, or true literal when omitted.
    /// </summary>
    public required ExpressionNode WhereExpression { get; init; }

    /// <summary>
    /// Builds a model from UPDATE AST.
    /// </summary>
    /// <param name="statement">The parsed UPDATE statement.</param>
    /// <returns>The normalized update model.</returns>
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
