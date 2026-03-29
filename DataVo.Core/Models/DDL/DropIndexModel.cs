using System.Text.RegularExpressions;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

/// <summary>
/// Represents the normalized model for a DROP INDEX statement.
/// </summary>
/// <param name="indexName">The index name.</param>
/// <param name="tableName">The table name.</param>
public class DropIndexModel(string indexName, string tableName)
{
    /// <summary>
    /// Gets or sets the table name.
    /// </summary>
    public string TableName { get; set; } = tableName;

    /// <summary>
    /// Gets or sets the index name.
    /// </summary>
    public string IndexName { get; set; } = indexName;

    /// <summary>
    /// Builds a model from DROP INDEX AST.
    /// </summary>
    /// <param name="ast">The parsed DROP INDEX statement.</param>
    /// <returns>The normalized drop-index model.</returns>
    public static DropIndexModel FromAst(DropIndexStatement ast) => new(ast.IndexName.Name, ast.TableName.Name);
}