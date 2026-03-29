using System.Text.RegularExpressions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

/// <summary>
/// Represents the normalized model for a CREATE INDEX statement.
/// </summary>
/// <param name="indexName">The index name.</param>
/// <param name="tableName">The target table name.</param>
/// <param name="attributes">Indexed column names.</param>
/// <param name="indexKind">The index kind (for example BTREE or HNSW).</param>
public class CreateIndexModel(string indexName, string tableName, List<string> attributes, string indexKind)
{
    /// <summary>
    /// Gets or sets the index name.
    /// </summary>
    public string IndexName { get; set; } = indexName;

    /// <summary>
    /// Gets or sets the table name.
    /// </summary>
    public string TableName { get; set; } = tableName;

    /// <summary>
    /// Gets or sets indexed attribute names.
    /// </summary>
    public List<string> Attributes { get; set; } = attributes;

    /// <summary>
    /// Gets or sets the index kind.
    /// </summary>
    public string IndexKind { get; set; } = indexKind;


    /// <summary>
    /// Builds a model from CREATE INDEX AST.
    /// </summary>
    /// <param name="ast">The parsed CREATE INDEX statement.</param>
    /// <returns>The normalized create-index model.</returns>
    public static CreateIndexModel FromAst(CreateIndexStatement ast) => new(
        ast.IndexName.Name,
        ast.TableName.Name,
        ast.ColumnNames.Select(c => c.Name).ToList(),
        ast.UsingMethod?.Name.ToUpperInvariant() ?? "BTREE");

    /// <summary>
    /// Converts the model into catalog index metadata.
    /// </summary>
    /// <returns>The catalog index file definition.</returns>
    public IndexFile ToIndexFile() =>
        new()
        {
            IndexFileName = IndexName,
            AttributeNames = Attributes,
            IndexKind = IndexKind,
        };
}