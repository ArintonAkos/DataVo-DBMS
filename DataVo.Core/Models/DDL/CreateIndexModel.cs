using System.Text.RegularExpressions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

public class CreateIndexModel(string indexName, string tableName, List<string> attributes, string indexKind)
{
    public string IndexName { get; set; } = indexName;
    public string TableName { get; set; } = tableName;
    public List<string> Attributes { get; set; } = attributes;
    public string IndexKind { get; set; } = indexKind;


    public static CreateIndexModel FromAst(CreateIndexStatement ast) => new(
        ast.IndexName.Name,
        ast.TableName.Name,
        [ast.ColumnName.Name],
        ast.UsingMethod?.Name.ToUpperInvariant() ?? "BTREE");

    public IndexFile ToIndexFile() =>
        new()
        {
            IndexFileName = IndexName,
            AttributeNames = Attributes,
            IndexKind = IndexKind,
        };
}