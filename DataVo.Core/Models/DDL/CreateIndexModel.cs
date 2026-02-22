using System.Text.RegularExpressions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

public class CreateIndexModel(string indexName, string tableName, List<string> attributes)
{
    public string IndexName { get; set; } = indexName;
    public string TableName { get; set; } = tableName;
    public List<string> Attributes { get; set; } = attributes;

    public static CreateIndexModel FromMatch(Match match)
    {
        string indexName = match.Groups["IndexName"].Value;
        string tableName = match.Groups["TableName"].Value;
        List<string> attributes = [];

        foreach (Capture column in match.Groups["Column"].Captures)
        {
            attributes.Add(column.Value);
        }

        return new CreateIndexModel(indexName, tableName, attributes);
    }

    public static CreateIndexModel FromAst(CreateIndexStatement ast) => new(ast.IndexName.Name, ast.TableName.Name, [ast.ColumnName.Name]);

    public IndexFile ToIndexFile() =>
        new()
        {
            IndexFileName = IndexName,
            AttributeNames = Attributes,
        };
}