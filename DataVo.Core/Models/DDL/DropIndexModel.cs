using System.Text.RegularExpressions;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

public class DropIndexModel
{
    public DropIndexModel(string indexName, string tableName)
    {
        TableName = tableName;
        IndexName = indexName;
    }

    public string TableName { get; set; }
    public string IndexName { get; set; }

    public static DropIndexModel FromMatch(Match match) => new(match.NthGroup(n: 1).Value, match.NthGroup(n: 2).Value);
    public static DropIndexModel FromAst(DropIndexStatement ast) => new(ast.IndexName.Name, ast.TableName.Name);
}