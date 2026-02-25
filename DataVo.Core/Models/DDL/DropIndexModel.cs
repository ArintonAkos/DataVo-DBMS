using System.Text.RegularExpressions;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

public class DropIndexModel(string indexName, string tableName)
{
    public string TableName { get; set; } = tableName;
    public string IndexName { get; set; } = indexName;

    public static DropIndexModel FromAst(DropIndexStatement ast) => new(ast.IndexName.Name, ast.TableName.Name);
}