using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

internal class DropTableModel(string databaseName)
{
    public string TableName { get; set; } = databaseName;

    public static DropTableModel FromAst(DropTableStatement ast) => new(ast.TableName.Name);
}