using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

internal class DropDatabaseModel(string databaseName)
{
    public string DatabaseName { get; set; } = databaseName;

    public static DropDatabaseModel FromAst(DropDatabaseStatement ast) => new(ast.DatabaseName.Name);
}