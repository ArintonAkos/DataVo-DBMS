using System.Text.RegularExpressions;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DML;

internal class DeleteFromModel
{
    public string TableName { get; set; }
    public Where WhereStatement { get; set; }

    public static DeleteFromModel FromMatch(Match match)
    {
        string tableName = match.Groups["TableName"].Value;
        var whereStatement = new Where(match.Groups["WhereStatement"].Value);

        return new DeleteFromModel()
        {
            TableName = tableName,
            WhereStatement = whereStatement,
        };
    }

    public static DeleteFromModel FromAst(DeleteFromStatement ast)
    {
        return new DeleteFromModel()
        {
            TableName = ast.TableName.Name,
            WhereStatement = ast.WhereExpression != null ? new Where(ast.WhereExpression, null!) : new Where("")
        };
    }
}