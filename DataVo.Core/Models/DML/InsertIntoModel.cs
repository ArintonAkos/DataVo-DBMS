using System.Text.RegularExpressions;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DML;

internal class InsertIntoModel(string tableName, List<List<string>> rawRows, List<string> columns)
{
    public string TableName { get; set; } = tableName;
    public List<List<string>> RawRows { get; set; } = rawRows;
    public List<string> Columns { get; set; } = columns;



    public static InsertIntoModel FromAst(InsertIntoStatement ast)
    {
        var columns = ast.Columns.Select(c => c.Name).ToList();
        List<List<string>> rows = [];

        foreach (var rowAst in ast.ValuesLists)
        {
            if (columns.Count > 0 && rowAst.Count != columns.Count)
            {
                throw new Exception("The number of values provided in a row must be the same as " +
                                    "the number of columns provided inside the paranthesis after the table name attribute.");
            }

            List<string> rowList = [];
            for (int i = 0; i < rowAst.Count; ++i)
            {
                rowList.Add(((IdentifierNode)rowAst[i]).Name);
            }
            rows.Add(rowList);
        }

        return new InsertIntoModel(ast.TableName.Name, rows, columns);
    }
}