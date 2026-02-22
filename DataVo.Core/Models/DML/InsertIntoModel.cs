using System.Text.RegularExpressions;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DML;

internal class InsertIntoModel
{
    public InsertIntoModel(string tableName, List<Dictionary<string, string>> rows, List<string> columns)
    {
        TableName = tableName;
        Rows = rows;
        Columns = columns;
    }

    public string TableName { get; set; }
    public List<Dictionary<string, string>> Rows { get; set; }
    public List<string> Columns { get; set; }

    public static InsertIntoModel FromMatch(Match match)
    {
        var columns = match.Groups["Columns"].Value
            .RemoveWhiteSpaces()
            .Split(",")
            .ToList();

        List<Dictionary<string, string>> rows = new();
        foreach (Capture rowCapture in match.Groups["Values"].Captures)
        {
            var row = rowCapture.Value
                .RemoveWhiteSpaces()
                .Split(",")
                .ToList();

            if (row.Count != columns.Count)
            {
                throw new Exception("The number of values provided in a row must be the same as " +
                                    "the number of columns provided inside the paranthesis after the table name attribute.");
            }

            Dictionary<string, string> rowDict = new();
            for (int i = 0; i < row.Count; ++i)
            {
                rowDict.Add(columns[i], row[i]);
            }

            rows.Add(rowDict);
        }

        return new InsertIntoModel(match.Groups["TableName"].Value, rows, columns);
    }

    public static InsertIntoModel FromAst(InsertIntoStatement ast)
    {
        var columns = ast.Columns.Select(c => c.Name).ToList();
        List<Dictionary<string, string>> rows = new();

        foreach (var rowAst in ast.ValuesLists)
        {
            if (rowAst.Count != columns.Count)
            {
                throw new Exception("The number of values provided in a row must be the same as " +
                                    "the number of columns provided inside the paranthesis after the table name attribute.");
            }

            Dictionary<string, string> rowDict = new();
            for (int i = 0; i < rowAst.Count; ++i)
            {
                rowDict.Add(columns[i], ((IdentifierNode)rowAst[i]).Name);
            }
            rows.Add(rowDict);
        }

        return new InsertIntoModel(ast.TableName.Name, rows, columns);
    }
}