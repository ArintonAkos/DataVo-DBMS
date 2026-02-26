using DataVo.Core.Models.Statement;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

public class JoinStrategyContext
{
    public TableService TableService { get; set; } = null!;
    public JoinModel JoinModel { get; set; } = null!;

    public Dictionary<string, Dictionary<string, dynamic>> GetTableData(string tableAliasOrName)
    {
        var tableDetail = TableService.GetTableDetailByAliasOrName(tableAliasOrName);

        if (tableDetail.TableContent == null)
        {
            throw new EvaluationException($"Join Strategy Error: table '{tableAliasOrName}' has no content loaded.");
        }

        return tableDetail.TableContent;
    }

    public string BuildHash(string leftRowKey, string rightRowKey, bool insertHashAfter)
    {
        return insertHashAfter
            ? $"{leftRowKey}##{rightRowKey}"
            : $"{rightRowKey}##{leftRowKey}";
    }

    public JoinedRow CreateJoinedRow(JoinedRow existingLeftRow, string rightTable, Row rightRow)
    {
        var dict = new Dictionary<string, Row>();
        foreach (var key in existingLeftRow.Keys)
        {
            dict[key] = existingLeftRow[key];
        }
        dict[rightTable] = rightRow;
        
        return new JoinedRow(dict);
    }

    public JoinedRow CreateNullRightRow(JoinedRow existingLeftRow, string rightTable)
    {
        var dict = new Dictionary<string, Row>();
        foreach (var key in existingLeftRow.Keys)
        {
            dict[key] = existingLeftRow[key];
        }
        dict[rightTable] = TableService.GetNullRowForTable(rightTable);
        
        return new JoinedRow(dict);
    }
}