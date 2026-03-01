using DataVo.Core.Models.Statement;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;

using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

public class JoinStrategyContext
{
    public TableService TableService { get; set; } = null!;
    public JoinModel JoinModel { get; set; } = null!;

    public TableData GetTableData(string tableAliasOrName)
    {
        var tableDetail = TableService.GetTableDetailByAliasOrName(tableAliasOrName);

        if (tableDetail.TableContent == null)
        {
            throw new EvaluationException($"Join Strategy Error: table '{tableAliasOrName}' has no content loaded.");
        }

        return tableDetail.TableContent;
    }

    public JoinedRowId BuildHash(JoinedRowId? leftRowKey, long? rightRowKey, bool insertHashAfter)
    {
        long rKey = rightRowKey ?? long.MinValue;
        
        if (leftRowKey == null)
        {
            return insertHashAfter 
                ? new JoinedRowId(long.MinValue, rKey) 
                : new JoinedRowId(rKey, long.MinValue);
        }
        
        return insertHashAfter
            ? leftRowKey.Append(rKey)
            : leftRowKey.Prepend(rKey);
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