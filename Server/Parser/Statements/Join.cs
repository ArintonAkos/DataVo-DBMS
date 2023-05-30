using System.Text.RegularExpressions;
using Server.Models.Catalog;
using Server.Models.Statement;
using Server.Server.MongoDB;
using Server.Services;
using static Server.Models.Statement.JoinModel;

namespace Server.Parser.Statements;

public class Join
{
    private readonly bool _isValid;
    private readonly TableService? _tableService;
    public readonly JoinModel Model;

    public Join(Group group, TableService tableService)
    {
        if (group.Success)
        {
            Model = JoinModel.FromMatchGroup(group, tableService);
            
            _tableService = tableService;
            _isValid = true;
        }
        else
        {
            _isValid = false;
            _tableService = null;
            Model = new();
        }
    }

    public bool ContainsJoin() => _isValid;

    public List<Dictionary<string, Dictionary<string, dynamic>>> PerformJoinCondition(List<Dictionary<string, Dictionary<string, dynamic>>> tableRows, JoinCondition joinCondition)
    {
        var leftTable = joinCondition.LeftColumn.TableName;
        var leftColumn = joinCondition.LeftColumn.ColumnName;
        var rightTable = joinCondition.RightColumn.TableName;
        var rightColumn = joinCondition.RightColumn.ColumnName;

        List<Dictionary<string, dynamic>> rightTableData = _tableService!.GetTableDetailByAliasOrName(rightTable).TableContent!.Select(c => c.Value).ToList();

        List<Dictionary<string, Dictionary<string, dynamic>>> result = new();

        foreach (var leftTableRow in tableRows)
        {
            if (leftTableRow.ContainsKey(leftTable) && leftTableRow[leftTable].ContainsKey(leftColumn))
            {
                var leftValue = leftTableRow[leftTable][leftColumn];

                foreach (var rightTableRow in rightTableData)
                {
                    if (rightTableRow.ContainsKey(rightColumn) && rightTableRow[rightColumn] == leftValue)
                    {
                        result.Add(new Dictionary<string, Dictionary<string, dynamic>>
                        {
                            { leftTable, leftTableRow[leftTable] },
                            { rightTable, rightTableRow }
                        });

                        break;
                    }
                }
            }
            else
            {
                throw new Exception("Easter egg ha ide bejon!");
            }
        }

        return result;
    }

    // Lista<Táblanév, <Oszlopnév, érték>>
    public List<Dictionary<string, Dictionary<string, dynamic>>> Evaluate(List<Dictionary<string, Dictionary<string, dynamic>>> tableRows)
    {
        // Ha ures a tabla, akkor a JOIN eredmenye ugyis ures marad (Mivel INNER JOIN)
        if (tableRows.Count == 0)
        {
            return new();
        }

        int tableCount = tableRows[0].Keys.Count;

        if (tableCount == 0)
        {
            throw new Exception("JOIN expression must contain at least one table!");
        }

        if (tableCount != 1)
        {
            throw new Exception("Couldn't JOIN already joined tables!");
        }

        TopologicalSort sorter = new();

        foreach (var condition in Model.JoinConditions)
        {
            sorter.AddEdge(condition.LeftColumn, condition.RightColumn);
        }

        sorter.Sort();
        List<string> sortedTableNames = sorter.GetSorted().Select(jc => jc.TableName).ToList();

        List<JoinCondition> sortedJoinConditions = Model.JoinConditions
            .Where(jc => sortedTableNames.IndexOf(jc.LeftColumn.TableName) < sortedTableNames.IndexOf(jc.RightColumn.TableName))
            .ToList();

        string joinFrom = tableRows[0].Keys.First();
        List<string> joinedTables = new() { joinFrom };

        foreach (var joinCondition in sortedJoinConditions)
        {
            var currentTable = joinCondition.RightColumn.TableName;

            if (!joinedTables.Contains(currentTable))
            {
                tableRows = PerformJoinCondition(tableRows, joinCondition);
                joinedTables.Add(currentTable);

                // Na gata
            }
        }

        return tableRows;
    }
}