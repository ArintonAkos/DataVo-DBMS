using System.Text.RegularExpressions;
using MongoDB.Bson.Serialization.IdGenerators;
using Server.Models.Statement;
using Server.Parser.Types;
using Server.Services;
using Server.Utils;
using static Server.Models.Statement.JoinModel;

namespace Server.Parser.Statements;

public class Join
{
    private readonly bool _isValid;
    private readonly TableService? _tableService;
    public readonly JoinModel Model;

    public Join(Group group, TableService tableService)
    {
        if (group.Success && group.Length > 0)
        {
            Model = FromMatchGroup(group, tableService);
            
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

    public HashedTable PerformJoinCondition(HashedTable tableRows, JoinCondition joinCondition)
    {
        var leftTable = joinCondition.LeftColumn.TableName;
        var leftColumn = joinCondition.LeftColumn.ColumnName;
        var rightTable = joinCondition.RightColumn.TableName;
        var rightColumn = joinCondition.RightColumn.ColumnName;

        Dictionary<string, Dictionary<string, dynamic>> rightTableData = _tableService!.GetTableDetailByAliasOrName(rightTable).TableContent!;

        HashedTable result = new();

        bool insertHashAfter = false;
        List<string> joinTables = Model.JoinTableDetails.Values.Select(jtd => jtd.TableName).ToList();

        if (joinTables.IndexOf(leftTable) < joinTables.IndexOf(rightTable))
        {
            insertHashAfter = true;
        }

        foreach (var leftTableRow in tableRows)
        {
            if (leftTableRow.Value.ContainsKey(leftTable) && leftTableRow.Value[leftTable].ContainsKey(leftColumn))
            {
                var leftValue = leftTableRow.Value[leftTable][leftColumn];

                foreach (var rightTableRow in rightTableData)
                {
                    if (rightTableRow.Value.ContainsKey(rightColumn) && rightTableRow.Value[rightColumn] == leftValue)
                    {
                        var joinedRow = new JoinedRow();
                        string hash = string.Empty;

                        if (insertHashAfter)
                        {
                            hash = $"{leftTableRow.Key}##{rightTableRow.Key}";
                        }
                        else
                        {
                            hash = $"{rightTableRow.Key}##{leftTableRow.Key}";
                        }

                        joinedRow.Add(leftTable, leftTableRow.Value[leftTable]);
                        joinedRow.Add(rightTable, rightTableRow.Value.ToRow());

                        result.Add(hash, joinedRow);
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

    public HashedTable Evaluate(HashedTable tableRows)
    {
        // Ha ures a tabla, akkor a JOIN eredmenye ugyis ures marad (Mivel INNER JOIN)
        if (tableRows.Count == 0)
        {
            return new();
        }

        int tableCount = tableRows.First().Value.Keys.Count();

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

        string joinFrom = tableRows.First().Value.Keys.First();
        List<string> joinedTables = new() { joinFrom };

        foreach (var joinCondition in sortedJoinConditions)
        {
            var leftTableName = joinCondition.LeftColumn.TableName;
            var rightTableName = joinCondition.RightColumn.TableName;

            if (!joinedTables.Contains(rightTableName) && joinedTables.Contains(leftTableName))
            {
                tableRows = PerformJoinCondition(tableRows, joinCondition);
                joinedTables.Add(rightTableName);
            }
            else if(!joinedTables.Contains(leftTableName) && joinedTables.Contains(rightTableName))
            {
                (joinCondition.LeftColumn, joinCondition.RightColumn) = (joinCondition.RightColumn, joinCondition.LeftColumn);
                tableRows = PerformJoinCondition(tableRows, joinCondition);
                joinedTables.Add(leftTableName);
            }
            else if (!joinedTables.Contains(leftTableName) && !joinedTables.Contains(rightTableName))
            {
                throw new Exception("Error while joining tables!");
            }
        }

        return tableRows;
    }
}