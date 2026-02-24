using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;
using DataVo.Core.Utils;
using static DataVo.Core.Models.Statement.JoinModel;

namespace DataVo.Core.Parser.Statements;

public class Join
{
    private readonly bool _isValid;
    private readonly TableService? _tableService;
    public readonly JoinModel Model;

    public Join(JoinModel model, TableService tableService)
    {
        Model = model;
        _tableService = tableService;
        _isValid = model.JoinConditions.Count > 0 || model.JoinTableDetails.Count > 0;
    }

    public bool ContainsJoin() => _isValid;

    public HashedTable PerformJoinCondition(HashedTable tableRows, JoinCondition joinCondition)
    {
        var leftTable = joinCondition.LeftColumn.TableName;
        var leftColumn = joinCondition.LeftColumn.ColumnName;
        var rightTable = joinCondition.RightColumn.TableName;
        var rightColumn = joinCondition.RightColumn.ColumnName;

        Dictionary<string, Dictionary<string, dynamic>> rightTableData = _tableService!.GetTableDetailByAliasOrName(rightTable).TableContent!;

        HashedTable result = [];

        bool insertHashAfter = false;
        List<string> joinTables = Model.JoinTableDetails.Values.Select(jtd => jtd.TableName).ToList();

        if (joinTables.IndexOf(leftTable) < joinTables.IndexOf(rightTable))
        {
            insertHashAfter = true;
        }

        if (rightTableData.Count >= 32)
        {
            var rightLookup = BuildRightLookup(rightTableData, rightColumn);

            foreach (var leftTableRow in tableRows)
            {
                if (!leftTableRow.Value.ContainsKey(leftTable) || !leftTableRow.Value[leftTable].ContainsKey(leftColumn))
                {
                    throw new Exception("Easter egg ha ide bejon!");
                }

                var leftValue = leftTableRow.Value[leftTable][leftColumn];
                if (!rightLookup.TryGetValue(leftValue, out KeyValuePair<string, Dictionary<string, dynamic>> rightTableRow))
                {
                    continue;
                }

                var joinedRow = new JoinedRow();
                string hash = insertHashAfter
                    ? $"{leftTableRow.Key}##{rightTableRow.Key}"
                    : $"{rightTableRow.Key}##{leftTableRow.Key}";

                joinedRow.Add(leftTable, leftTableRow.Value[leftTable]);
                joinedRow.Add(rightTable, rightTableRow.Value.ToRow());

                result.Add(hash, joinedRow);
            }
        }
        else
        {
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
                            string hash = insertHashAfter
                                ? $"{leftTableRow.Key}##{rightTableRow.Key}"
                                : $"{rightTableRow.Key}##{leftTableRow.Key}";

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
        }

        return result;
    }

    private static Dictionary<dynamic, KeyValuePair<string, Dictionary<string, dynamic>>> BuildRightLookup(
        Dictionary<string, Dictionary<string, dynamic>> rightTableData,
        string rightColumn)
    {
        Dictionary<dynamic, KeyValuePair<string, Dictionary<string, dynamic>>> lookup = [];

        foreach (var rightTableRow in rightTableData)
        {
            if (!rightTableRow.Value.ContainsKey(rightColumn))
            {
                continue;
            }

            dynamic key = rightTableRow.Value[rightColumn];
            if (!lookup.ContainsKey(key))
            {
                lookup[key] = rightTableRow;
            }
        }

        return lookup;
    }

    public HashedTable Evaluate(HashedTable tableRows)
    {
        // Ha ures a tabla, akkor a JOIN eredmenye ugyis ures marad (Mivel INNER JOIN)
        if (tableRows.Count == 0)
        {
            return [];
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
        List<string> joinedTables = [joinFrom];

        foreach (var joinCondition in sortedJoinConditions)
        {
            var leftTableName = joinCondition.LeftColumn.TableName;
            var rightTableName = joinCondition.RightColumn.TableName;

            if (!joinedTables.Contains(rightTableName) && joinedTables.Contains(leftTableName))
            {
                tableRows = PerformJoinCondition(tableRows, joinCondition);
                joinedTables.Add(rightTableName);
            }
            else if (!joinedTables.Contains(leftTableName) && joinedTables.Contains(rightTableName))
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