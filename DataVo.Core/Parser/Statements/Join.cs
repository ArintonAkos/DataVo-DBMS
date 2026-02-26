using DataVo.Core.Models.Statement;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Statements.JoinStrategies;
using DataVo.Core.Services;
using DataVo.Core.Enums;
using static DataVo.Core.Models.Statement.JoinModel;

namespace DataVo.Core.Parser.Statements;

public class Join
{
    private readonly bool _containsJoin;
    private readonly JoinStrategyContext _strategyContext;
    private readonly Dictionary<string, IJoinStrategy> _strategies;
    public readonly JoinModel Model;

    public Join(JoinModel model, TableService tableService)
    {
        Model = model;
        _strategyContext = new JoinStrategyContext
        {
            TableService = tableService,
            JoinModel = model
        };

        _strategies = new Dictionary<string, IJoinStrategy>(StringComparer.OrdinalIgnoreCase)
        {
            [JoinTypes.INNER] = new InnerJoinStrategy(),
            [JoinTypes.LEFT] = new LeftJoinStrategy(),
            [JoinTypes.RIGHT] = new RightJoinStrategy(),
            [JoinTypes.FULL] = new FullJoinStrategy(),
            [JoinTypes.CROSS] = new CrossJoinStrategy()
        };

        // Canonical signal for JOIN presence is joined table registration.
        // This also correctly covers CROSS JOIN (no ON condition -> no JoinConditions).
        _containsJoin = model.JoinTableDetails.Count > 0;
    }

    public bool ContainsJoin() => _containsJoin;

    public HashedTable PerformJoinCondition(HashedTable tableRows, JoinCondition joinCondition)
    {
        IJoinStrategy strategy = ResolveStrategy(joinCondition.JoinType);
        return strategy.Execute(tableRows, joinCondition, _strategyContext);
    }

    private IJoinStrategy ResolveStrategy(string joinType)
    {
        if (_strategies.TryGetValue(joinType, out IJoinStrategy? strategy))
        {
            return strategy;
        }

        throw new EvaluationException($"Unsupported JOIN type '{joinType}'.");
    }

    public HashedTable Evaluate(HashedTable tableRows, string baseTableName = "")
    {
        int tableCount;
        
        if (tableRows.Count > 0)
        {
            tableCount = tableRows.First().Value.Keys.Count();
        }
        else
        {
            tableCount = string.IsNullOrEmpty(baseTableName) ? 0 : 1;
        }

        if (tableCount == 0)
        {
            throw new Exception("JOIN expression must contain at least one table! Cannot deduce table origin.");
        }

        if (tableCount > 1)
        {
            throw new Exception("Couldn't JOIN already joined tables!");
        }

        TopologicalSort sorter = new();

        foreach (var condition in Model.JoinConditions)
        {
            sorter.AddEdge(condition.LeftColumn, condition.RightColumn);
        }

        sorter.Sort();
        List<string> sortedTableNames = [.. sorter.GetSorted().Select(jc => jc.TableName)];

        List<JoinCondition> sortedJoinConditions = [.. Model.JoinConditions.Where(jc => sortedTableNames.IndexOf(jc.LeftColumn.TableName) < sortedTableNames.IndexOf(jc.RightColumn.TableName))];

        string joinFrom = string.IsNullOrEmpty(baseTableName) ? tableRows.First().Value.Keys.First() : baseTableName;
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