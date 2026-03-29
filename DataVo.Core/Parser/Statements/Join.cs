using DataVo.Core.Models.Statement;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Statements.JoinStrategies;
using DataVo.Core.Services;
using DataVo.Core.Enums;
using DataVo.Core.Models.Statement.Utils;
using static DataVo.Core.Models.Statement.JoinModel;

namespace DataVo.Core.Parser.Statements;

/// <summary>
/// Coordinates join evaluation across configured join conditions and strategies.
/// </summary>
public class Join
{
    private readonly bool _containsJoin;
    private readonly JoinStrategyContext _strategyContext;
    private readonly Dictionary<string, IJoinStrategy> _strategies;

    /// <summary>
    /// Gets the join model backing this evaluator.
    /// </summary>
    public readonly JoinModel Model;

    /// <summary>
    /// Initializes a join evaluator for the provided model and table service.
    /// </summary>
    /// <param name="model">The join model definition.</param>
    /// <param name="tableService">The table resolver used by strategies.</param>
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

    /// <summary>
    /// Indicates whether the model includes joined tables.
    /// </summary>
    /// <returns><see langword="true"/> when at least one joined table exists.</returns>
    public bool ContainsJoin() => _containsJoin;

    /// <summary>
    /// Executes one join condition over the current hashed rows.
    /// </summary>
    /// <param name="tableRows">Current left-side hashed rows.</param>
    /// <param name="joinCondition">The join condition to apply.</param>
    /// <returns>The joined row set.</returns>
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

    /// <summary>
    /// Evaluates all configured join conditions over an initial row set.
    /// </summary>
    /// <param name="tableRows">Initial table rows.</param>
    /// <param name="baseTableName">Optional explicit base table name.</param>
    /// <returns>The fully joined row set.</returns>
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
            throw new BindingException("JOIN expression must contain at least one table! Cannot deduce table origin.");
        }

        if (tableCount > 1)
        {
            throw new BindingException("Couldn't JOIN already joined tables!");
        }

        string joinFrom = string.IsNullOrEmpty(baseTableName) ? tableRows.First().Value.Keys.First() : baseTableName;
        List<string> joinedTables = [joinFrom];

        List<JoinCondition> remainingConditions = [.. Model.JoinConditions.Select(condition => new JoinCondition(
            new Column(condition.LeftColumn.DatabaseName, condition.LeftColumn.TableName, condition.LeftColumn.ColumnName),
            new Column(condition.RightColumn.DatabaseName, condition.RightColumn.TableName, condition.RightColumn.ColumnName),
            condition.JoinType))];

        while (remainingConditions.Count > 0)
        {
            bool progressed = false;

            for (int i = 0; i < remainingConditions.Count; i++)
            {
                var joinCondition = remainingConditions[i];
                var leftTableName = joinCondition.LeftColumn.TableName;
                var rightTableName = joinCondition.RightColumn.TableName;

                if (!joinedTables.Contains(rightTableName) && joinedTables.Contains(leftTableName))
                {
                    tableRows = PerformJoinCondition(tableRows, joinCondition);
                    joinedTables.Add(rightTableName);
                    remainingConditions.RemoveAt(i);
                    progressed = true;
                    break;
                }

                if (!joinedTables.Contains(leftTableName) && joinedTables.Contains(rightTableName))
                {
                    (joinCondition.LeftColumn, joinCondition.RightColumn) = (joinCondition.RightColumn, joinCondition.LeftColumn);
                    tableRows = PerformJoinCondition(tableRows, joinCondition);
                    joinedTables.Add(leftTableName);
                    remainingConditions.RemoveAt(i);
                    progressed = true;
                    break;
                }
            }

            if (!progressed)
            {
                throw new EvaluationException("Error while joining tables!");
            }
        }

        return tableRows;
    }
}