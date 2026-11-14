using DataVo.Core.StorageEngine;
﻿using DataVo.Core.Models.Statement;
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
    private List<string>? _canonicalLayout;
    private bool _canonicalLayoutResolved;

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

        if (!TryPlanJoinOrder(joinFrom, out List<JoinCondition> steps, out List<string> layout))
        {
            throw new EvaluationException("Error while joining tables!");
        }

        for (int i = 0; i < steps.Count; i++)
        {
            _strategyContext.LeftTableCount = i + 1;
            tableRows = PerformJoinCondition(tableRows, steps[i]);
        }

        return ToCanonicalKeys(tableRows, layout);
    }

    /// <summary>
    /// Orders the join conditions so each step joins one new table to the tables already joined, starting from
    /// <paramref name="startTable"/>. Conditions written the other way round are flipped.
    /// </summary>
    /// <param name="startTable">The table the evaluation starts from.</param>
    /// <param name="steps">The conditions in execution order.</param>
    /// <param name="layout">The tables in the order their row keys appear in a joined row's key.</param>
    /// <returns><see langword="false"/> when some condition can never be connected to the joined tables.</returns>
    private bool TryPlanJoinOrder(string startTable, out List<JoinCondition> steps, out List<string> layout)
    {
        layout = [startTable];
        steps = new List<JoinCondition>(Model.JoinConditions.Count);

        List<JoinCondition> remaining = [.. Model.JoinConditions.Select(condition => new JoinCondition(
            new Column(condition.LeftColumn.DatabaseName, condition.LeftColumn.TableName, condition.LeftColumn.ColumnName),
            new Column(condition.RightColumn.DatabaseName, condition.RightColumn.TableName, condition.RightColumn.ColumnName),
            condition.JoinType))];

        while (remaining.Count > 0)
        {
            int next = -1;
            for (int i = 0; i < remaining.Count && next < 0; i++)
            {
                JoinCondition condition = remaining[i];
                string leftTable = condition.LeftColumn.TableName;
                string rightTable = condition.RightColumn.TableName;

                if (!layout.Contains(rightTable) && layout.Contains(leftTable))
                {
                    next = i;
                }
                else if (!layout.Contains(leftTable) && layout.Contains(rightTable))
                {
                    (condition.LeftColumn, condition.RightColumn) = (condition.RightColumn, condition.LeftColumn);
                    next = i;
                }
            }

            if (next < 0)
            {
                return false;
            }

            steps.Add(remaining[next]);
            layout.Add(remaining[next].RightColumn.TableName);
            remaining.RemoveAt(next);
        }

        return true;
    }

    /// <summary>
    /// Rewrites row keys from the join order of this evaluation into the canonical table order, the order an
    /// evaluation starting from the FROM table produces. WHERE evaluation starts from whichever table a
    /// predicate filters and combines the partial results with AND/OR on their keys, so the keys must not
    /// depend on the starting table.
    /// </summary>
    /// <param name="rows">The joined rows, keyed in <paramref name="layout"/> order.</param>
    /// <param name="layout">The table order of the keys in <paramref name="rows"/>.</param>
    /// <returns>The same rows keyed in canonical order.</returns>
    private HashedTable ToCanonicalKeys(HashedTable rows, List<string> layout)
    {
        List<string>? canonical = CanonicalLayout;
        if (canonical == null || canonical.Count != layout.Count || rows.Count == 0)
        {
            return rows;
        }

        int[] source = new int[canonical.Count];
        bool identity = true;
        for (int i = 0; i < canonical.Count; i++)
        {
            source[i] = layout.IndexOf(canonical[i]);
            if (source[i] < 0)
            {
                return rows;
            }

            identity &= source[i] == i;
        }

        if (identity)
        {
            return rows;
        }

        HashedTable result = [];
        foreach (KeyValuePair<JoinedRowId, JoinedRow> entry in rows)
        {
            IReadOnlyList<RowKey> ids = entry.Key.RowIds;
            if (ids.Count != source.Length)
            {
                result.Add(entry.Key, entry.Value);
                continue;
            }

            var reordered = new List<RowKey>(source.Length);
            foreach (int position in source)
            {
                reordered.Add(ids[position]);
            }

            result.Add(new JoinedRowId(reordered), entry.Value);
        }

        return result;
    }

    /// <summary>
    /// Gets the key layout of an evaluation that starts from the FROM table (the one condition table that is
    /// not a joined table), or <see langword="null"/> when it cannot be determined.
    /// </summary>
    private List<string>? CanonicalLayout
    {
        get
        {
            if (!_canonicalLayoutResolved)
            {
                _canonicalLayoutResolved = true;
                HashSet<string> joined = [.. Model.JoinTableDetails.Values.Select(detail => detail.TableName)];
                string? from = Model.JoinConditions
                    .SelectMany(condition => new[] { condition.LeftColumn.TableName, condition.RightColumn.TableName })
                    .FirstOrDefault(table => !joined.Contains(table));

                if (from != null && TryPlanJoinOrder(from, out _, out List<string> layout))
                {
                    _canonicalLayout = layout;
                }
            }

            return _canonicalLayout;
        }
    }
}