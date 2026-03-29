using DataVo.Core.Constants;
using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DQL;

internal partial class Select
{
    private enum LogicalPlanKind
    {
        LegacyWhereJoin,
        LegacyWhereExpression,
        LegacyJoinOnly,
        LegacyNoJoinScan,
        VolcanoNoJoin,
        VolcanoInnerJoin
    }

    private sealed class PhysicalPlanDecision
    {
        public PhysicalPlanDecision(LogicalPlanKind logicalPlan, bool useVolcano, int estimatedCost, string reason)
        {
            LogicalPlan = logicalPlan;
            UseVolcano = useVolcano;
            EstimatedCost = estimatedCost;
            Reason = reason;
        }

        public LogicalPlanKind LogicalPlan { get; }
        public bool UseVolcano { get; }
        public int EstimatedCost { get; }
        public string Reason { get; }
    }

    private PhysicalPlanDecision BuildPhysicalPlan(ExpressionNode? whereExpression)
    {
        if (whereExpression != null)
        {
            if (ShouldUseVolcanoInnerJoinPath(whereExpression))
            {
                return ChooseCheaperPlan(
                    volcanoPlan: LogicalPlanKind.VolcanoInnerJoin,
                    legacyPlan: LogicalPlanKind.LegacyWhereJoin,
                    whereExpression,
                    volcanoReason: "JOIN graph is fully INNER and connected",
                    legacyReason: "legacy join path estimated cheaper for current predicate");
            }

            if (ShouldUseVolcanoNoJoinPath(whereExpression))
            {
                LogicalPlanKind legacyCandidate = RequiresExpressionEvaluation(whereExpression)
                    ? LogicalPlanKind.LegacyWhereExpression
                    : LogicalPlanKind.LegacyWhereJoin;

                return ChooseCheaperPlan(
                    volcanoPlan: LogicalPlanKind.VolcanoNoJoin,
                    legacyPlan: legacyCandidate,
                    whereExpression,
                    volcanoReason: "single-table filter with no subquery",
                    legacyReason: "legacy filter path estimated cheaper for current predicate");
            }

            if (RequiresExpressionEvaluation(whereExpression))
            {
                return new PhysicalPlanDecision(
                    LogicalPlanKind.LegacyWhereExpression,
                    useVolcano: false,
                    estimatedCost: EstimateCost(LogicalPlanKind.LegacyWhereExpression, whereExpression),
                    reason: "expression predicate requires legacy evaluator");
            }

            return new PhysicalPlanDecision(
                LogicalPlanKind.LegacyWhereJoin,
                useVolcano: false,
                estimatedCost: EstimateCost(LogicalPlanKind.LegacyWhereJoin, whereExpression),
                reason: "fallback where/join evaluation");
        }

        if (_model.JoinStatement.ContainsJoin())
        {
            if (ShouldUseVolcanoInnerJoinPath(null))
            {
                return ChooseCheaperPlan(
                    volcanoPlan: LogicalPlanKind.VolcanoInnerJoin,
                    legacyPlan: LogicalPlanKind.LegacyJoinOnly,
                    whereExpression: null,
                    volcanoReason: "join-only query has connected INNER JOIN graph",
                    legacyReason: "legacy join-only path estimated cheaper");
            }

            return new PhysicalPlanDecision(
                LogicalPlanKind.LegacyJoinOnly,
                useVolcano: false,
                estimatedCost: EstimateCost(LogicalPlanKind.LegacyJoinOnly, null),
                reason: "join without where uses legacy join strategy");
        }

        if (ShouldUseVolcanoNoJoinPath(null))
        {
            return ChooseCheaperPlan(
                volcanoPlan: LogicalPlanKind.VolcanoNoJoin,
                legacyPlan: LogicalPlanKind.LegacyNoJoinScan,
                whereExpression: null,
                volcanoReason: "simple no-join scan path",
                legacyReason: "legacy no-join scan estimated cheaper");
        }

        return new PhysicalPlanDecision(
            LogicalPlanKind.LegacyNoJoinScan,
            useVolcano: false,
            estimatedCost: EstimateCost(LogicalPlanKind.LegacyNoJoinScan, null),
            reason: "volcano disabled or unsupported");
    }

    private PhysicalPlanDecision ChooseCheaperPlan(
        LogicalPlanKind volcanoPlan,
        LogicalPlanKind legacyPlan,
        ExpressionNode? whereExpression,
        string volcanoReason,
        string legacyReason)
    {
        int volcanoCost = EstimateCost(volcanoPlan, whereExpression);
        int legacyCost = EstimateCost(legacyPlan, whereExpression);

        if (volcanoCost <= legacyCost)
        {
            return new PhysicalPlanDecision(
                volcanoPlan,
                useVolcano: true,
                estimatedCost: volcanoCost,
                reason: $"{volcanoReason}; compared to {legacyPlan} cost {legacyCost}");
        }

        return new PhysicalPlanDecision(
            legacyPlan,
            useVolcano: false,
            estimatedCost: legacyCost,
            reason: $"{legacyReason}; compared to {volcanoPlan} cost {volcanoCost}");
    }

    private int EstimateCost(LogicalPlanKind plan, ExpressionNode? whereExpression)
    {
        int rowCount = plan switch
        {
            LogicalPlanKind.VolcanoInnerJoin or LogicalPlanKind.LegacyWhereJoin or LogicalPlanKind.LegacyJoinOnly => EstimateJoinInputRowCount(),
            _ => _model.FromTable?.TableContentValues?.Count ?? 0
        };

        int complexity = whereExpression == null ? 1 : EstimatePredicateComplexity(whereExpression);
        double selectivity = EstimatePredicateSelectivity(whereExpression);
        int effectiveRows = Math.Max(1, (int)Math.Ceiling(rowCount * selectivity));
        int pipelineFeatures = EstimatePipelineFeatureCost();

        return plan switch
        {
            LogicalPlanKind.VolcanoNoJoin => 8 + complexity + pipelineFeatures + (effectiveRows / 1000),
            LogicalPlanKind.VolcanoInnerJoin => 14 + complexity + pipelineFeatures + (effectiveRows / 750),
            LogicalPlanKind.LegacyWhereExpression => 30 + (2 * complexity) + pipelineFeatures + (effectiveRows / 500),
            LogicalPlanKind.LegacyWhereJoin => 24 + complexity + pipelineFeatures + (effectiveRows / 400),
            LogicalPlanKind.LegacyJoinOnly => 22 + pipelineFeatures + (effectiveRows / 350),
            LogicalPlanKind.LegacyNoJoinScan => 16 + pipelineFeatures + (effectiveRows / 450),
            _ => 100
        };
    }

    private int EstimatePipelineFeatureCost()
    {
        int score = 0;

        if (_model.GetOrderByExpression()?.Columns.Count > 0)
        {
            score += 2;
        }

        if (_model.IsDistinct)
        {
            score += 2;
        }

        if (_model.GroupByStatement.ContainsGroupBy())
        {
            score += 3;
        }

        if (_model.LimitTake.HasValue || (_model.LimitSkip.HasValue && _model.LimitSkip.Value > 0))
        {
            score += 1;
        }

        return score;
    }

    private static double EstimatePredicateSelectivity(ExpressionNode? node)
    {
        if (node == null)
        {
            return 1d;
        }

        if (node is LiteralNode literal && literal.Value?.ToString() == SqlLiterals.TrueExpression)
        {
            return 1d;
        }

        if (node is ScalarFunctionExpressionNode)
        {
            return 0.5d;
        }

        if (node is not BinaryExpressionNode binary)
        {
            return 0.5d;
        }

        if (binary.Operator.Equals(Operators.AND, StringComparison.OrdinalIgnoreCase))
        {
            double left = EstimatePredicateSelectivity(binary.Left);
            double right = EstimatePredicateSelectivity(binary.Right);
            return ClampDouble(left * right, 0.01d, 1d);
        }

        if (binary.Operator.Equals(Operators.OR, StringComparison.OrdinalIgnoreCase))
        {
            double left = EstimatePredicateSelectivity(binary.Left);
            double right = EstimatePredicateSelectivity(binary.Right);
            double combined = left + right - (left * right);
            return ClampDouble(combined, 0.01d, 1d);
        }

        if (binary.Operator.Equals(Operators.EQUALS, StringComparison.OrdinalIgnoreCase))
        {
            return 0.1d;
        }

        if (binary.Operator.Equals(Operators.NOT_EQUALS, StringComparison.OrdinalIgnoreCase))
        {
            return 0.9d;
        }

        if (binary.Operator.Equals(Operators.GREATER_THAN, StringComparison.OrdinalIgnoreCase)
            || binary.Operator.Equals(Operators.GREATER_THAN_OR_EQUAL_TO, StringComparison.OrdinalIgnoreCase)
            || binary.Operator.Equals(Operators.LESS_THAN, StringComparison.OrdinalIgnoreCase)
            || binary.Operator.Equals(Operators.LESS_THAN_OR_EQUAL_TO, StringComparison.OrdinalIgnoreCase))
        {
            return 0.35d;
        }

        if (binary.Operator.Equals(Operators.LIKE, StringComparison.OrdinalIgnoreCase))
        {
            return 0.25d;
        }

        if (binary.Operator.Equals(Operators.IS_NULL, StringComparison.OrdinalIgnoreCase)
            || binary.Operator.Equals(Operators.IS_NOT_NULL, StringComparison.OrdinalIgnoreCase))
        {
            return 0.1d;
        }

        return 0.5d;
    }

    private int EstimateJoinInputRowCount()
    {
        int total = _model.FromTable?.TableContentValues?.Count ?? 0;
        if (_model.TableService == null)
        {
            return total;
        }

        foreach (var detail in _model.JoinStatement.Model.JoinTableDetails.Values)
        {
            total += detail.TableContentValues?.Count ?? 0;
        }

        return total;
    }

    private bool ShouldUseVolcanoInnerJoinPath(ExpressionNode? whereExpression)
    {
        if (!Engine.Config.EnableVolcanoExecution)
        {
            return false;
        }

        if (!_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (whereExpression != null && ContainsSubqueryExpression(whereExpression))
        {
            return false;
        }

        var conditions = _model.JoinStatement.Model.JoinConditions;
        if (conditions.Count == 0)
        {
            return false;
        }

        if (conditions.Any(c => !c.JoinType.Equals(JoinTypes.INNER, StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        string fromTable = _model.FromTable.TableName;
        HashSet<string> reachable = [fromTable];
        int expanded;

        do
        {
            expanded = 0;
            foreach (var condition in conditions)
            {
                bool leftIn = reachable.Contains(condition.LeftColumn.TableName);
                bool rightIn = reachable.Contains(condition.RightColumn.TableName);

                if (leftIn && !rightIn)
                {
                    if (reachable.Add(condition.RightColumn.TableName))
                    {
                        expanded++;
                    }
                }
                else if (rightIn && !leftIn)
                {
                    if (reachable.Add(condition.LeftColumn.TableName))
                    {
                        expanded++;
                    }
                }
            }
        }
        while (expanded > 0);

        return _model.JoinStatement.Model.JoinTableDetails.Values
            .All(detail => reachable.Contains(detail.TableName));
    }

    private bool ShouldUseVolcanoNoJoinPath(ExpressionNode? whereExpression)
    {
        if (!Engine.Config.EnableVolcanoExecution)
        {
            return false;
        }

        if (_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (_model.GetHavingExpression() != null)
        {
            return false;
        }

        if (whereExpression != null && ContainsSubqueryExpression(whereExpression))
        {
            return false;
        }

        return true;
    }

    private static bool ContainsSubqueryExpression(ExpressionNode expression)
    {
        return expression switch
        {
            BinaryExpressionNode binary => ContainsSubqueryExpression(binary.Left)
                || ContainsSubqueryExpression(binary.Right),
            ScalarFunctionExpressionNode scalar => scalar.Arguments.Any(ContainsSubqueryExpression),
            InSubqueryExpressionNode or ExistsSubqueryExpressionNode or ScalarSubqueryExpressionNode => true,
            _ => false,
        };
    }

    private static bool RequiresExpressionEvaluation(ExpressionNode node)
    {
        if (node is BinaryExpressionNode binary)
        {
            if (binary.Operator is "+" or "-" or "*" or "/" or "ADD" or "SUB" or "MUL" or "DIV")
            {
                return true;
            }

            return RequiresExpressionEvaluation(binary.Left) || RequiresExpressionEvaluation(binary.Right);
        }

        if (node is ScalarFunctionExpressionNode)
        {
            return true;
        }

        if (node is AggregateExpressionNode aggregate && aggregate.Argument != null)
        {
            return RequiresExpressionEvaluation(aggregate.Argument);
        }

        return false;
    }
}
