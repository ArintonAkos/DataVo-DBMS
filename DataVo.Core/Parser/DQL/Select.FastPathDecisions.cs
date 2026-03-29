using DataVo.Core.Constants;
using DataVo.Core.Enums;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DQL;

internal partial class Select
{
    private static bool IsAllTrueExpression(ExpressionNode? expression)
    {
        return expression is LiteralNode literal
            && literal.Value?.ToString() == SqlLiterals.TrueExpression;
    }

    private int ResolveVectorPredicateTopK(int totalRows, double estimatedSelectivity)
    {
        if (totalRows <= 0)
        {
            return 0;
        }

        if (!_model.LimitTake.HasValue || _model.LimitTake.Value <= 0)
        {
            int multiplier = Math.Max(1, Engine.Config.VectorPredicateFastPathCandidateMultiplier);
            int cap = Engine.Config.VectorPredicateFastPathMaxTopK > 0
                ? Engine.Config.VectorPredicateFastPathMaxTopK
                : totalRows;

            int estimatedMatches = Math.Max(1, (int)Math.Ceiling(totalRows * ClampDouble(estimatedSelectivity, 0.01d, 1d)));
            int adaptiveTopK = Math.Max(64, estimatedMatches * multiplier);
            return Math.Min(totalRows, Math.Min(adaptiveTopK, cap));
        }

        int requested = _model.LimitTake.Value + (_model.LimitSkip ?? 0);
        if (requested <= 0)
        {
            return totalRows;
        }

        return Math.Min(totalRows, requested);
    }

    private bool ShouldUseVectorPredicateFastPath(int totalRows, int topK, double estimatedSelectivity, ExpressionNode? whereExpression, out string reason)
    {
        reason = "accepted";

        if (!Engine.Config.EnableVectorPredicateFastPath)
        {
            reason = "disabled by config";
            return false;
        }

        if (totalRows < Engine.Config.VectorPredicateFastPathMinRows)
        {
            reason = $"table too small ({totalRows} < {Engine.Config.VectorPredicateFastPathMinRows})";
            return false;
        }

        if (topK <= 0 || topK >= totalRows)
        {
            reason = $"invalid candidate size ({topK} of {totalRows})";
            return false;
        }

        double ratio = (double)topK / totalRows;
        if (ratio > Engine.Config.VectorPredicateFastPathMaxTopKRatio)
        {
            reason = $"candidate ratio too high ({ratio:F3} > {Engine.Config.VectorPredicateFastPathMaxTopKRatio:F3})";
            return false;
        }

        if (whereExpression != null && ContainsSubqueryExpression(whereExpression))
        {
            reason = "where contains subquery";
            return false;
        }

        if (estimatedSelectivity >= 0.85d)
        {
            reason = $"low expected gain (selectivity {estimatedSelectivity:F3})";
            return false;
        }

        return true;
    }

    private int ResolveHybridOrderByInitialTopK(int totalRows, int requestedTopK, ExpressionNode? seedPredicate, out string sizingMode)
    {
        sizingMode = "baseline";
        if (requestedTopK <= 0)
        {
            return 0;
        }

        if (seedPredicate == null)
        {
            return Math.Min(totalRows, requestedTopK);
        }

        if (!Engine.Config.EnableHybridOrderByAdaptiveInitialTopK)
        {
            return Math.Min(totalRows, requestedTopK);
        }

        double selectivity = ClampDouble(EstimatePredicateSelectivity(seedPredicate), 0.01d, 1d);
        if (selectivity >= 0.95d)
        {
            return Math.Min(totalRows, requestedTopK);
        }

        int capByConfig = Engine.Config.VectorPredicateFastPathMaxTopK > 0
            ? Engine.Config.VectorPredicateFastPathMaxTopK
            : totalRows;

        int capByRatio = (int)Math.Ceiling(totalRows * ClampDouble(Engine.Config.VectorPredicateFastPathMaxTopKRatio, 0.01d, 1d));
        int effectiveCap = Math.Min(totalRows, Math.Max(requestedTopK, Math.Min(capByConfig, capByRatio)));

        int estimatedRequired = Math.Max(requestedTopK, (int)Math.Ceiling(requestedTopK / selectivity));
        int initialTopK = Math.Min(effectiveCap, estimatedRequired);
        if (initialTopK > requestedTopK)
        {
            sizingMode = "adaptive";
        }

        return Math.Max(1, initialTopK);
    }

    private static double EstimateVectorDistanceSelectivity(string distanceOperator, string comparisonOperator, double threshold)
    {
        bool lessStyle = comparisonOperator == Operators.LESS_THAN || comparisonOperator == Operators.LESS_THAN_OR_EQUAL_TO;

        double baseSelectivity = distanceOperator switch
        {
            Operators.VECTOR_DISTANCE_COSINE => threshold switch
            {
                <= 0.10d => 0.02d,
                <= 0.20d => 0.05d,
                <= 0.40d => 0.15d,
                <= 0.80d => 0.40d,
                _ => 0.70d
            },
            Operators.VECTOR_DISTANCE_L2 => threshold switch
            {
                <= 0.25d => 0.05d,
                <= 0.50d => 0.10d,
                <= 1.00d => 0.20d,
                <= 2.00d => 0.45d,
                _ => 0.75d
            },
            _ => 0.50d
        };

        double selectivity = lessStyle ? baseSelectivity : (1d - baseSelectivity);
        return ClampDouble(selectivity, 0.01d, 0.99d);
    }

    private static int ClampInt(int value, int minValue, int maxValue)
    {
        if (value < minValue)
        {
            return minValue;
        }

        if (value > maxValue)
        {
            return maxValue;
        }

        return value;
    }

    private static double ClampDouble(double value, double minValue, double maxValue)
    {
        if (value < minValue)
        {
            return minValue;
        }

        if (value > maxValue)
        {
            return maxValue;
        }

        return value;
    }

    private bool ShouldUseVectorFastPath(int topK, ExpressionNode? whereExpression, out string reason)
    {
        reason = "accepted";

        int totalRows = _model.FromTable?.TableContentValues?.Count ?? 0;
        if (totalRows <= 0)
        {
            reason = "empty_input";
            return false;
        }

        if (topK >= totalRows)
        {
            reason = "topk_ge_total_rows";
            return false;
        }

        if (whereExpression == null || IsAllTrueExpression(whereExpression))
        {
            return true;
        }

        int complexity = EstimatePredicateComplexity(whereExpression);
        int threshold = Math.Max(32, totalRows / 3);
        bool accepted = topK <= threshold || complexity <= 6;
        if (!accepted)
        {
            reason = "complexity_gate";
        }

        return accepted;
    }
}
