using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Classifies a LINQ query shape as native-translatable, guarded fallback or blocked. A shape it
/// classifies as native must translate in <see cref="DataVoNativeQueryTranslator"/>: both classify
/// expressions with <see cref="DataVoExpressionHelpers"/>.
/// </summary>
internal static class DataVoQueryTranslationAnalyzer
{
    private const string TrackingFallbackReason =
        "Tracking queries (asNoTracking: false) refresh the EF mirror so the returned entities are tracked; native translation runs only for no-tracking queries.";

    private static readonly string[] NativePreviewOperators =
    [
        nameof(Queryable.Where),
        nameof(Queryable.OrderBy),
        nameof(Queryable.OrderByDescending),
        nameof(Queryable.ThenBy),
        nameof(Queryable.ThenByDescending),
        nameof(Queryable.Skip),
        nameof(Queryable.Take),
        nameof(Queryable.Select)
    ];

    private static readonly string[] BlockedOperators =
    [
        nameof(Queryable.GroupBy),
        nameof(Queryable.Join),
        nameof(Queryable.GroupJoin),
        nameof(Queryable.Union),
        nameof(Queryable.Intersect),
        nameof(Queryable.Except),
        nameof(Queryable.Zip)
    ];

    private static readonly HashSet<string> NativePreviewQueryableOperators =
        NativePreviewOperators.ToHashSet(StringComparer.Ordinal);

    private static readonly HashSet<string> BlockedQueryableOperators =
        BlockedOperators.ToHashSet(StringComparer.Ordinal);

    public static IReadOnlyList<string> GetNativePreviewOperators() => NativePreviewOperators;

    public static IReadOnlyList<string> GetBlockedOperators() => BlockedOperators;

    /// <summary>Classifies <paramref name="expression"/> for the given mode.</summary>
    /// <param name="expression">The composed query expression.</param>
    /// <param name="mode">The active provider mode.</param>
    /// <param name="model">The EF model, used to require that entity members are mapped properties.</param>
    /// <param name="isTracking">Whether the caller wants tracked entities back.</param>
    public static DataVoQueryTranslationDiagnostics Analyze(
        Expression expression,
        DataVoProviderModeStatus mode,
        IModel? model = null,
        bool isTracking = false)
    {
        var visitor = new OperatorCollector(model);
        visitor.Visit(expression);

        var operators = visitor.Operators.Distinct(StringComparer.Ordinal).ToArray();
        var blockedReasons = visitor.BlockedOperators
            .Distinct(StringComparer.Ordinal)
            .Select(op => $"Operator '{op}' is explicitly blocked by the current bridge.")
            .ToArray();

        if (blockedReasons.Length > 0)
        {
            return new DataVoQueryTranslationDiagnostics(
                mode.Mode,
                DataVoQueryTranslationOutcome.Blocked,
                operators,
                FallbackReasons: [],
                BlockedReasons: blockedReasons,
                Summary: string.Join(" ", blockedReasons));
        }

        if (!mode.NativeQueryTranslationPreviewEnabled)
        {
            return Fallback(mode, operators, ["Native query translation preview is disabled."], "Guarded fallback: native query translation preview is disabled.");
        }

        var fallbackReasons = visitor.FallbackReasons.ToList();
        if (isTracking)
        {
            fallbackReasons.Add(TrackingFallbackReason);
        }

        if (fallbackReasons.Count > 0)
        {
            return Fallback(mode, operators, fallbackReasons, string.Join(" ", fallbackReasons));
        }

        return new DataVoQueryTranslationDiagnostics(
            mode.Mode,
            DataVoQueryTranslationOutcome.NativeTranslationPreview,
            operators,
            FallbackReasons: [],
            BlockedReasons: [],
            Summary: "Query shape is within the current native translation preview subset.");
    }

    private static DataVoQueryTranslationDiagnostics Fallback(
        DataVoProviderModeStatus mode,
        IReadOnlyList<string> operators,
        IReadOnlyList<string> reasons,
        string summary)
    {
        return new DataVoQueryTranslationDiagnostics(
            mode.Mode,
            DataVoQueryTranslationOutcome.GuardedFallback,
            operators,
            FallbackReasons: reasons,
            BlockedReasons: [],
            Summary: summary);
    }

    private sealed class OperatorCollector(IModel? model) : ExpressionVisitor
    {
        public List<string> Operators { get; } = [];
        public List<string> FallbackReasons { get; } = [];
        public List<string> BlockedOperators { get; } = [];

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            string methodName = node.Method.Name;

            if (node.Method.DeclaringType == typeof(Queryable))
            {
                Operators.Add(methodName);

                if (BlockedQueryableOperators.Contains(methodName))
                {
                    BlockedOperators.Add(methodName);
                    return node;
                }

                if (!NativePreviewQueryableOperators.Contains(methodName))
                {
                    FallbackReasons.Add($"Operator '{methodName}' is not yet in the native translation preview subset.");
                    return base.VisitMethodCall(node);
                }

                ClassifyNativeOperator(node, methodName);
            }
            else if (node.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions) &&
                     methodName is nameof(EntityFrameworkQueryableExtensions.Include) or nameof(EntityFrameworkQueryableExtensions.ThenInclude))
            {
                Operators.Add(methodName);
                FallbackReasons.Add($"Operator '{methodName}' is not yet in the native translation preview subset.");
            }

            return base.VisitMethodCall(node);
        }

        private void ClassifyNativeOperator(MethodCallExpression node, string methodName)
        {
            switch (methodName)
            {
                case nameof(Queryable.Where):
                    if (!TryGetLambda(node.Arguments[1], out LambdaExpression? whereLambda) ||
                        !IsSupportedPredicate(whereLambda.Body, whereLambda.Parameters[0]))
                    {
                        FallbackReasons.Add("Operator 'Where' uses an expression shape that is not yet translatable in native preview.");
                    }
                    break;

                case nameof(Queryable.OrderBy):
                case nameof(Queryable.OrderByDescending):
                case nameof(Queryable.ThenBy):
                case nameof(Queryable.ThenByDescending):
                    if (!TryGetLambda(node.Arguments[1], out LambdaExpression? orderingLambda) ||
                        !IsSupportedOrderingSelector(orderingLambda))
                    {
                        FallbackReasons.Add($"Operator '{methodName}' requires either a simple entity property selector or DataVo vector distance function selector for native preview.");
                    }
                    break;

                case nameof(Queryable.Take):
                case nameof(Queryable.Skip):
                    if (!DataVoExpressionHelpers.TryEvaluate(node.Arguments[1]))
                    {
                        FallbackReasons.Add($"Operator '{methodName}' requires a constant or captured integer value for native preview.");
                    }
                    break;

                case nameof(Queryable.Select):
                    if (!TryGetLambda(node.Arguments[1], out LambdaExpression? selectLambda) ||
                        !IsSupportedSelectProjection(selectLambda))
                    {
                        FallbackReasons.Add("Operator 'Select' uses members or expression forms outside the native preview projection subset.");
                    }
                    break;
            }
        }

        private static bool TryGetLambda(Expression expression, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out LambdaExpression? lambda)
        {
            lambda = expression is UnaryExpression { NodeType: ExpressionType.Quote } unary
                ? unary.Operand as LambdaExpression
                : expression as LambdaExpression;
            return lambda is not null;
        }

        // ------------------------------------------------------------------ predicates (mirror TranslatePredicate)

        private bool IsSupportedPredicate(Expression expression, ParameterExpression parameter)
        {
            expression = DataVoExpressionHelpers.UnwrapConvert(expression);

            if (!DataVoExpressionHelpers.ReferencesParameter(expression, parameter))
            {
                return expression.Type == typeof(bool) && DataVoExpressionHelpers.TryEvaluate(expression);
            }

            return expression switch
            {
                BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.OrElse } logical =>
                    IsSupportedPredicate(logical.Left, parameter) && IsSupportedPredicate(logical.Right, parameter),
                BinaryExpression { NodeType: ExpressionType.Coalesce } coalesce =>
                    IsSupportedOperand(coalesce.Left, parameter) && IsSupportedPredicate(coalesce.Right, parameter),
                BinaryExpression comparison => IsSupportedComparison(comparison, parameter),
                MethodCallExpression methodCall => IsSupportedStringPredicate(methodCall, parameter),
                MemberExpression member => IsMappedMember(member, parameter, requiredType: typeof(bool)),
                UnaryExpression { NodeType: ExpressionType.Not } not => IsSupportedPredicate(not.Operand, parameter),
                _ => false
            };
        }

        private bool IsSupportedComparison(BinaryExpression expression, ParameterExpression parameter)
        {
            if (!DataVoExpressionHelpers.IsComparison(expression.NodeType))
            {
                return false;
            }

            if (DataVoExpressionHelpers.TrySplitBooleanComparison(expression, parameter, out Expression? predicate, out _))
            {
                return IsSupportedPredicate(predicate, parameter);
            }

            if (expression.Left is BinaryExpression { NodeType: ExpressionType.Coalesce } leftCoalesce)
            {
                return IsSupportedCoalesceOperand(leftCoalesce, parameter) && IsSupportedOperand(expression.Right, parameter);
            }

            if (expression.Right is BinaryExpression { NodeType: ExpressionType.Coalesce } rightCoalesce)
            {
                return IsSupportedCoalesceOperand(rightCoalesce, parameter) && IsSupportedOperand(expression.Left, parameter);
            }

            return IsSupportedOperand(expression.Left, parameter) && IsSupportedOperand(expression.Right, parameter);
        }

        private bool IsSupportedCoalesceOperand(BinaryExpression coalesce, ParameterExpression parameter)
            => IsSupportedOperand(coalesce.Left, parameter) && IsSupportedOperand(coalesce.Right, parameter);

        // Mirrors TranslateOperand: parameter-free values, mapped members, vector distances, string scalars.
        private bool IsSupportedOperand(Expression expression, ParameterExpression parameter)
        {
            expression = DataVoExpressionHelpers.UnwrapConvert(expression);

            if (!DataVoExpressionHelpers.ReferencesParameter(expression, parameter))
            {
                return DataVoExpressionHelpers.TryEvaluate(expression);
            }

            return expression switch
            {
                MemberExpression member => IsMappedMember(member, parameter),
                MethodCallExpression methodCall =>
                    IsSupportedVectorDistance(methodCall, parameter) || IsSupportedStringScalar(methodCall, parameter),
                _ => false
            };
        }

        private bool IsSupportedStringPredicate(MethodCallExpression methodCall, ParameterExpression parameter)
        {
            return methodCall.Method.DeclaringType == typeof(string) &&
                   methodCall.Method.Name is nameof(string.Contains) or nameof(string.StartsWith) or nameof(string.EndsWith) &&
                   methodCall.Arguments.Count == 1 &&
                   IsSupportedStringSource(methodCall.Object, parameter) &&
                   !DataVoExpressionHelpers.ReferencesParameter(methodCall.Arguments[0], parameter) &&
                   DataVoExpressionHelpers.TryEvaluate(methodCall.Arguments[0]);
        }

        private bool IsSupportedStringScalar(MethodCallExpression methodCall, ParameterExpression parameter)
        {
            return methodCall.Method.DeclaringType == typeof(string) &&
                   methodCall.Method.Name is nameof(string.ToLower) or nameof(string.ToUpper) &&
                   methodCall.Arguments.Count == 0 &&
                   IsSupportedStringSource(methodCall.Object, parameter);
        }

        private bool IsSupportedStringSource(Expression? expression, ParameterExpression parameter)
        {
            return expression is not null && DataVoExpressionHelpers.UnwrapConvert(expression) switch
            {
                MemberExpression member => IsMappedMember(member, parameter, requiredType: typeof(string)),
                MethodCallExpression methodCall => IsSupportedStringScalar(methodCall, parameter),
                _ => false
            };
        }

        private bool IsSupportedVectorDistance(MethodCallExpression methodCall, ParameterExpression parameter)
        {
            return DataVoExpressionHelpers.TryGetVectorDistanceOperands(methodCall, out Expression? left, out Expression? right, out _) &&
                   IsSupportedOperand(left, parameter) &&
                   IsSupportedOperand(right, parameter);
        }

        // ------------------------------------------------------------------ ordering & projection

        private bool IsSupportedOrderingSelector(LambdaExpression lambda)
        {
            ParameterExpression parameter = lambda.Parameters[0];
            return DataVoExpressionHelpers.UnwrapConvert(lambda.Body) switch
            {
                MemberExpression member => IsMappedMember(member, parameter),
                MethodCallExpression methodCall =>
                    IsSupportedVectorDistance(methodCall, parameter) || IsSupportedStringScalar(methodCall, parameter),
                _ => false
            };
        }

        // Projections run client-side over the referenced columns, so every parameter member must be mapped.
        private bool IsSupportedSelectProjection(LambdaExpression lambda)
        {
            ParameterExpression parameter = lambda.Parameters[0];
            return DataVoExpressionHelpers.FindParameterMembers(lambda.Body, parameter).All(member => IsMappedMember(member, parameter)) &&
                   !UsesParameterOutsideMembers(lambda.Body, parameter);
        }

        private static bool UsesParameterOutsideMembers(Expression body, ParameterExpression parameter)
        {
            var finder = new BareParameterFinder(parameter);
            finder.Visit(body);
            return finder.Found;
        }

        private bool IsMappedMember(MemberExpression member, ParameterExpression parameter, Type? requiredType = null)
        {
            if (!DataVoExpressionHelpers.IsParameterMember(member, parameter))
            {
                return false;
            }

            if (model is not null)
            {
                IProperty? property = model.FindEntityType(parameter.Type)?.FindProperty(member.Member.Name);
                if (property is null || property.PropertyInfo is null)
                {
                    return false;
                }
            }

            return requiredType is null || member.Type == requiredType;
        }

        /// <summary>Finds uses of the parameter that are not the source of a member access (e.g. <c>x =&gt; x</c>).</summary>
        private sealed class BareParameterFinder(ParameterExpression parameter) : ExpressionVisitor
        {
            public bool Found { get; private set; }

            protected override Expression VisitMember(MemberExpression node)
            {
                return DataVoExpressionHelpers.IsParameterMember(node, parameter) ? node : base.VisitMember(node);
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                Found |= node == parameter;
                return node;
            }
        }
    }
}
