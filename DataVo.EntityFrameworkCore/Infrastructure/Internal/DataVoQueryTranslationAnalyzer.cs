using Microsoft.EntityFrameworkCore;
using System.Linq.Expressions;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

internal static class DataVoQueryTranslationAnalyzer
{
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

    public static DataVoQueryTranslationDiagnostics Analyze(Expression expression, DataVoProviderModeStatus mode)
    {
        var visitor = new OperatorCollector();
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
            return new DataVoQueryTranslationDiagnostics(
                mode.Mode,
                DataVoQueryTranslationOutcome.GuardedFallback,
                operators,
                FallbackReasons: ["Native query translation preview is disabled."],
                BlockedReasons: [],
                Summary: "Guarded fallback: native query translation preview is disabled.");
        }

        var fallbackReasons = visitor.FallbackReasons
            .ToArray();

        if (fallbackReasons.Length > 0)
        {
            return new DataVoQueryTranslationDiagnostics(
                mode.Mode,
                DataVoQueryTranslationOutcome.GuardedFallback,
                operators,
                FallbackReasons: fallbackReasons,
                BlockedReasons: [],
                Summary: string.Join(" ", fallbackReasons));
        }

        return new DataVoQueryTranslationDiagnostics(
            mode.Mode,
            DataVoQueryTranslationOutcome.NativeTranslationPreview,
            operators,
            FallbackReasons: [],
            BlockedReasons: [],
            Summary: "Query shape is within the current native translation preview subset.");
    }

    private sealed class OperatorCollector : ExpressionVisitor
    {
        public List<string> Operators { get; } = [];
        public List<string> FallbackOperators { get; } = [];
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
                    FallbackOperators.Add(methodName);
                    FallbackReasons.Add($"Operator '{methodName}' is not yet in the native translation preview subset.");
                    return base.VisitMethodCall(node);
                }

                switch (methodName)
                {
                    case nameof(Queryable.Where):
                        if (!TryGetLambda(node.Arguments[1], out LambdaExpression? whereLambda) ||
                            !IsSupportedWherePredicate(whereLambda!))
                        {
                            FallbackReasons.Add("Operator 'Where' uses an expression shape that is not yet translatable in native preview.");
                        }
                        break;

                    case nameof(Queryable.OrderBy):
                    case nameof(Queryable.OrderByDescending):
                    case nameof(Queryable.ThenBy):
                    case nameof(Queryable.ThenByDescending):
                        if (!TryGetLambda(node.Arguments[1], out LambdaExpression? orderingLambda) ||
                            !IsSimpleMemberAccess(orderingLambda!))
                        {
                            FallbackReasons.Add($"Operator '{methodName}' requires a simple entity property selector for native preview.");
                        }
                        break;

                    case nameof(Queryable.Take):
                    case nameof(Queryable.Skip):
                        if (!IsConstantOrClosureValue(node.Arguments[1]))
                        {
                            FallbackReasons.Add($"Operator '{methodName}' requires a constant or captured integer value for native preview.");
                        }
                        break;

                    case nameof(Queryable.Select):
                        if (!TryGetLambda(node.Arguments[1], out LambdaExpression? selectLambda) ||
                            !IsSupportedSelectProjection(selectLambda!))
                        {
                            FallbackReasons.Add("Operator 'Select' uses members or expression forms outside the native preview projection subset.");
                        }
                        break;
                }
            }
            else if (node.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions))
            {
                if (methodName is nameof(EntityFrameworkQueryableExtensions.Include) or nameof(EntityFrameworkQueryableExtensions.ThenInclude))
                {
                    Operators.Add(methodName);
                    FallbackOperators.Add(methodName);
                    FallbackReasons.Add($"Operator '{methodName}' is not yet in the native translation preview subset.");
                }
            }

            return base.VisitMethodCall(node);
        }

        private static bool TryGetLambda(Expression expression, out LambdaExpression? lambda)
        {
            if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote)
            {
                lambda = unary.Operand as LambdaExpression;
                return lambda is not null;
            }

            lambda = expression as LambdaExpression;
            return lambda is not null;
        }

        private static bool IsConstantOrClosureValue(Expression expression)
        {
            if (expression is ConstantExpression constant && constant.Value is int)
            {
                return true;
            }

            try
            {
                var eval = Expression.Lambda<Func<int>>(Expression.Convert(expression, typeof(int))).Compile();
                _ = eval();
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static bool IsSimpleMemberAccess(LambdaExpression lambda)
        {
            Expression body = UnwrapConvert(lambda.Body);
            if (body is not MemberExpression memberExpression)
            {
                return false;
            }

            Expression? current = memberExpression.Expression;
            while (current is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                current = unary.Operand;
            }

            return current == lambda.Parameters[0];
        }

        private static bool IsSupportedWherePredicate(LambdaExpression lambda)
        {
            return IsSupportedPredicateNode(UnwrapConvert(lambda.Body), lambda.Parameters[0]);
        }

        private static bool IsSupportedPredicateNode(Expression expression, ParameterExpression parameter)
        {
            expression = UnwrapConvert(expression);

            if (expression is BinaryExpression binary)
            {
                if (binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
                {
                    return IsSupportedPredicateNode(binary.Left, parameter) &&
                           IsSupportedPredicateNode(binary.Right, parameter);
                }

                if (binary.NodeType is ExpressionType.Equal or ExpressionType.NotEqual or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual or ExpressionType.LessThan or ExpressionType.LessThanOrEqual)
                {
                    return IsSimpleOperand(binary.Left, parameter) && IsSimpleOperand(binary.Right, parameter);
                }

                if (binary.NodeType == ExpressionType.Coalesce)
                {
                    return IsSimpleOperand(binary.Left, parameter) && IsSimpleOperand(binary.Right, parameter);
                }

                return false;
            }

            if (expression is MethodCallExpression methodCallExpression)
            {
                return IsSupportedStringPredicate(methodCallExpression, parameter);
            }

            if (expression is MemberExpression memberExpression)
            {
                return IsEntityMember(memberExpression, parameter);
            }

            if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Not)
            {
                return IsSupportedPredicateNode(unary.Operand, parameter);
            }

            return expression is ConstantExpression;
        }

        private static bool IsSupportedStringPredicate(MethodCallExpression methodCallExpression, ParameterExpression parameter)
        {
            if (methodCallExpression.Method.DeclaringType != typeof(string))
            {
                return false;
            }

            if (methodCallExpression.Method.Name is not (nameof(string.Contains) or nameof(string.StartsWith) or nameof(string.EndsWith)))
            {
                return false;
            }

            if (methodCallExpression.Object is not MemberExpression sourceMember || !IsEntityMember(sourceMember, parameter))
            {
                return false;
            }

            if (methodCallExpression.Arguments.Count != 1)
            {
                return false;
            }

            return IsClosureEvaluatable(methodCallExpression.Arguments[0]);
        }

        private static bool IsSimpleOperand(Expression expression, ParameterExpression parameter)
        {
            expression = UnwrapConvert(expression);

            if (expression is MemberExpression memberExpression)
            {
                return IsEntityMember(memberExpression, parameter) || IsClosureMember(memberExpression);
            }

            if (expression is ConstantExpression)
            {
                return true;
            }

            if (expression is BinaryExpression binaryExpression && binaryExpression.NodeType == ExpressionType.Coalesce)
            {
                return IsSimpleOperand(binaryExpression.Left, parameter) &&
                       IsSimpleOperand(binaryExpression.Right, parameter);
            }

            if (expression is MethodCallExpression methodCallExpression)
            {
                return IsSupportedStringPredicate(methodCallExpression, parameter);
            }

            return IsClosureEvaluatable(expression);
        }

        private static bool IsSupportedSelectProjection(LambdaExpression lambda)
        {
            return IsSupportedSelectNode(UnwrapConvert(lambda.Body), lambda.Parameters[0]);
        }

        private static bool IsSupportedSelectNode(Expression expression, ParameterExpression parameter)
        {
            expression = UnwrapConvert(expression);

            if (expression is ConstantExpression)
            {
                return true;
            }

            if (expression is MemberExpression memberExpression)
            {
                return IsEntityMember(memberExpression, parameter) || IsClosureMember(memberExpression);
            }

            if (expression is BinaryExpression binaryExpression)
            {
                return IsSupportedSelectNode(binaryExpression.Left, parameter) &&
                       IsSupportedSelectNode(binaryExpression.Right, parameter);
            }

            if (expression is UnaryExpression unaryExpression)
            {
                return IsSupportedSelectNode(unaryExpression.Operand, parameter);
            }

            if (expression is MethodCallExpression methodCallExpression)
            {
                bool objectSupported = methodCallExpression.Object is null || IsSupportedSelectNode(methodCallExpression.Object, parameter);
                bool argumentsSupported = methodCallExpression.Arguments.All(argument => IsSupportedSelectNode(argument, parameter));
                return objectSupported && argumentsSupported;
            }

            if (expression is ConditionalExpression conditionalExpression)
            {
                return IsSupportedSelectNode(conditionalExpression.Test, parameter) &&
                       IsSupportedSelectNode(conditionalExpression.IfTrue, parameter) &&
                       IsSupportedSelectNode(conditionalExpression.IfFalse, parameter);
            }

            if (expression is NewExpression newExpression)
            {
                return newExpression.Arguments.All(argument => IsSupportedSelectNode(argument, parameter));
            }

            if (expression is MemberInitExpression memberInitExpression)
            {
                if (!IsSupportedSelectNode(memberInitExpression.NewExpression, parameter))
                {
                    return false;
                }

                return memberInitExpression.Bindings
                    .OfType<MemberAssignment>()
                    .All(assignment => IsSupportedSelectNode(assignment.Expression, parameter));
            }

            return IsClosureEvaluatable(expression);
        }

        private static bool IsEntityMember(MemberExpression memberExpression, ParameterExpression parameter)
        {
            Expression? current = memberExpression.Expression;
            while (current is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                current = unary.Operand;
            }

            return current == parameter;
        }

        private static bool IsClosureMember(MemberExpression memberExpression)
        {
            return memberExpression.Expression is ConstantExpression;
        }

        private static bool IsClosureEvaluatable(Expression expression)
        {
            try
            {
                var eval = Expression.Lambda<Func<object?>>(Expression.Convert(expression, typeof(object))).Compile();
                _ = eval();
                return true;
            }
            catch
            {
                return false;
            }
        }

        private static Expression UnwrapConvert(Expression expression)
        {
            while (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            {
                expression = unary.Operand;
            }

            return expression;
        }
    }
}
