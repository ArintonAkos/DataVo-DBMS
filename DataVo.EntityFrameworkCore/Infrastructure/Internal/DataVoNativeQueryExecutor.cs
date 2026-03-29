using DataVo.Data;
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

internal static class DataVoNativeQueryExecutor
{
    public static List<TEntity> ExecuteEntityQuery<TEntity>(DataVoDbContext context, Expression queryExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(queryExpression);

        var entityType = ResolveEntityType<TEntity>(context);
        var queryPlan = DataVoNativeQueryTranslator.TranslateEntityQuery(queryExpression, entityType);
        string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(context);

        using var connection = new DataVoConnection(connectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = queryPlan.Sql;

        using var reader = command.ExecuteReader();

        var results = new List<TEntity>();
        while (reader.Read())
        {
            object entity = Activator.CreateInstance(entityType.ClrType)
                ?? throw new DataVoEfException(
                    DataVoEfOperation.Query,
                    $"Could not create an instance of '{entityType.ClrType.FullName}' during native DataVo query materialization.");

            foreach (var binding in queryPlan.ColumnBindings)
            {
                int ordinal;
                try
                {
                    ordinal = reader.GetOrdinal(binding.ColumnName);
                }
                catch (IndexOutOfRangeException)
                {
                    continue;
                }

                object? raw = reader.GetValue(ordinal);
                if (raw is null or DBNull)
                {
                    continue;
                }

                object converted = DataVoEntityMaterializer.ConvertToClrType(raw, binding.Property.ClrType);
                binding.Property.PropertyInfo!.SetValue(entity, converted);
            }

            results.Add((TEntity)entity);
        }

        return results;
    }

    public static List<TDto> ExecuteProjectionQuery<TEntity, TDto>(
        DataVoDbContext context,
        Expression projectedExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(projectedExpression);

        var entityType = ResolveEntityType<TEntity>(context);
        var queryPlan = DataVoNativeQueryTranslator.TranslateProjectionQuery<TEntity, TDto>(projectedExpression, entityType);
        string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(context);

        using var connection = new DataVoConnection(connectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = queryPlan.Sql;

        using var reader = command.ExecuteReader();

        var results = new List<TDto>();
        while (reader.Read())
        {
            results.Add(queryPlan.Materialize(reader));
        }

        return results;
    }

    private static IEntityType ResolveEntityType<TEntity>(DataVoDbContext context)
        where TEntity : class
    {
        var entityType = context.Model.FindEntityType(typeof(TEntity));
        if (entityType is null)
        {
            throw new DataVoEfException(
                DataVoEfOperation.Query,
                $"Entity '{typeof(TEntity).Name}' is not mapped in the current DbContext model.");
        }

        return entityType;
    }
}

internal static class DataVoNativeQueryTranslator
{
    internal sealed record EntityColumnBinding(IProperty Property, string ColumnName);

    internal sealed record EntityQueryPlan(
        string Sql,
        IReadOnlyList<EntityColumnBinding> ColumnBindings);

    internal sealed record ProjectionQueryPlan<TDto>(
        string Sql,
        Func<DbDataReader, TDto> Materialize);

    public static EntityQueryPlan TranslateEntityQuery(Expression expression, IEntityType entityType)
    {
        var shape = ParseShape(expression);
        if (shape.SelectSelector is not null)
        {
            throw new NotSupportedException("Entity-native translation does not support projection in QueryFromDataVo. Use ProjectFromDataVo for projection queries.");
        }

        string tableName = entityType.GetTableName()
            ?? throw new NotSupportedException($"Entity '{entityType.DisplayName()}' is not mapped to a table.");

        var tableIdentifier = StoreObjectIdentifier.Table(tableName, entityType.GetSchema());

        var propertyBindings = entityType
            .GetProperties()
            .Where(static property => property.PropertyInfo is not null)
            .Select(property => new EntityColumnBinding(
                property,
                property.GetColumnName(tableIdentifier) ?? property.Name))
            .ToArray();

        string selectClause = string.Join(", ", propertyBindings.Select(static binding => binding.ColumnName));
        string sql = BuildSqlCore(
            tableName,
            selectClause,
            shape,
            entityType,
            tableIdentifier,
            requireSelect: false);

        return new EntityQueryPlan(sql, propertyBindings);
    }

    public static ProjectionQueryPlan<TDto> TranslateProjectionQuery<TEntity, TDto>(Expression expression, IEntityType entityType)
        where TEntity : class
    {
        var shape = ParseShape(expression);
        if (shape.SelectSelector is null)
        {
            throw new NotSupportedException("Projection-native translation requires a Select operator.");
        }

        string tableName = entityType.GetTableName()
            ?? throw new NotSupportedException($"Entity '{entityType.DisplayName()}' is not mapped to a table.");

        var tableIdentifier = StoreObjectIdentifier.Table(tableName, entityType.GetSchema());
        var selectProjection = BuildProjection<TEntity, TDto>(shape.SelectSelector, entityType, tableIdentifier);

        string sql = BuildSqlCore(
            tableName,
            selectProjection.SelectClause,
            shape,
            entityType,
            tableIdentifier,
            requireSelect: true);

        return new ProjectionQueryPlan<TDto>(sql, selectProjection.Materialize);
    }

    internal static bool IsSimpleSelectExpression(LambdaExpression selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Expression body = UnwrapConvert(selector.Body);

        return body switch
        {
            MemberExpression member => IsEntityMemberAccess(member, selector.Parameters[0]),
            NewExpression constructor => constructor.Arguments.All(argument => IsEntityMemberAccess(UnwrapConvert(argument) as MemberExpression, selector.Parameters[0])),
            MemberInitExpression memberInit =>
                memberInit.Bindings.OfType<MemberAssignment>().All(static assignment => assignment.Expression is MemberExpression or UnaryExpression) &&
                memberInit.Bindings.OfType<MemberAssignment>().All(assignment =>
                    IsEntityMemberAccess(UnwrapConvert(assignment.Expression) as MemberExpression, selector.Parameters[0])),
            _ => false
        };
    }

    private static string BuildSqlCore(
        string tableName,
        string selectClause,
        QueryShape shape,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier,
        bool requireSelect)
    {
        if (requireSelect && shape.SelectSelector is null)
        {
            throw new NotSupportedException("A Select expression is required for native projection translation.");
        }

        var whereClauses = shape.WherePredicates
            .Select(predicate => TranslatePredicate(predicate.Body, predicate.Parameters[0], entityType, tableIdentifier))
            .Where(static clause => !string.IsNullOrWhiteSpace(clause))
            .ToArray();

        string whereSql = whereClauses.Length == 0
            ? string.Empty
            : $" WHERE {string.Join(" AND ", whereClauses)}";

        var orderBuild = BuildOrderBySql(shape.Orderings, entityType, tableIdentifier);
        if (orderBuild.AdditionalSelectExpressions.Count > 0)
        {
            selectClause = $"{selectClause}, {string.Join(", ", orderBuild.AdditionalSelectExpressions)}";
        }

        string orderSql = orderBuild.OrderBySql;

        string pagingSql = string.Empty;
        if (shape.Skip is int skip)
        {
            pagingSql += $" OFFSET {skip}";
        }

        if (shape.Take is int take)
        {
            pagingSql += $" LIMIT {take}";
        }

        return $"SELECT {selectClause} FROM {tableName}{whereSql}{orderSql}{pagingSql};";
    }

    private static OrderByBuildResult BuildOrderBySql(
        IReadOnlyList<OrderingClause> orderings,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
    {
        if (orderings.Count == 0)
        {
            return new OrderByBuildResult(string.Empty, []);
        }

        var additionalSelects = new List<string>();

        string[] clauses = orderings
            .Select((ordering, index) =>
            {
                Expression keyBody = UnwrapConvert(ordering.KeySelector.Body);

                string keySql;
                if (keyBody is MemberExpression memberExpression &&
                    TryResolveProperty(memberExpression, ordering.KeySelector.Parameters[0], entityType, out IProperty? property))
                {
                    IProperty resolvedProperty = property
                        ?? throw new NotSupportedException("Native ORDER BY translation resolved to an unmapped property.");

                    keySql = resolvedProperty.GetColumnName(tableIdentifier) ?? resolvedProperty.Name;
                }
                else if (keyBody is MethodCallExpression)
                {
                    string expressionSql = TranslateOperand(keyBody, ordering.KeySelector.Parameters[0], entityType, tableIdentifier).Sql;
                    string alias = $"__ord_{index}";
                    additionalSelects.Add($"{expressionSql} AS {alias}");
                    keySql = alias;
                }
                else
                {
                    throw new NotSupportedException("Native ORDER BY translation supports mapped entity properties and translated scalar method expressions.");
                }

                string direction = ordering.Descending ? "DESC" : "ASC";
                return $"{keySql} {direction}";
            })
            .ToArray();

        return new OrderByBuildResult($" ORDER BY {string.Join(", ", clauses)}", additionalSelects);
    }

    private static string TranslatePredicate(
        Expression expression,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
    {
        expression = UnwrapConvert(expression);

        if (expression is BinaryExpression binaryExpression)
        {
            return TranslateBinaryPredicate(binaryExpression, parameter, entityType, tableIdentifier);
        }

        if (expression is MethodCallExpression methodCallExpression)
        {
            return TranslateStringMethodPredicate(methodCallExpression, parameter, entityType, tableIdentifier);
        }

        if (expression is MemberExpression memberExpression &&
            TryResolveProperty(memberExpression, parameter, entityType, out IProperty? boolProperty) &&
            boolProperty is not null &&
            boolProperty.ClrType == typeof(bool))
        {
            string columnName = boolProperty.GetColumnName(tableIdentifier) ?? boolProperty.Name;
            return $"{columnName} = 1";
        }

        if (expression is UnaryExpression unaryExpression && unaryExpression.NodeType == ExpressionType.Not)
        {
            Expression operand = UnwrapConvert(unaryExpression.Operand);
            if (operand is MemberExpression negatedMember &&
                TryResolveProperty(negatedMember, parameter, entityType, out IProperty? negatedBoolProperty) &&
                negatedBoolProperty is not null &&
                negatedBoolProperty.ClrType == typeof(bool))
            {
                string columnName = negatedBoolProperty.GetColumnName(tableIdentifier) ?? negatedBoolProperty.Name;
                return $"{columnName} = 0";
            }
        }

        throw new NotSupportedException("Native WHERE translation supports boolean and binary predicate expressions only.");
    }

    private static string TranslateBinaryPredicate(
        BinaryExpression expression,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
    {
        if (expression.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
        {
            string left = TranslatePredicate(expression.Left, parameter, entityType, tableIdentifier);
            string right = TranslatePredicate(expression.Right, parameter, entityType, tableIdentifier);
            string logical = expression.NodeType == ExpressionType.AndAlso ? "AND" : "OR";
            return $"({left} {logical} {right})";
        }

        if (TryTranslateCoalesceComparison(expression, parameter, entityType, tableIdentifier, out string? expandedCoalesceSql))
        {
            return expandedCoalesceSql!;
        }

        var leftOperand = TranslateOperand(expression.Left, parameter, entityType, tableIdentifier);
        var rightOperand = TranslateOperand(expression.Right, parameter, entityType, tableIdentifier);

        return BuildComparisonSql(expression.NodeType, leftOperand, rightOperand);
    }

    private static string BuildComparisonSql(
        ExpressionType comparisonType,
        SqlOperand leftOperand,
        SqlOperand rightOperand)
    {
        if (comparisonType == ExpressionType.Equal)
        {
            if (leftOperand.IsNullLiteral)
            {
                return $"{rightOperand.Sql} IS NULL";
            }

            if (rightOperand.IsNullLiteral)
            {
                return $"{leftOperand.Sql} IS NULL";
            }

            return $"{leftOperand.Sql} = {rightOperand.Sql}";
        }

        if (comparisonType == ExpressionType.NotEqual)
        {
            if (leftOperand.IsNullLiteral)
            {
                return $"{rightOperand.Sql} IS NOT NULL";
            }

            if (rightOperand.IsNullLiteral)
            {
                return $"{leftOperand.Sql} IS NOT NULL";
            }

            return $"{leftOperand.Sql} != {rightOperand.Sql}";
        }

        string op = comparisonType switch
        {
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new NotSupportedException($"Native WHERE translation does not support '{comparisonType}'.")
        };

        return $"{leftOperand.Sql} {op} {rightOperand.Sql}";
    }

    private static SqlOperand TranslateOperand(
        Expression expression,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
    {
        expression = UnwrapConvert(expression);

        if (expression is MemberExpression memberExpression &&
            TryResolveProperty(memberExpression, parameter, entityType, out IProperty? property))
        {
            IProperty resolvedProperty = property
                ?? throw new NotSupportedException("Native operand translation resolved to an unmapped property.");

            string columnName = resolvedProperty.GetColumnName(tableIdentifier) ?? resolvedProperty.Name;
            return new SqlOperand(columnName, IsNullLiteral: false);
        }

        if (expression is BinaryExpression binaryExpression && binaryExpression.NodeType == ExpressionType.Coalesce)
        {
            var left = TranslateOperand(binaryExpression.Left, parameter, entityType, tableIdentifier);
            var right = TranslateOperand(binaryExpression.Right, parameter, entityType, tableIdentifier);
            if (left.IsNullLiteral)
            {
                return right;
            }

            if (right.IsNullLiteral)
            {
                return left;
            }

            throw new NotSupportedException("Native coalesce translation requires comparison-context expansion and cannot be used as a standalone SQL operand.");
        }

        if (expression is MethodCallExpression methodCallExpression)
        {
            if (TryTranslateVectorDistanceExpression(methodCallExpression, parameter, entityType, tableIdentifier, out string? vectorDistanceSql))
            {
                return new SqlOperand(vectorDistanceSql!, IsNullLiteral: false);
            }

            if (methodCallExpression.Method.DeclaringType == typeof(string) && methodCallExpression.Type == typeof(string))
            {
                return new SqlOperand(
                    TranslateStringScalarExpression(methodCallExpression, parameter, entityType, tableIdentifier),
                    IsNullLiteral: false);
            }

            return new SqlOperand(
                TranslateStringMethodExpression(methodCallExpression, parameter, entityType, tableIdentifier),
                IsNullLiteral: false);
        }

        object? value = EvaluateExpression(expression);
        return new SqlOperand(DataVoSqlLiteralFormatter.Format(value), value is null);
    }

    private static bool TryTranslateVectorDistanceExpression(
        MethodCallExpression expression,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier,
        out string? sql)
    {
        sql = null;

        if (expression.Method.DeclaringType != typeof(DataVoVectorDbFunctions))
        {
            return false;
        }

        bool isCosine = expression.Method.Name == nameof(DataVoVectorDbFunctions.CosineDistance);
        bool isL2 = expression.Method.Name == nameof(DataVoVectorDbFunctions.L2Distance);
        if (!isCosine && !isL2)
        {
            return false;
        }

        // Extension-method form includes EF.Functions as the first argument.
        int vectorArgsStart = expression.Arguments.Count == 3 ? 1 : 0;
        int vectorArgsCount = expression.Arguments.Count - vectorArgsStart;
        if (vectorArgsCount != 2)
        {
            throw new NotSupportedException($"Native vector translation for '{expression.Method.Name}' requires exactly two vector operands.");
        }

        var left = TranslateOperand(expression.Arguments[vectorArgsStart], parameter, entityType, tableIdentifier);
        var right = TranslateOperand(expression.Arguments[vectorArgsStart + 1], parameter, entityType, tableIdentifier);

        if (isL2)
        {
            throw new NotSupportedException("Native translation for DataVoVectorDbFunctions.L2Distance is not available yet. Use cosine distance in native preview or raw SQL for custom distance behavior.");
        }

        // DataVo's vector-distance parser surface currently accepts the cosine operator form.
        sql = $"({left.Sql} <=> {right.Sql})";
        return true;
    }

    private static bool TryTranslateCoalesceComparison(
        BinaryExpression expression,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier,
        out string? sql)
    {
        sql = null;

        if (expression.NodeType is not (
            ExpressionType.Equal or
            ExpressionType.NotEqual or
            ExpressionType.GreaterThan or
            ExpressionType.GreaterThanOrEqual or
            ExpressionType.LessThan or
            ExpressionType.LessThanOrEqual))
        {
            return false;
        }

        if (expression.Left is BinaryExpression leftCoalesce && leftCoalesce.NodeType == ExpressionType.Coalesce)
        {
            sql = BuildExpandedCoalesceComparison(
                leftCoalesce,
                expression.Right,
                expression.NodeType,
                parameter,
                entityType,
                tableIdentifier,
                coalesceOnLeft: true);
            return true;
        }

        if (expression.Right is BinaryExpression rightCoalesce && rightCoalesce.NodeType == ExpressionType.Coalesce)
        {
            sql = BuildExpandedCoalesceComparison(
                rightCoalesce,
                expression.Left,
                expression.NodeType,
                parameter,
                entityType,
                tableIdentifier,
                coalesceOnLeft: false);
            return true;
        }

        return false;
    }

    private static string BuildExpandedCoalesceComparison(
        BinaryExpression coalesce,
        Expression otherExpression,
        ExpressionType comparisonType,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier,
        bool coalesceOnLeft)
    {
        var primary = TranslateOperand(coalesce.Left, parameter, entityType, tableIdentifier);
        var fallback = TranslateOperand(coalesce.Right, parameter, entityType, tableIdentifier);
        var other = TranslateOperand(otherExpression, parameter, entityType, tableIdentifier);

        string whenNull = coalesceOnLeft
            ? BuildComparisonSql(comparisonType, fallback, other)
            : BuildComparisonSql(comparisonType, other, fallback);

        string whenNotNull = coalesceOnLeft
            ? BuildComparisonSql(comparisonType, primary, other)
            : BuildComparisonSql(comparisonType, other, primary);

        return $"(({primary.Sql} IS NULL AND {whenNull}) OR ({primary.Sql} IS NOT NULL AND {whenNotNull}))";
    }

    private static string TranslateStringMethodPredicate(
        MethodCallExpression expression,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
    {
        string sql = TranslateStringMethodExpression(expression, parameter, entityType, tableIdentifier);
        return sql;
    }

    private static string TranslateStringMethodExpression(
        MethodCallExpression expression,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
    {
        if (expression.Method.DeclaringType != typeof(string))
        {
            throw new NotSupportedException($"Native translation does not support method '{expression.Method.Name}'.");
        }

        string sourceColumn = ResolveStringSourceSql(expression.Object, parameter, entityType, tableIdentifier);

        if (expression.Arguments.Count != 1)
        {
            throw new NotSupportedException($"Native string method '{expression.Method.Name}' requires exactly one argument.");
        }

        object? argumentValue = EvaluateExpression(expression.Arguments[0]);
        string argument = argumentValue?.ToString() ?? string.Empty;

        return expression.Method.Name switch
        {
            nameof(string.Contains) => BuildLikePredicate(sourceColumn, $"%{argument}%"),
            nameof(string.StartsWith) => BuildLikePredicate(sourceColumn, $"{argument}%"),
            nameof(string.EndsWith) => BuildLikePredicate(sourceColumn, $"%{argument}"),
            _ => throw new NotSupportedException($"Native string method '{expression.Method.Name}' is not supported.")
        };
    }

    private static string TranslateStringScalarExpression(
        MethodCallExpression expression,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
    {
        if (expression.Method.DeclaringType != typeof(string))
        {
            throw new NotSupportedException($"Native translation does not support method '{expression.Method.Name}'.");
        }

        if (expression.Arguments.Count != 0)
        {
            throw new NotSupportedException($"Native string scalar method '{expression.Method.Name}' requires zero arguments.");
        }

        string sourceSql = ResolveStringSourceSql(expression.Object, parameter, entityType, tableIdentifier);

        return expression.Method.Name switch
        {
            nameof(string.ToLower) => $"LOWER({sourceSql})",
            nameof(string.ToUpper) => $"UPPER({sourceSql})",
            _ => throw new NotSupportedException($"Native string scalar method '{expression.Method.Name}' is not supported.")
        };
    }

    private static string ResolveStringSourceSql(
        Expression? source,
        ParameterExpression parameter,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
    {
        if (source is null)
        {
            throw new NotSupportedException("Native string translation requires an instance source.");
        }

        source = UnwrapConvert(source);

        if (source is MemberExpression sourceMember &&
            TryResolveProperty(sourceMember, parameter, entityType, out IProperty? sourceProperty) &&
            sourceProperty is not null &&
            sourceProperty.ClrType == typeof(string))
        {
            return sourceProperty.GetColumnName(tableIdentifier) ?? sourceProperty.Name;
        }

        if (source is MethodCallExpression methodCallExpression &&
            methodCallExpression.Method.DeclaringType == typeof(string) &&
            methodCallExpression.Type == typeof(string))
        {
            return TranslateStringScalarExpression(methodCallExpression, parameter, entityType, tableIdentifier);
        }

        throw new NotSupportedException("Native string method translation requires a mapped string property source.");
    }

    private static string BuildLikePredicate(string sourceColumn, string pattern)
    {
        string escapedPattern = pattern.Replace("'", "''");
        return $"({sourceColumn} LIKE '{escapedPattern}')";
    }

    private static ProjectionBuildResult<TDto> BuildProjection<TEntity, TDto>(
        LambdaExpression selector,
        IEntityType entityType,
        StoreObjectIdentifier tableIdentifier)
        where TEntity : class
    {
        var mappedProperties = entityType
            .GetProperties()
            .Where(static property => property.PropertyInfo is not null)
            .ToDictionary(
                property => property.Name,
                property => property,
                StringComparer.Ordinal);

        ParameterExpression parameter = selector.Parameters[0];
        var referencedPropertyNames = CollectReferencedEntityPropertyNames(selector.Body, parameter);

        if (referencedPropertyNames.Any(name => !mappedProperties.ContainsKey(name)))
        {
            throw new NotSupportedException("Native projection selector references unmapped members.");
        }

        var projectionBindings = referencedPropertyNames
            .Distinct(StringComparer.Ordinal)
            .Select(propertyName =>
            {
                IProperty property = mappedProperties[propertyName];
                string columnName = property.GetColumnName(tableIdentifier) ?? property.Name;
                return new EntityColumnBinding(property, columnName);
            })
            .ToArray();

        string selectClause = projectionBindings.Length > 0
            ? string.Join(", ", projectionBindings.Select(static binding => binding.ColumnName))
            : "*";

        var compiledSelector = (Func<TEntity, TDto>)selector.Compile();

        return new ProjectionBuildResult<TDto>(
            SelectClause: selectClause,
            Materialize: reader =>
            {
                object entity = Activator.CreateInstance(entityType.ClrType)
                    ?? throw new DataVoEfException(
                        DataVoEfOperation.Query,
                        $"Could not create an instance of '{entityType.ClrType.FullName}' during native projection materialization.");

                foreach (var binding in projectionBindings)
                {
                    int ordinal;
                    try
                    {
                        ordinal = reader.GetOrdinal(binding.ColumnName);
                    }
                    catch (IndexOutOfRangeException)
                    {
                        continue;
                    }

                    object? raw = reader.GetValue(ordinal);
                    if (raw is null or DBNull)
                    {
                        continue;
                    }

                    object converted = DataVoEntityMaterializer.ConvertToClrType(raw, binding.Property.ClrType);
                    binding.Property.PropertyInfo!.SetValue(entity, converted);
                }

                return compiledSelector((TEntity)entity);
            });
    }

    private static IReadOnlyList<string> CollectReferencedEntityPropertyNames(Expression expression, ParameterExpression parameter)
    {
        var collector = new List<string>();

        void Visit(Expression node)
        {
            if (node is null)
            {
                return;
            }

            node = UnwrapConvert(node);

            switch (node)
            {
                case MemberExpression memberExpression:
                    {
                        if (IsEntityMemberAccess(memberExpression, parameter))
                        {
                            collector.Add(memberExpression.Member.Name);
                        }

                        if (memberExpression.Expression is not null)
                        {
                            Visit(memberExpression.Expression);
                        }

                        break;
                    }

                case BinaryExpression binaryExpression:
                    Visit(binaryExpression.Left);
                    Visit(binaryExpression.Right);
                    break;

                case UnaryExpression unaryExpression:
                    Visit(unaryExpression.Operand);
                    break;

                case MethodCallExpression methodCallExpression:
                    if (methodCallExpression.Object is not null)
                    {
                        Visit(methodCallExpression.Object);
                    }

                    foreach (Expression argument in methodCallExpression.Arguments)
                    {
                        Visit(argument);
                    }

                    break;

                case ConditionalExpression conditionalExpression:
                    Visit(conditionalExpression.Test);
                    Visit(conditionalExpression.IfTrue);
                    Visit(conditionalExpression.IfFalse);
                    break;

                case NewExpression newExpression:
                    foreach (Expression argument in newExpression.Arguments)
                    {
                        Visit(argument);
                    }

                    break;

                case MemberInitExpression memberInitExpression:
                    Visit(memberInitExpression.NewExpression);
                    foreach (MemberBinding binding in memberInitExpression.Bindings)
                    {
                        if (binding is MemberAssignment assignment)
                        {
                            Visit(assignment.Expression);
                        }
                    }

                    break;
            }
        }

        Visit(expression);
        return collector;
    }

    private static IProperty ResolveProperty(
        MemberExpression memberExpression,
        ParameterExpression parameter,
        IEntityType entityType)
    {
        if (!TryResolveProperty(memberExpression, parameter, entityType, out IProperty? property) || property is null)
        {
            throw new NotSupportedException(
                $"Member '{memberExpression.Member.Name}' is not mapped as a scalar property on '{entityType.DisplayName()}'.");
        }

        return property;
    }

    private static bool TryResolveProperty(
        MemberExpression memberExpression,
        ParameterExpression parameter,
        IEntityType entityType,
        out IProperty? property)
    {
        property = null;

        Expression? current = memberExpression.Expression;
        while (current is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            current = unary.Operand;
        }

        if (current != parameter)
        {
            return false;
        }

        property = entityType.FindProperty(memberExpression.Member.Name);
        return property is not null;
    }

    private static QueryShape ParseShape(Expression expression)
    {
        var shape = new QueryShape();
        ParseRecursive(expression, shape);
        return shape;
    }

    private static void ParseRecursive(Expression expression, QueryShape shape)
    {
        if (expression is MethodCallExpression methodCall)
        {
            if (methodCall.Method.DeclaringType == typeof(Queryable))
            {
                ParseRecursive(methodCall.Arguments[0], shape);

                switch (methodCall.Method.Name)
                {
                    case nameof(Queryable.Where):
                        shape.WherePredicates.Add(Unquote(methodCall.Arguments[1]));
                        return;

                    case nameof(Queryable.OrderBy):
                        shape.Orderings.Add(new OrderingClause(Unquote(methodCall.Arguments[1]), Descending: false));
                        return;

                    case nameof(Queryable.OrderByDescending):
                        shape.Orderings.Add(new OrderingClause(Unquote(methodCall.Arguments[1]), Descending: true));
                        return;

                    case nameof(Queryable.ThenBy):
                        shape.Orderings.Add(new OrderingClause(Unquote(methodCall.Arguments[1]), Descending: false));
                        return;

                    case nameof(Queryable.ThenByDescending):
                        shape.Orderings.Add(new OrderingClause(Unquote(methodCall.Arguments[1]), Descending: true));
                        return;

                    case nameof(Queryable.Skip):
                        shape.Skip = Convert.ToInt32(EvaluateExpression(methodCall.Arguments[1]), CultureInfo.InvariantCulture);
                        return;

                    case nameof(Queryable.Take):
                        shape.Take = Convert.ToInt32(EvaluateExpression(methodCall.Arguments[1]), CultureInfo.InvariantCulture);
                        return;

                    case nameof(Queryable.Select):
                        shape.SelectSelector = Unquote(methodCall.Arguments[1]);
                        return;

                    default:
                        throw new NotSupportedException($"Native query translation does not support '{methodCall.Method.Name}'.");
                }
            }

            if (methodCall.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions) &&
                methodCall.Method.Name == nameof(EntityFrameworkQueryableExtensions.AsNoTracking))
            {
                ParseRecursive(methodCall.Arguments[0], shape);
                return;
            }
        }

        if (expression is ConstantExpression)
        {
            return;
        }

        if (expression is ParameterExpression)
        {
            return;
        }

        return;
    }

    private static object? EvaluateExpression(Expression expression)
    {
        if (expression is ConstantExpression constant)
        {
            return constant.Value;
        }

        var lambda = Expression.Lambda<Func<object?>>(
            Expression.Convert(expression, typeof(object)));
        return lambda.Compile().Invoke();
    }

    private static LambdaExpression Unquote(Expression expression)
    {
        return expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expression;
    }

    private static Expression UnwrapConvert(Expression expression)
    {
        while (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static bool IsEntityMemberAccess(MemberExpression? memberExpression, ParameterExpression parameter)
    {
        if (memberExpression is null)
        {
            return false;
        }

        Expression? current = memberExpression.Expression;
        while (current is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            current = unary.Operand;
        }

        return current == parameter;
    }

    private readonly record struct SqlOperand(string Sql, bool IsNullLiteral);

    private sealed class QueryShape
    {
        public List<LambdaExpression> WherePredicates { get; } = [];
        public List<OrderingClause> Orderings { get; } = [];
        public int? Skip { get; set; }
        public int? Take { get; set; }
        public LambdaExpression? SelectSelector { get; set; }
    }

    private sealed record OrderingClause(LambdaExpression KeySelector, bool Descending);

    private sealed record ProjectionBuildResult<TDto>(
        string SelectClause,
        Func<DbDataReader, TDto> Materialize);

    private sealed record OrderByBuildResult(
        string OrderBySql,
        IReadOnlyList<string> AdditionalSelectExpressions);

}

internal static class DataVoSqlLiteralFormatter
{
    public static string Format(object? value)
    {
        return value switch
        {
            null or DBNull => "NULL",
            bool booleanValue => booleanValue ? "1" : "0",
            string stringValue => $"'{stringValue.Replace("'", "''")}'",
            float[] floatVector => FormatVector(floatVector),
            double[] doubleVector => FormatVector(doubleVector),
            DateOnly dateOnlyValue => $"'{dateOnlyValue:yyyy-MM-dd}'",
            DateTime dateTimeValue => $"'{dateTimeValue:yyyy-MM-dd}'",
            DateTimeOffset dateTimeOffsetValue => $"'{dateTimeOffsetValue:yyyy-MM-dd}'",
            Guid guidValue => $"'{guidValue}'",
            Enum enumValue => Convert.ToInt32(enumValue, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => $"'{value.ToString()?.Replace("'", "''")}'"
        };
    }

    private static string FormatVector<T>(IEnumerable<T> vector)
        where T : IFormattable
    {
        string joined = string.Join(",", vector.Select(component => component.ToString(null, CultureInfo.InvariantCulture)));
        return $"'[{joined}]'";
    }
}