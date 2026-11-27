using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Runs LINQ shapes accepted by <see cref="DataVoQueryTranslationAnalyzer"/> as parameterized DataVo SQL.
/// </summary>
internal static class DataVoNativeQueryExecutor
{
    /// <summary>Runs an entity query and returns untracked entities.</summary>
    public static List<TEntity> ExecuteEntityQuery<TEntity>(
        DataVoDbContext context,
        Expression queryExpression,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(queryExpression);

        var entityType = ResolveEntityType<TEntity>(context);
        var plan = DataVoNativeQueryTranslator.TranslateEntityQuery(queryExpression, entityType);
        var rowReader = new EntityRowReader(entityType, plan.ColumnBindings);

        return ExecuteReader(context, plan.Statement, reader => (TEntity)rowReader.Read(reader), cancellationToken);
    }

    /// <summary>Runs a projection query, reading only the columns the selector uses.</summary>
    public static List<TDto> ExecuteProjectionQuery<TEntity, TDto>(
        DataVoDbContext context,
        Expression projectedExpression,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(projectedExpression);

        var entityType = ResolveEntityType<TEntity>(context);
        var plan = DataVoNativeQueryTranslator.TranslateProjectionQuery<TEntity, TDto>(projectedExpression, entityType);

        return ExecuteReader(context, plan.Statement, plan.Materialize, cancellationToken);
    }

    /// <summary>Counts the rows a filter-only query matches with <c>COUNT(*)</c>.</summary>
    public static int ExecuteCountQuery<TEntity>(
        DataVoDbContext context,
        Expression queryExpression,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(queryExpression);

        var entityType = ResolveEntityType<TEntity>(context);
        var statement = DataVoNativeQueryTranslator.TranslateCountQuery(queryExpression, entityType);

        var counts = ExecuteReader(
            context,
            statement,
            static reader => Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
            cancellationToken);
        return counts.Count == 0 ? 0 : counts[0];
    }

    private static List<T> ExecuteReader<T>(
        DataVoDbContext context,
        DataVoSqlStatement statement,
        Func<DbDataReader, T> materialize,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(context);

        using var connection = DataVoBridgeStorage.Open(connectionString);
        using var command = connection.CreateCommand(statement);
        using var reader = command.ExecuteReader();

        var results = new List<T>();
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            results.Add(materialize(reader));
        }

        return results;
    }

    private static IEntityType ResolveEntityType<TEntity>(DataVoDbContext context)
        where TEntity : class
    {
        return context.Model.FindEntityType(typeof(TEntity))
            ?? throw new DataVoEfException(
                DataVoEfOperation.Query,
                $"Entity '{typeof(TEntity).Name}' is not mapped in the current DbContext model.");
    }
}

/// <summary>
/// A mapped property and the result column it is read from.
/// </summary>
internal sealed record EntityColumnBinding(IProperty Property, string ColumnName);

/// <summary>
/// Creates entity instances from result rows, resolving column ordinals once per result set.
/// </summary>
internal sealed class EntityRowReader(IEntityType entityType, IReadOnlyList<EntityColumnBinding> bindings)
{
    private int[]? _ordinals;

    public object Read(DbDataReader reader)
    {
        _ordinals ??= ResolveOrdinals(reader);

        object entity = Activator.CreateInstance(entityType.ClrType)
            ?? throw new DataVoEfException(
                DataVoEfOperation.Query,
                $"Could not create an instance of '{entityType.ClrType.FullName}' during native DataVo query materialization.");

        for (int i = 0; i < bindings.Count; i++)
        {
            int ordinal = _ordinals[i];
            if (ordinal < 0)
            {
                continue;
            }

            object? raw = reader.GetValue(ordinal);
            if (raw is null or DBNull)
            {
                continue;
            }

            IProperty property = bindings[i].Property;
            property.PropertyInfo!.SetValue(entity, DataVoEntityMaterializer.ConvertToClrType(raw, property.ClrType));
        }

        return entity;
    }

    private int[] ResolveOrdinals(DbDataReader reader)
    {
        var ordinals = new int[bindings.Count];
        for (int i = 0; i < bindings.Count; i++)
        {
            try
            {
                ordinals[i] = reader.GetOrdinal(bindings[i].ColumnName);
            }
            catch (IndexOutOfRangeException)
            {
                // Column in the model but not in the DataVo table (schema drift): keep the CLR default.
                ordinals[i] = -1;
            }
        }

        return ordinals;
    }
}

/// <summary>
/// Translates a LINQ query shape over one entity type into parameterized DataVo SQL. Every value taken
/// from the query (constants and captured variables) is bound as a parameter.
/// </summary>
internal sealed class DataVoNativeQueryTranslator
{
    private readonly IEntityType _entityType;
    private readonly string _tableName;
    private readonly StoreObjectIdentifier _table;
    private readonly DataVoSqlParameterList _parameters = new();

    private DataVoNativeQueryTranslator(IEntityType entityType)
    {
        _entityType = entityType;
        _tableName = entityType.GetTableName()
            ?? throw new NotSupportedException($"Entity '{entityType.DisplayName()}' is not mapped to a table.");
        _table = StoreObjectIdentifier.Table(_tableName, entityType.GetSchema());
    }

    internal sealed record EntityQueryPlan(DataVoSqlStatement Statement, IReadOnlyList<EntityColumnBinding> ColumnBindings);

    internal sealed record ProjectionQueryPlan<TDto>(DataVoSqlStatement Statement, Func<DbDataReader, TDto> Materialize);

    public static EntityQueryPlan TranslateEntityQuery(Expression expression, IEntityType entityType)
    {
        var shape = DataVoQueryShape.Parse(expression);
        if (shape.SelectSelector is not null)
        {
            throw new NotSupportedException("Entity-native translation does not support projection in QueryFromDataVo. Use ProjectFromDataVo for projection queries.");
        }

        var translator = new DataVoNativeQueryTranslator(entityType);
        var bindings = entityType
            .GetProperties()
            .Where(static property => property.PropertyInfo is not null)
            .Select(translator.Bind)
            .ToArray();

        string sql = translator.BuildSelectSql(string.Join(", ", bindings.Select(static binding => binding.ColumnName)), shape);
        return new EntityQueryPlan(translator.Statement(sql), bindings);
    }

    public static ProjectionQueryPlan<TDto> TranslateProjectionQuery<TEntity, TDto>(Expression expression, IEntityType entityType)
        where TEntity : class
    {
        var shape = DataVoQueryShape.Parse(expression);
        LambdaExpression selector = shape.SelectSelector
            ?? throw new NotSupportedException("Projection-native translation requires a Select operator.");

        var translator = new DataVoNativeQueryTranslator(entityType);
        var bindings = translator.BindReferencedProperties(selector);
        string selectClause = bindings.Count > 0
            ? string.Join(", ", bindings.Select(static binding => binding.ColumnName))
            : "*";

        string sql = translator.BuildSelectSql(selectClause, shape);
        var rowReader = new EntityRowReader(entityType, bindings);
        var compiledSelector = (Func<TEntity, TDto>)selector.Compile();

        return new ProjectionQueryPlan<TDto>(
            translator.Statement(sql),
            reader => compiledSelector((TEntity)rowReader.Read(reader)));
    }

    public static DataVoSqlStatement TranslateCountQuery(Expression expression, IEntityType entityType)
    {
        var shape = DataVoQueryShape.Parse(expression);
        if (shape.SelectSelector is not null || shape.Skip is not null || shape.Take is not null)
        {
            throw new NotSupportedException("Native COUNT translation supports filter-only query shapes.");
        }

        var translator = new DataVoNativeQueryTranslator(entityType);
        return translator.Statement($"SELECT COUNT(*) AS __count FROM {translator._tableName}{translator.BuildWhereSql(shape)};");
    }

    private DataVoSqlStatement Statement(string sql) => new(sql, _parameters.Parameters);

    private EntityColumnBinding Bind(IProperty property) => new(property, ColumnOf(property));

    private string ColumnOf(IProperty property) => property.GetColumnName(_table) ?? property.Name;

    // ------------------------------------------------------------------ SELECT assembly

    private string BuildSelectSql(string selectClause, DataVoQueryShape shape)
    {
        string whereSql = BuildWhereSql(shape);

        var orderBy = BuildOrderBy(shape.Orderings);
        if (orderBy.AdditionalSelectExpressions.Count > 0)
        {
            selectClause = $"{selectClause}, {string.Join(", ", orderBy.AdditionalSelectExpressions)}";
        }

        string pagingSql = string.Empty;
        if (shape.Skip is int skip)
        {
            pagingSql += $" OFFSET {skip.ToString(CultureInfo.InvariantCulture)}";
        }

        if (shape.Take is int take)
        {
            pagingSql += $" LIMIT {take.ToString(CultureInfo.InvariantCulture)}";
        }

        return $"SELECT {selectClause} FROM {_tableName}{whereSql}{orderBy.Sql}{pagingSql};";
    }

    private string BuildWhereSql(DataVoQueryShape shape)
    {
        var clauses = shape.WherePredicates
            .Select(predicate => TranslatePredicate(predicate.Body, predicate.Parameters[0]))
            .ToArray();

        return clauses.Length == 0 ? string.Empty : $" WHERE {string.Join(" AND ", clauses)}";
    }

    private (string Sql, IReadOnlyList<string> AdditionalSelectExpressions) BuildOrderBy(IReadOnlyList<DataVoQueryShape.Ordering> orderings)
    {
        if (orderings.Count == 0)
        {
            return (string.Empty, []);
        }

        var additionalSelects = new List<string>();
        var clauses = new List<string>(orderings.Count);

        foreach (var ordering in orderings)
        {
            ParameterExpression parameter = ordering.KeySelector.Parameters[0];
            Expression keyBody = DataVoExpressionHelpers.UnwrapConvert(ordering.KeySelector.Body);

            string keySql;
            if (keyBody is MemberExpression member && TryResolveProperty(member, parameter, out IProperty? property))
            {
                keySql = ColumnOf(property);
            }
            else if (keyBody is MethodCallExpression)
            {
                string alias = $"__ord_{clauses.Count}";
                additionalSelects.Add($"{TranslateOperand(keyBody, parameter).Sql} AS {alias}");
                keySql = alias;
            }
            else
            {
                throw new NotSupportedException("Native ORDER BY translation supports mapped entity properties and translated scalar method expressions.");
            }

            clauses.Add($"{keySql} {(ordering.Descending ? "DESC" : "ASC")}");
        }

        return ($" ORDER BY {string.Join(", ", clauses)}", additionalSelects);
    }

    // ------------------------------------------------------------------ predicates

    private string TranslatePredicate(Expression expression, ParameterExpression parameter)
    {
        expression = DataVoExpressionHelpers.UnwrapConvert(expression);

        // Constant or captured booleans, e.g. Where(x => true) or Where(x => includeAll).
        if (!DataVoExpressionHelpers.ReferencesParameter(expression, parameter))
        {
            return Equals(DataVoExpressionHelpers.Evaluate(expression), true) ? "1 = 1" : "1 = 0";
        }

        switch (expression)
        {
            case BinaryExpression binary when binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse:
                string logical = binary.NodeType == ExpressionType.AndAlso ? "AND" : "OR";
                return $"({TranslatePredicate(binary.Left, parameter)} {logical} {TranslatePredicate(binary.Right, parameter)})";

            case BinaryExpression binary when binary.NodeType == ExpressionType.Coalesce:
                // x.Flag ?? fallback: true when the flag is set, otherwise the fallback decides.
                string flag = TranslateOperand(binary.Left, parameter).Sql;
                return $"({flag} = 1 OR ({flag} IS NULL AND {TranslatePredicate(binary.Right, parameter)}))";

            case BinaryExpression binary:
                return TranslateComparison(binary, parameter);

            case MethodCallExpression methodCall:
                return TranslateStringPredicate(methodCall, parameter);

            case MemberExpression member when TryResolveProperty(member, parameter, out IProperty? property) && property.ClrType == typeof(bool):
                return $"{ColumnOf(property)} = 1";

            case UnaryExpression { NodeType: ExpressionType.Not } not:
                Expression operand = DataVoExpressionHelpers.UnwrapConvert(not.Operand);
                if (operand is MemberExpression negatedMember &&
                    TryResolveProperty(negatedMember, parameter, out IProperty? negatedProperty) &&
                    negatedProperty.ClrType == typeof(bool))
                {
                    return $"{ColumnOf(negatedProperty)} = 0";
                }

                return $"NOT ({TranslatePredicate(operand, parameter)})";
        }

        throw new NotSupportedException("Native WHERE translation supports boolean and binary predicate expressions only.");
    }

    private string TranslateComparison(BinaryExpression expression, ParameterExpression parameter)
    {
        if (!DataVoExpressionHelpers.IsComparison(expression.NodeType))
        {
            throw new NotSupportedException($"Native WHERE translation does not support '{expression.NodeType}'.");
        }

        // A predicate compared with a boolean value, e.g. x.Name.Contains(s) == false.
        if (DataVoExpressionHelpers.TrySplitBooleanComparison(expression, parameter, out Expression? predicate, out bool expected))
        {
            string predicateSql = TranslatePredicate(predicate, parameter);
            return expected ? predicateSql : $"NOT ({predicateSql})";
        }

        if (expression.Left is BinaryExpression { NodeType: ExpressionType.Coalesce } leftCoalesce)
        {
            return BuildCoalesceComparison(leftCoalesce, expression.Right, expression.NodeType, parameter, coalesceOnLeft: true);
        }

        if (expression.Right is BinaryExpression { NodeType: ExpressionType.Coalesce } rightCoalesce)
        {
            return BuildCoalesceComparison(rightCoalesce, expression.Left, expression.NodeType, parameter, coalesceOnLeft: false);
        }

        return BuildComparisonSql(
            expression.NodeType,
            TranslateOperand(expression.Left, parameter),
            TranslateOperand(expression.Right, parameter));
    }

    private string BuildCoalesceComparison(
        BinaryExpression coalesce,
        Expression otherExpression,
        ExpressionType comparisonType,
        ParameterExpression parameter,
        bool coalesceOnLeft)
    {
        var primary = TranslateOperand(coalesce.Left, parameter);
        var fallback = TranslateOperand(coalesce.Right, parameter);
        var other = TranslateOperand(otherExpression, parameter);

        string whenNull = coalesceOnLeft
            ? BuildComparisonSql(comparisonType, fallback, other)
            : BuildComparisonSql(comparisonType, other, fallback);

        string whenNotNull = coalesceOnLeft
            ? BuildComparisonSql(comparisonType, primary, other)
            : BuildComparisonSql(comparisonType, other, primary);

        return $"(({primary.Sql} IS NULL AND {whenNull}) OR ({primary.Sql} IS NOT NULL AND {whenNotNull}))";
    }

    private static string BuildComparisonSql(ExpressionType comparisonType, SqlOperand left, SqlOperand right)
    {
        if (comparisonType is ExpressionType.Equal or ExpressionType.NotEqual)
        {
            string nullTest = comparisonType == ExpressionType.Equal ? "IS NULL" : "IS NOT NULL";
            if (left.IsNullLiteral)
            {
                return $"{right.Sql} {nullTest}";
            }

            if (right.IsNullLiteral)
            {
                return $"{left.Sql} {nullTest}";
            }
        }

        if (left.IsNullLiteral || right.IsNullLiteral)
        {
            // An ordering comparison with null is never true in LINQ or SQL.
            return "1 = 0";
        }

        string op = comparisonType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new NotSupportedException($"Native WHERE translation does not support '{comparisonType}'.")
        };

        return $"{left.Sql} {op} {right.Sql}";
    }

    // ------------------------------------------------------------------ operands

    private SqlOperand TranslateOperand(Expression expression, ParameterExpression parameter)
    {
        expression = DataVoExpressionHelpers.UnwrapConvert(expression);

        if (!DataVoExpressionHelpers.ReferencesParameter(expression, parameter))
        {
            object? value = DataVoExpressionHelpers.Evaluate(expression);
            return value is null
                ? new SqlOperand("NULL", IsNullLiteral: true)
                : new SqlOperand(_parameters.Add(value), IsNullLiteral: false);
        }

        if (expression is MemberExpression member && TryResolveProperty(member, parameter, out IProperty? property))
        {
            return new SqlOperand(ColumnOf(property), IsNullLiteral: false);
        }

        if (expression is MethodCallExpression methodCall)
        {
            if (TryTranslateVectorDistance(methodCall, parameter, out string? vectorDistanceSql))
            {
                return new SqlOperand(vectorDistanceSql, IsNullLiteral: false);
            }

            if (methodCall.Method.DeclaringType == typeof(string) && methodCall.Type == typeof(string))
            {
                return new SqlOperand(TranslateStringScalar(methodCall, parameter), IsNullLiteral: false);
            }
        }

        throw new NotSupportedException($"Native translation does not support the operand '{expression}'.");
    }

    private bool TryTranslateVectorDistance(MethodCallExpression expression, ParameterExpression parameter, out string sql)
    {
        sql = string.Empty;
        if (!DataVoExpressionHelpers.TryGetVectorDistanceOperands(expression, out Expression? left, out Expression? right, out string? operatorToken))
        {
            return false;
        }

        sql = $"({TranslateOperand(left, parameter).Sql} {operatorToken} {TranslateOperand(right, parameter).Sql})";
        return true;
    }

    private string TranslateStringPredicate(MethodCallExpression expression, ParameterExpression parameter)
    {
        if (expression.Method.DeclaringType != typeof(string) || expression.Arguments.Count != 1)
        {
            throw new NotSupportedException($"Native translation does not support method '{expression.Method.Name}'.");
        }

        string source = ResolveStringSourceSql(expression.Object, parameter);
        string argument = DataVoExpressionHelpers.Evaluate(expression.Arguments[0])?.ToString() ?? string.Empty;
        string escaped = EscapeLikePattern(argument);

        string pattern = expression.Method.Name switch
        {
            nameof(string.Contains) => $"%{escaped}%",
            nameof(string.StartsWith) => $"{escaped}%",
            nameof(string.EndsWith) => $"%{escaped}",
            _ => throw new NotSupportedException($"Native string method '{expression.Method.Name}' is not supported.")
        };

        return $"({source} LIKE {_parameters.Add(pattern)})";
    }

    private string TranslateStringScalar(MethodCallExpression expression, ParameterExpression parameter)
    {
        if (expression.Arguments.Count != 0)
        {
            throw new NotSupportedException($"Native string scalar method '{expression.Method.Name}' requires zero arguments.");
        }

        string source = ResolveStringSourceSql(expression.Object, parameter);
        return expression.Method.Name switch
        {
            nameof(string.ToLower) => $"LOWER({source})",
            nameof(string.ToUpper) => $"UPPER({source})",
            _ => throw new NotSupportedException($"Native string scalar method '{expression.Method.Name}' is not supported.")
        };
    }

    private string ResolveStringSourceSql(Expression? source, ParameterExpression parameter)
    {
        source = source is null ? null : DataVoExpressionHelpers.UnwrapConvert(source);

        if (source is MemberExpression member &&
            TryResolveProperty(member, parameter, out IProperty? property) &&
            property.ClrType == typeof(string))
        {
            return ColumnOf(property);
        }

        if (source is MethodCallExpression methodCall &&
            methodCall.Method.DeclaringType == typeof(string) &&
            methodCall.Type == typeof(string))
        {
            return TranslateStringScalar(methodCall, parameter);
        }

        throw new NotSupportedException("Native string method translation requires a mapped string property source.");
    }

    /// <summary>Escapes LIKE wildcards (and the backslash escape itself) so the argument matches literally.</summary>
    internal static string EscapeLikePattern(string value)
        => value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");

    // ------------------------------------------------------------------ projection

    private IReadOnlyList<EntityColumnBinding> BindReferencedProperties(LambdaExpression selector)
    {
        var bindings = new List<EntityColumnBinding>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (MemberExpression member in DataVoExpressionHelpers.FindParameterMembers(selector.Body, selector.Parameters[0]))
        {
            if (!TryResolveProperty(member, selector.Parameters[0], out IProperty? property) || property.PropertyInfo is null)
            {
                throw new NotSupportedException("Native projection selector references unmapped members.");
            }

            if (seen.Add(property.Name))
            {
                bindings.Add(Bind(property));
            }
        }

        return bindings;
    }

    private bool TryResolveProperty(MemberExpression member, ParameterExpression parameter, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out IProperty? property)
    {
        property = DataVoExpressionHelpers.IsParameterMember(member, parameter)
            ? _entityType.FindProperty(member.Member.Name)
            : null;
        return property is not null;
    }

    private readonly record struct SqlOperand(string Sql, bool IsNullLiteral);
}

/// <summary>
/// The operators of a LINQ query shape that native translation understands.
/// </summary>
internal sealed class DataVoQueryShape
{
    public List<LambdaExpression> WherePredicates { get; } = [];
    public List<Ordering> Orderings { get; } = [];
    public int? Skip { get; private set; }
    public int? Take { get; private set; }
    public LambdaExpression? SelectSelector { get; private set; }

    internal sealed record Ordering(LambdaExpression KeySelector, bool Descending);

    public static DataVoQueryShape Parse(Expression expression)
    {
        var shape = new DataVoQueryShape();
        shape.ParseRecursive(expression);
        return shape;
    }

    private void ParseRecursive(Expression expression)
    {
        if (expression is not MethodCallExpression methodCall)
        {
            return;
        }

        if (methodCall.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions) &&
            methodCall.Method.Name == nameof(EntityFrameworkQueryableExtensions.AsNoTracking))
        {
            ParseRecursive(methodCall.Arguments[0]);
            return;
        }

        if (methodCall.Method.DeclaringType != typeof(Queryable))
        {
            return;
        }

        ParseRecursive(methodCall.Arguments[0]);

        switch (methodCall.Method.Name)
        {
            case nameof(Queryable.Where):
                WherePredicates.Add(DataVoExpressionHelpers.Unquote(methodCall.Arguments[1]));
                return;
            case nameof(Queryable.OrderBy):
            case nameof(Queryable.ThenBy):
                Orderings.Add(new Ordering(DataVoExpressionHelpers.Unquote(methodCall.Arguments[1]), Descending: false));
                return;
            case nameof(Queryable.OrderByDescending):
            case nameof(Queryable.ThenByDescending):
                Orderings.Add(new Ordering(DataVoExpressionHelpers.Unquote(methodCall.Arguments[1]), Descending: true));
                return;
            case nameof(Queryable.Skip):
                Skip = Convert.ToInt32(DataVoExpressionHelpers.Evaluate(methodCall.Arguments[1]), CultureInfo.InvariantCulture);
                return;
            case nameof(Queryable.Take):
                Take = Convert.ToInt32(DataVoExpressionHelpers.Evaluate(methodCall.Arguments[1]), CultureInfo.InvariantCulture);
                return;
            case nameof(Queryable.Select):
                SelectSelector = DataVoExpressionHelpers.Unquote(methodCall.Arguments[1]);
                return;
            default:
                throw new NotSupportedException($"Native query translation does not support '{methodCall.Method.Name}'.");
        }
    }
}

/// <summary>
/// Expression-tree helpers shared by the query analyzer and the native translator, so both classify
/// the same shapes the same way.
/// </summary>
internal static class DataVoExpressionHelpers
{
    public static Expression UnwrapConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    public static LambdaExpression Unquote(Expression expression)
    {
        return expression is UnaryExpression { NodeType: ExpressionType.Quote } unary
            ? (LambdaExpression)unary.Operand
            : (LambdaExpression)expression;
    }

    /// <summary>Whether <paramref name="member"/> reads a member directly off the lambda parameter.</summary>
    public static bool IsParameterMember(MemberExpression member, ParameterExpression parameter)
        => member.Expression is { } source && UnwrapConvert(source) == parameter;

    /// <summary>Whether <paramref name="expression"/> reads the lambda parameter anywhere.</summary>
    public static bool ReferencesParameter(Expression expression, ParameterExpression parameter)
    {
        var finder = new ParameterFinder(parameter);
        finder.Visit(expression);
        return finder.Found;
    }

    /// <summary>Returns every member read directly off the lambda parameter, in visit order.</summary>
    public static IReadOnlyList<MemberExpression> FindParameterMembers(Expression expression, ParameterExpression parameter)
    {
        var collector = new ParameterMemberCollector(parameter);
        collector.Visit(expression);
        return collector.Members;
    }

    public static bool IsComparison(ExpressionType nodeType) => nodeType is
        ExpressionType.Equal or ExpressionType.NotEqual or
        ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual or
        ExpressionType.LessThan or ExpressionType.LessThanOrEqual;

    /// <summary>
    /// Splits <c>predicate == true/false</c> (or <c>!=</c>) where one side is a parameter-free boolean,
    /// returning the predicate side and whether it must hold.
    /// </summary>
    public static bool TrySplitBooleanComparison(
        BinaryExpression expression,
        ParameterExpression parameter,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Expression? predicate,
        out bool expected)
    {
        predicate = null;
        expected = false;

        if (expression.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual) ||
            expression.Left.Type != typeof(bool) || expression.Right.Type != typeof(bool))
        {
            return false;
        }

        Expression constantSide;
        if (!ReferencesParameter(expression.Right, parameter))
        {
            predicate = expression.Left;
            constantSide = expression.Right;
        }
        else if (!ReferencesParameter(expression.Left, parameter))
        {
            predicate = expression.Right;
            constantSide = expression.Left;
        }
        else
        {
            return false;
        }

        bool value = Equals(Evaluate(constantSide), true);
        expected = value == (expression.NodeType == ExpressionType.Equal);
        return true;
    }

    /// <summary>
    /// Matches a <see cref="DataVoVectorDbFunctions"/> distance call and returns its vector operands and
    /// DataVo operator (<c>&lt;=&gt;</c> for cosine, <c>&lt;-&gt;</c> for L2).
    /// </summary>
    public static bool TryGetVectorDistanceOperands(
        MethodCallExpression expression,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Expression? left,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Expression? right,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? operatorToken)
    {
        left = right = null;
        operatorToken = expression.Method.DeclaringType != typeof(DataVoVectorDbFunctions)
            ? null
            : expression.Method.Name switch
            {
                nameof(DataVoVectorDbFunctions.CosineDistance) => "<=>",
                nameof(DataVoVectorDbFunctions.L2Distance) => "<->",
                _ => null
            };

        // Extension-method form includes EF.Functions as the first argument.
        int start = expression.Arguments.Count == 3 ? 1 : 0;
        if (operatorToken is null || expression.Arguments.Count - start != 2)
        {
            operatorToken = null;
            return false;
        }

        left = expression.Arguments[start];
        right = expression.Arguments[start + 1];
        return true;
    }

    /// <summary>Evaluates a parameter-free expression (a constant or captured variable).</summary>
    public static object? Evaluate(Expression expression)
    {
        if (expression is ConstantExpression constant)
        {
            return constant.Value;
        }

        return Expression.Lambda<Func<object?>>(Expression.Convert(expression, typeof(object))).Compile().Invoke();
    }

    /// <summary>Whether a parameter-free expression can be evaluated client-side.</summary>
    public static bool TryEvaluate(Expression expression)
    {
        try
        {
            _ = Evaluate(expression);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private sealed class ParameterFinder(ParameterExpression parameter) : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            Found |= node == parameter;
            return node;
        }
    }

    private sealed class ParameterMemberCollector(ParameterExpression parameter) : ExpressionVisitor
    {
        public List<MemberExpression> Members { get; } = [];

        protected override Expression VisitMember(MemberExpression node)
        {
            if (IsParameterMember(node, parameter))
            {
                Members.Add(node);
            }

            return base.VisitMember(node);
        }
    }
}
