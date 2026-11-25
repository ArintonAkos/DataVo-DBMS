using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using DataVo.Core;

namespace DataVo.VectorData;

/// <summary>
/// Translates a LINQ filter over records (<c>r =&gt; r.Category == "docs" &amp;&amp; r.UpdatedAt &gt; since</c>) into a
/// DataVo SQL <c>WHERE</c> predicate whose values are bound as parameters. Supported: comparisons of a property with
/// a value (either side), <c>==</c>/<c>!=</c> <c>null</c>, <c>&amp;&amp;</c>, <c>||</c>, <c>!</c>, a bool property on
/// its own, <c>string.StartsWith</c>/<c>EndsWith</c>/<c>Contains</c> on a property (as <c>LIKE</c>, which has no escape
/// character: a <c>StartsWith</c> value holding <c>%</c> or <c>_</c> becomes an ordinal range instead), and <c>Contains</c> of a property
/// in a collection of values (<c>IN</c>). Values may be constants or captured variables; they are evaluated without
/// compiling the expression, so translation is Native AOT safe. Anything else throws <see cref="NotSupportedException"/>.
/// </summary>
internal sealed class DataVoFilterTranslator
{
    private readonly DataVoCollectionModel _model;
    private readonly ParameterExpression _record;
    private readonly List<DataVoQueryParameter> _parameters;
    private readonly StringBuilder _sql = new();

    private DataVoFilterTranslator(DataVoCollectionModel model, ParameterExpression record, List<DataVoQueryParameter> parameters)
    {
        _model = model;
        _record = record;
        _parameters = parameters;
    }

    /// <summary>The SQL predicate of <paramref name="filter"/>; its values are appended to <paramref name="parameters"/>.</summary>
    public static string Translate(LambdaExpression filter, DataVoCollectionModel model, List<DataVoQueryParameter> parameters)
    {
        var translator = new DataVoFilterTranslator(model, filter.Parameters[0], parameters);
        translator.Predicate(filter.Body);
        return translator._sql.ToString();
    }

    /// <summary>The model property a member selector such as <c>r =&gt; r.Name</c> (or <c>r =&gt; r["Name"]</c>) names.</summary>
    public static DataVoPropertyModel SelectedProperty(LambdaExpression selector, DataVoCollectionModel model) =>
        new DataVoFilterTranslator(model, selector.Parameters[0], []).PropertyOf(StripConvert(selector.Body))
            ?? throw new NotSupportedException($"'{selector}' does not select a property of the record.");

    private void Predicate(Expression node)
    {
        node = StripConvert(node);
        switch (node)
        {
            case BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.OrElse } logical:
                _sql.Append('(');
                Predicate(logical.Left);
                _sql.Append(logical.NodeType == ExpressionType.AndAlso ? " AND " : " OR ");
                Predicate(logical.Right);
                _sql.Append(')');
                return;
            case UnaryExpression { NodeType: ExpressionType.Not } not:
                _sql.Append("NOT (");
                Predicate(not.Operand);
                _sql.Append(')');
                return;
            case BinaryExpression comparison when ComparisonOperator(comparison.NodeType) is { } op:
                Comparison(comparison, op);
                return;
            case MethodCallExpression call:
                MethodPredicate(call);
                return;
            case ConstantExpression { Value: bool constant }:
                _sql.Append(constant ? "1 = 1" : "1 = 0");
                return;
        }

        if (PropertyOf(node) is { } flag && (Nullable.GetUnderlyingType(flag.Type) ?? flag.Type) == typeof(bool))
        {
            _sql.Append(flag.Column).Append(" = TRUE");
            return;
        }

        throw Unsupported(node);
    }

    private void Comparison(BinaryExpression comparison, string op)
    {
        Expression left = StripConvert(comparison.Left);
        Expression right = StripConvert(comparison.Right);
        DataVoPropertyModel? property = PropertyOf(left);
        Expression value = right;
        if (property is null)
        {
            property = PropertyOf(right);
            value = left;
            op = Mirror(op);
        }

        if (property is null || ReferencesRecord(value))
        {
            throw Unsupported(comparison);
        }

        RequireFilterable(property, comparison);
        object? evaluated = Evaluate(value);
        if (evaluated is null)
        {
            _sql.Append(property.Column).Append(op switch
            {
                "=" => " IS NULL",
                "<>" => " IS NOT NULL",
                _ => throw new NotSupportedException($"'{comparison}' compares with null using {op}."),
            });
            return;
        }

        _sql.Append(property.Column).Append(' ').Append(op).Append(' ').Append(Bind(evaluated));
    }

    private void MethodPredicate(MethodCallExpression call)
    {
        // string methods on a property: LIKE (or an ordinal range for a StartsWith prefix holding a wildcard).
        if (call.Method.DeclaringType == typeof(string) && call.Object is { } target && call.Arguments.Count == 1
            && call.Method.Name is nameof(string.StartsWith) or nameof(string.EndsWith) or nameof(string.Contains)
            && PropertyOf(StripConvert(target)) is { } textProperty)
        {
            if (Evaluate(call.Arguments[0]) is not string text)
            {
                throw new NotSupportedException($"'{call}' needs a non-null string value.");
            }

            bool wildcards = text.AsSpan().IndexOfAny('%', '_') >= 0;
            if (call.Method.Name == nameof(string.StartsWith) && wildcards)
            {
                StartsWithRange(textProperty, text, call);
                return;
            }

            if (wildcards)
            {
                throw new NotSupportedException(
                    $"'{call}': DataVo LIKE has no escape character, so {call.Method.Name} cannot match text containing '%' or '_'.");
            }

            string pattern = call.Method.Name switch
            {
                nameof(string.StartsWith) => text + "%",
                nameof(string.EndsWith) => "%" + text,
                _ => "%" + text + "%",
            };
            _sql.Append(textProperty.Column).Append(" LIKE ").Append(Bind(pattern));
            return;
        }

        // values.Contains(r.Property): Enumerable.Contains, List<T>.Contains, or (C# 14) MemoryExtensions.Contains over a span.
        if (call.Method.Name == nameof(Enumerable.Contains) && TryGetContains(call, out Expression? source, out Expression? item)
            && PropertyOf(StripConvert(item)) is { } member && !ReferencesRecord(source))
        {
            RequireFilterable(member, call);
            if (Evaluate(UnwrapSpan(source)) is not IEnumerable values)
            {
                throw new NotSupportedException($"'{call}' needs a collection of values.");
            }

            var bound = new List<string>();
            bool hasNull = false;
            foreach (object? value in values)
            {
                if (value is null)
                {
                    hasNull = true;
                    continue;
                }

                bound.Add(Bind(value));
            }

            if (bound.Count == 0)
            {
                _sql.Append(hasNull ? $"{member.Column} IS NULL" : "1 = 0");
                return;
            }

            _sql.Append('(').Append(member.Column).Append(" IN (").AppendJoin(", ", bound).Append(')');
            if (hasNull)
            {
                _sql.Append(" OR ").Append(member.Column).Append(" IS NULL");
            }

            _sql.Append(')');
            return;
        }

        if (call.Method.Name == nameof(Equals) && call.Arguments.Count == 1 && call.Object is { } instance)
        {
            Comparison(Expression.Equal(StripConvert(instance), StripConvert(call.Arguments[0]), false, null), "=");
            return;
        }

        throw Unsupported(call);
    }

    private static bool TryGetContains(MethodCallExpression call, out Expression source, out Expression item)
    {
        if (call.Object is null && call.Arguments.Count == 2)
        {
            source = call.Arguments[0];
            item = call.Arguments[1];
            return true;
        }

        if (call.Object is not null && call.Arguments.Count == 1)
        {
            source = call.Object;
            item = call.Arguments[0];
            return true;
        }

        source = item = null!;
        return false;
    }

    /// <summary>C# 14 binds <c>array.Contains(x)</c> to MemoryExtensions over an implicit span conversion; the array is what holds the values.</summary>
    private static Expression UnwrapSpan(Expression source) =>
        source is MethodCallExpression { Method.Name: "op_Implicit", Arguments.Count: 1 } conversion
            && conversion.Method.ReturnType.IsGenericType
            && conversion.Method.ReturnType.GetGenericTypeDefinition() is var definition
            && (definition == typeof(ReadOnlySpan<>) || definition == typeof(Span<>))
            ? conversion.Arguments[0]
            : StripConvert(source);

    /// <summary>The model property <paramref name="node"/> reads from the record (<c>r.Name</c> or <c>r["Name"]</c>); null otherwise.</summary>
    private DataVoPropertyModel? PropertyOf(Expression node)
    {
        string? name = node switch
        {
            MemberExpression { Expression: { } owner } member when StripConvert(owner) == _record => member.Member.Name,
            MethodCallExpression { Method.Name: "get_Item", Object: { } owner, Arguments: [var index] } when StripConvert(owner) == _record
                => Evaluate(index) as string,
            _ => null,
        };
        if (name is null)
        {
            return null;
        }

        return _model.TryGetProperty(name, out DataVoPropertyModel? property)
            ? property
            : throw new NotSupportedException($"'{name}' is not a mapped property of the record.");
    }

    private static void RequireFilterable(DataVoPropertyModel property, Expression node)
    {
        if (property.Role == DataVoPropertyRole.Vector || !DataVoValueMapper.IsFilterable(Nullable.GetUnderlyingType(property.Type) ?? property.Type))
        {
            throw new NotSupportedException($"'{node}' filters on '{property.Name}', which cannot be compared in a filter.");
        }
    }

    private string Bind(object value)
    {
        string name = "f" + _parameters.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
        _parameters.Add(DataVoQueryParameter.FromObject(name, DataVoValueMapper.ToColumn(value)));
        return "@" + name;
    }

    private bool ReferencesRecord(Expression node) => new RecordReferenceFinder(_record).Finds(node);

    /// <summary>Evaluates an expression that does not read the record (constants, captured variables, members, calls) without compiling it.</summary>
    private static object? Evaluate(Expression node)
    {
        switch (node)
        {
            case ConstantExpression constant:
                return constant.Value;
            case MemberExpression member:
                object? owner = member.Expression is null ? null : Evaluate(member.Expression);
                return member.Member switch
                {
                    FieldInfo field => field.GetValue(owner),
                    PropertyInfo property => property.GetValue(owner),
                    _ => throw Unsupported(node),
                };
            case UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert:
                object? operand = Evaluate(convert.Operand);
                if (operand is null || convert.Method is null)
                {
                    return operand;
                }

                return convert.Method.Invoke(null, [operand]);
            case UnaryExpression { NodeType: ExpressionType.ArrayLength } length:
                return ((Array)Evaluate(length.Operand)!).Length;
            case BinaryExpression { NodeType: ExpressionType.ArrayIndex } index:
                return ((Array)Evaluate(index.Left)!).GetValue(Convert.ToInt64(Evaluate(index.Right), System.Globalization.CultureInfo.InvariantCulture));
            case NewArrayExpression { NodeType: ExpressionType.NewArrayInit } array:
                var values = new object?[array.Expressions.Count];
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = Evaluate(array.Expressions[i]);
                }

                return values;
            case MethodCallExpression call:
                object? instance = call.Object is null ? null : Evaluate(call.Object);
                object?[] arguments = [.. call.Arguments.Select(Evaluate)];
                return call.Method.Invoke(instance, arguments);
            default:
                throw Unsupported(node);
        }
    }

    private static Expression StripConvert(Expression node)
    {
        while (node is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs, Method: null } convert)
        {
            node = convert.Operand;
        }

        return node;
    }

    private static string? ComparisonOperator(ExpressionType type) => type switch
    {
        ExpressionType.Equal => "=",
        ExpressionType.NotEqual => "<>",
        ExpressionType.LessThan => "<",
        ExpressionType.LessThanOrEqual => "<=",
        ExpressionType.GreaterThan => ">",
        ExpressionType.GreaterThanOrEqual => ">=",
        _ => null,
    };

    private static string Mirror(string op) => op switch
    {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        _ => op,
    };

    /// <summary>
    /// <c>StartsWith</c> as the ordinal range <c>[prefix, prefix with its last character incremented)</c>, exact for any
    /// prefix (DataVo compares text ordinally), used when the prefix holds a LIKE wildcard.
    /// </summary>
    private void StartsWithRange(DataVoPropertyModel property, string prefix, Expression node)
    {
        if (prefix[^1] == char.MaxValue)
        {
            throw new NotSupportedException($"'{node}': the prefix cannot be turned into a range.");
        }

        string upper = string.Concat(prefix.AsSpan(0, prefix.Length - 1), ((char)(prefix[^1] + 1)).ToString());
        _sql.Append('(').Append(property.Column).Append(" >= ").Append(Bind(prefix))
            .Append(" AND ").Append(property.Column).Append(" < ").Append(Bind(upper)).Append(')');
    }

    private static NotSupportedException Unsupported(Expression node) =>
        new($"The DataVo vector store cannot translate the filter expression '{node}'.");

    /// <summary>Finds whether an expression reads the record parameter.</summary>
    private sealed class RecordReferenceFinder(ParameterExpression record) : ExpressionVisitor
    {
        private bool _found;

        public bool Finds(Expression node)
        {
            _found = false;
            Visit(node);
            return _found;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            _found |= node == record;
            return node;
        }
    }
}
