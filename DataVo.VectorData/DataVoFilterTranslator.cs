using System.Collections;
using System.Globalization;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Text;
using DataVo.Core;

namespace DataVo.VectorData;

/// <summary>
/// Translates a LINQ filter over records (<c>r =&gt; r.Category == "docs" &amp;&amp; r.UpdatedAt &gt; since</c>) into a
/// DataVo SQL <c>WHERE</c> predicate whose values are bound as parameters. Supported: comparisons of a property with
/// a value (either side), <c>==</c>/<c>!=</c> <c>null</c>, <c>&amp;&amp;</c>, <c>||</c>, <c>!</c>, a bool property on
/// its own, <c>string.StartsWith</c>/<c>EndsWith</c>/<c>Contains</c> on a property (as <c>LIKE</c>, with <c>%</c>, <c>_</c>
/// and backslash in the value escaped so they match literally), and <c>Contains</c> of a property in a collection of
/// values (<c>IN</c>). Values may be constants, captured variables, constructors (<c>new DateTime(2026, 1, 1)</c>),
/// arithmetic (<c>min + 1</c>, <c>-x</c>), conditionals and method calls; they are evaluated without compiling the
/// expression, so translation is Native AOT safe. Anything else throws <see cref="NotSupportedException"/>.
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
        // string methods on a property: LIKE, with the value's wildcards and escape character escaped.
        if (call.Method.DeclaringType == typeof(string) && call.Object is { } target && call.Arguments.Count == 1
            && call.Method.Name is nameof(string.StartsWith) or nameof(string.EndsWith) or nameof(string.Contains)
            && call.Arguments[0].Type == typeof(string)
            && PropertyOf(StripConvert(target)) is { } textProperty)
        {
            if (ReferencesRecord(call.Arguments[0]) || Evaluate(call.Arguments[0]) is not string value)
            {
                throw new NotSupportedException($"'{call}' needs a non-null string value.");
            }

            string text = DataVoLikePattern.Escape(value);
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
                if (operand is null)
                {
                    return null;
                }

                return convert.Method is not null ? convert.Method.Invoke(null, [operand]) : ConvertPrimitive(operand, convert.Type);
            case UnaryExpression { NodeType: ExpressionType.ArrayLength } length:
                return ((Array)Evaluate(length.Operand)!).Length;
            case UnaryExpression unary:
                return EvaluateUnary(unary);
            case BinaryExpression { NodeType: ExpressionType.ArrayIndex } index:
                return ((Array)Evaluate(index.Left)!).GetValue(Convert.ToInt64(Evaluate(index.Right), System.Globalization.CultureInfo.InvariantCulture));
            case BinaryExpression binary:
                return EvaluateBinary(binary);
            case ConditionalExpression conditional:
                return Evaluate(conditional.Test) is true ? Evaluate(conditional.IfTrue) : Evaluate(conditional.IfFalse);
            case NewExpression created:
                return EvaluateNew(created);
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
    /// Evaluates a constructor call such as <c>new DateTime(2026, 1, 1)</c> or <c>new Guid("...")</c> through its
    /// <see cref="ConstructorInfo"/> (kept by the expression tree), or the default value of a known struct for a
    /// parameterless <c>new T()</c>.
    /// </summary>
    private static object? EvaluateNew(NewExpression created)
    {
        if (created.Constructor is null)
        {
            Type type = created.Type;
            return type == typeof(DateTime) ? default(DateTime)
                : type == typeof(DateTimeOffset) ? default(DateTimeOffset)
                : type == typeof(DateOnly) ? default(DateOnly)
                : type == typeof(TimeOnly) ? default(TimeOnly)
                : type == typeof(TimeSpan) ? default(TimeSpan)
                : type == typeof(Guid) ? default(Guid)
                : type == typeof(decimal) ? default(decimal)
                : throw Unsupported(created);
        }

        object?[] arguments = [.. created.Arguments.Select(Evaluate)];
        return created.Constructor.Invoke(arguments);
    }

    /// <summary>Evaluates negation, logical/bitwise not, unary plus and checked conversions of a value.</summary>
    private static object? EvaluateUnary(UnaryExpression unary)
    {
        object? operand = Evaluate(unary.Operand);
        if (unary.NodeType is ExpressionType.Quote)
        {
            throw Unsupported(unary);
        }

        if (operand is null)
        {
            return null; // lifted operators propagate null
        }

        if (unary.Method is not null)
        {
            return unary.Method.Invoke(null, [operand]);
        }

        return unary.NodeType switch
        {
            ExpressionType.UnaryPlus => operand,
            ExpressionType.Not when operand is bool flag => !flag,
            ExpressionType.Not or ExpressionType.OnesComplement => Integer(unary, operand, operand, static (a, _) => ~a),
            ExpressionType.Negate or ExpressionType.NegateChecked => Negate(unary, operand),
            ExpressionType.TypeAs => operand,
            _ => throw Unsupported(unary),
        };
    }

    /// <summary>
    /// Evaluates arithmetic, comparison, logical, bitwise and coalescing operators. Operator methods (such as
    /// <c>DateTime + TimeSpan</c> or <c>decimal * decimal</c>) are invoked; primitive operands use generic math.
    /// </summary>
    private static object? EvaluateBinary(BinaryExpression binary)
    {
        switch (binary.NodeType)
        {
            case ExpressionType.Coalesce when binary.Conversion is null:
                return Evaluate(binary.Left) ?? Evaluate(binary.Right);
            case ExpressionType.AndAlso:
                return Evaluate(binary.Left) is true && Evaluate(binary.Right) is true;
            case ExpressionType.OrElse:
                return Evaluate(binary.Left) is true || Evaluate(binary.Right) is true;
        }

        object? left = Evaluate(binary.Left);
        object? right = Evaluate(binary.Right);
        if (left is null || right is null)
        {
            // Lifted operators: comparisons of null are false (== compares both for null), arithmetic is null.
            return binary.NodeType switch
            {
                ExpressionType.Equal => left is null && right is null,
                ExpressionType.NotEqual => !(left is null && right is null),
                ExpressionType.LessThan or ExpressionType.LessThanOrEqual or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual => false,
                _ => null,
            };
        }

        if (binary.Method is not null)
        {
            return binary.Method.Invoke(null, [left, right]);
        }

        if (left is bool l && right is bool r)
        {
            return binary.NodeType switch
            {
                ExpressionType.And => l & r,
                ExpressionType.Or => l | r,
                ExpressionType.ExclusiveOr => l ^ r,
                ExpressionType.Equal => l == r,
                ExpressionType.NotEqual => l != r,
                _ => throw Unsupported(binary),
            };
        }

        if (binary.NodeType is ExpressionType.Equal or ExpressionType.NotEqual && left.GetType() == right.GetType() && !IsNumber(left))
        {
            return Equals(left, right) == (binary.NodeType == ExpressionType.Equal);
        }

        return left switch
        {
            int a => Number(binary, a, (int)right),
            long a => Number(binary, a, (long)right),
            uint a => Number(binary, a, (uint)right),
            ulong a => Number(binary, a, (ulong)right),
            short a => Number(binary, a, (short)right),
            ushort a => Number(binary, a, (ushort)right),
            byte a => Number(binary, a, (byte)right),
            sbyte a => Number(binary, a, (sbyte)right),
            float a => Number(binary, a, (float)right),
            double a => Number(binary, a, (double)right),
            decimal a => Number(binary, a, (decimal)right),
            _ => throw Unsupported(binary),
        };
    }

    private static bool IsNumber(object value) =>
        value is int or long or uint or ulong or short or ushort or byte or sbyte or float or double or decimal;

    /// <summary>Applies an arithmetic, comparison or (for integers) bitwise operator to two primitive numbers of one type.</summary>
    private static object Number<T>(BinaryExpression node, T a, T b)
        where T : INumber<T>
    {
        switch (node.NodeType)
        {
            case ExpressionType.Add: return a + b;
            case ExpressionType.AddChecked: return checked(a + b);
            case ExpressionType.Subtract: return a - b;
            case ExpressionType.SubtractChecked: return checked(a - b);
            case ExpressionType.Multiply: return a * b;
            case ExpressionType.MultiplyChecked: return checked(a * b);
            case ExpressionType.Divide: return a / b;
            case ExpressionType.Modulo: return a % b;
            case ExpressionType.Equal: return a == b;
            case ExpressionType.NotEqual: return a != b;
            case ExpressionType.LessThan: return a < b;
            case ExpressionType.LessThanOrEqual: return a <= b;
            case ExpressionType.GreaterThan: return a > b;
            case ExpressionType.GreaterThanOrEqual: return a >= b;
        }

        return node.NodeType switch
        {
            ExpressionType.And => Integer(node, a, b, static (x, y) => x & y),
            ExpressionType.Or => Integer(node, a, b, static (x, y) => x | y),
            ExpressionType.ExclusiveOr => Integer(node, a, b, static (x, y) => x ^ y),
            ExpressionType.LeftShift => Integer(node, a, b, static (x, y) => x << (int)y),
            ExpressionType.RightShift => Integer(node, a, b, static (x, y) => x >> (int)y),
            _ => throw Unsupported(node),
        };
    }

    /// <summary>Applies an integer-only operator, widening through <see cref="long"/>/<see cref="ulong"/> and back to the operand type.</summary>
    private static object Integer(Expression node, object a, object b, Func<long, long, long> op)
    {
        Type type = a.GetType();
        if (type == typeof(ulong))
        {
            return (ulong)op(unchecked((long)(ulong)a), unchecked((long)(ulong)b));
        }

        if (!(a is int or long or uint or short or ushort or byte or sbyte) || b.GetType() != type)
        {
            throw Unsupported(node);
        }

        long result = op(Convert.ToInt64(a, CultureInfo.InvariantCulture), Convert.ToInt64(b, CultureInfo.InvariantCulture));
        return Convert.ChangeType(unchecked(type == typeof(uint) ? (uint)result : result), type, CultureInfo.InvariantCulture);
    }

    private static object Negate(UnaryExpression node, object operand) => operand switch
    {
        int a => node.NodeType == ExpressionType.NegateChecked ? checked(-a) : -a,
        long a => node.NodeType == ExpressionType.NegateChecked ? checked(-a) : -a,
        short a => -a,
        sbyte a => -a,
        float a => -a,
        double a => -a,
        decimal a => -a,
        TimeSpan a => -a,
        _ => throw Unsupported(node),
    };

    /// <summary>Converts a primitive value to another primitive (numeric) type, as an implicit or explicit C# cast does.</summary>
    private static object ConvertPrimitive(object operand, Type target)
    {
        Type type = Nullable.GetUnderlyingType(target) ?? target;
        if (type == typeof(object) || type.IsInstanceOfType(operand) || type.IsEnum || operand.GetType().IsEnum
            || !IsNumber(operand) || !(type.IsPrimitive || type == typeof(decimal)))
        {
            return operand;
        }

        return Convert.ChangeType(operand, type, CultureInfo.InvariantCulture);
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
