using System.Globalization;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace DataVo.Generators.Linq;

/// <summary>
/// Translates the setters lambda of an <c>ExecuteUpdate</c> call into an UPDATE's SET list over <c>@u0</c>,
/// <c>@u1</c>..., and the C# that binds those parameters from the call's state argument.
/// </summary>
/// <remarks>
/// The lambda must be a chain of <c>Set</c> and <c>SetNull</c> calls on its first parameter. Each sets one stored
/// column, other than the primary key, to a value (a literal, the state argument, a field or property of it, or a
/// static member), to null, or to a sum, difference or product of the row's own columns and such values:
/// <c>set.Set(p =&gt; p.Gold, p =&gt; p.Gold + bonus)</c> becomes <c>Gold = Gold + @u0</c>.
/// </remarks>
/// <param name="semantics">The semantic model of the file the call is in.</param>
/// <param name="model">The model the call updates.</param>
/// <param name="setters">The lambda's setters parameter.</param>
/// <param name="state">The lambda's state parameter, or <see langword="null"/> for the overload without state.</param>
/// <param name="cancellation">Cancels the analysis.</param>
internal sealed class LinqUpdateTranslator(SemanticModel semantics, TableModel model, IParameterSymbol setters, IParameterSymbol? state, CancellationToken cancellation)
    : LinqExpressionReader(semantics, state, cancellation)
{
    private const string Core = "global::DataVo.Core.";
    private const string Parameter = Core + "DataVoQueryParameter";
    private const string Support = Core + "Annotations.DataVoGeneratedTableSupport";

    private readonly HashSet<string> _assigned = new(StringComparer.Ordinal);

    /// <summary>Gets the C# that binds each <c>@u</c> parameter, in order.</summary>
    public List<string> Values { get; } = [];

    /// <summary>Translates the lambda's body.</summary>
    /// <param name="body">The chain of <c>Set</c> calls.</param>
    /// <returns>The SET list, or <see langword="null"/> when the body cannot be translated.</returns>
    public string? Translate(ExpressionSyntax body)
    {
        var calls = new List<(InvocationExpressionSyntax Call, IMethodSymbol Method)>();
        ExpressionSyntax current = Unwrap(body);
        while (current is InvocationExpressionSyntax { Expression: MemberAccessExpressionSyntax access } call
            && Semantics.GetSymbolInfo(call, Cancellation).Symbol is IMethodSymbol { Name: "Set" or "SetNull", ContainingType.Name: "DataVoSetters" } method)
        {
            calls.Add((call, method));
            current = Unwrap(access.Expression);
        }

        if (current is not IdentifierNameSyntax receiver || !SymbolEqualityComparer.Default.Equals(Semantics.GetSymbolInfo(receiver, Cancellation).Symbol, setters))
        {
            return Fail($"'{Shorten(body)}' is not a chain of Set calls on the setters, such as set => set.Set(p => p.Gold, 0)");
        }

        if (calls.Count == 0)
        {
            return Fail("it sets no column");
        }

        calls.Reverse(); // the outermost call is the last one written
        var assignments = new List<string>(calls.Count);
        foreach ((InvocationExpressionSyntax call, IMethodSymbol method) in calls)
        {
            if (Assignment(call, method) is not { } assignment)
            {
                return null;
            }

            assignments.Add(assignment);
        }

        return string.Join(", ", assignments);
    }

    /// <summary>Translates one <c>Set</c> or <c>SetNull</c> call into <c>Column = value</c>.</summary>
    private string? Assignment(InvocationExpressionSyntax call, IMethodSymbol method)
    {
        SeparatedSyntaxList<ArgumentSyntax> arguments = call.ArgumentList.Arguments;
        if (TargetColumn(arguments[0].Expression, method.TypeArguments[0]) is not { } target)
        {
            return null;
        }

        string name = target.Column.PropertyName;
        if (method.Name == "SetNull")
        {
            return target.Column.IsNullable ? $"{name} = NULL" : Fail($"column {name} cannot hold null");
        }

        ExpressionSyntax value = arguments[1].Expression;
        bool computed = method.Parameters[1].Type is INamedTypeSymbol { Name: "Func" };
        string? text = computed ? Computed(value, target.Column, target.Property) : Value(value, target.Column, method.TypeArguments[0]);
        return text is null ? null : $"{name} = {text}";
    }

    /// <summary>The column a <c>p =&gt; p.Column</c> lambda names, when it is stored, not the key, not set twice, and not converted.</summary>
    private (ColumnModel Column, IPropertySymbol Property)? TargetColumn(ExpressionSyntax expression, ITypeSymbol valueType)
    {
        if (expression is not SimpleLambdaExpressionSyntax { ExpressionBody: { } body } lambda
            || Semantics.GetDeclaredSymbol(lambda.Parameter, Cancellation) is not { } row
            || ColumnOf(body, row) is not { } target)
        {
            Fail($"'{Shorten(expression)}' does not name a column; write it as p => p.Column");
            return null;
        }

        string name = target.Column.PropertyName;
        if (target.Column.IsPrimaryKey)
        {
            Fail($"it sets the primary key {name}, which ExecuteUpdate does not change");
            return null;
        }

        if (!SymbolEqualityComparer.Default.Equals(valueType, target.Property.Type))
        {
            Fail($"it sets column {name} through a value of type {valueType.ToDisplayString()}; use the column's own type {target.Property.Type.ToDisplayString()}");
            return null;
        }

        if (!_assigned.Add(name))
        {
            Fail($"it sets column {name} twice");
            return null;
        }

        return target;
    }

    /// <summary>Binds a value that does not read the row: a literal, null, the state, a field or property of it, or a static member.</summary>
    private string? Value(ExpressionSyntax expression, ColumnModel column, ITypeSymbol valueType)
    {
        Optional<object?> constant = Semantics.GetConstantValue(expression, Cancellation);
        if (constant.HasValue && constant.Value is null)
        {
            return column.IsNullable ? "NULL" : Fail($"column {column.PropertyName} cannot hold null");
        }

        ITypeSymbol core = DataVoLinqAnalyzer.StripNullable(valueType)!;
        if (constant.HasValue)
        {
            return Literal(constant.Value!, core) is { } literal && !column.Type.IsVector3 && !column.Type.UsesObjectParameter
                ? Bind(name => $"new {Parameter}({name}, {literal})")
                : Fail($"the constant '{Shorten(expression)}' cannot be written as a {core.ToDisplayString()}");
        }

        if (ValueText(Unwrap(expression)) is not { } text)
        {
            return null;
        }

        ITypeSymbol? actual = Semantics.GetTypeInfo(expression, Cancellation).Type;
        bool mayBeNull = actual is { IsReferenceType: true } || actual is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T };
        string cast = valueType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        string coreCast = core.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        if (column.Type.IsVector3)
        {
            return Bind(name => $"{Support}.Vector3Parameter({name}, ({cast})({text}))");
        }

        if (column.Type.IsVector)
        {
            // float[] and ReadOnlyMemory<float> bind as VECTOR(n) values, as the table's own Update does.
            return Bind(name => $"{Support}.VectorParameter({name}, ({cast})({text}))");
        }

        if (column.Type.UsesObjectParameter)
        {
            return Bind(name => $"{Parameter}.FromObject({name}, ({cast})({text}))");
        }

        return mayBeNull
            ? Bind(name => $"({text}) == null ? {Parameter}.Null({name}) : new {Parameter}({name}, ({coreCast})({text}))")
            : Bind(name => $"new {Parameter}({name}, ({coreCast})({text}))");
    }

    /// <summary>Translates <c>p =&gt; expression</c> over the row's columns into SQL.</summary>
    private string? Computed(ExpressionSyntax expression, ColumnModel column, IPropertySymbol property)
    {
        if (expression is not SimpleLambdaExpressionSyntax { ExpressionBody: { } body } lambda
            || Semantics.GetDeclaredSymbol(lambda.Parameter, Cancellation) is not { } row)
        {
            return Fail($"'{Shorten(expression)}' must be a lambda with one expression, such as p => p.Gold + 1");
        }

        bool numeric = column.Type.SqlType is "INT" or "BIGINT" or "FLOAT" or "DOUBLE";
        return Operand(body, row, column, DataVoLinqAnalyzer.StripNullable(property.Type)!, numeric);
    }

    /// <summary>One operand of a computed value: a column, a value, or <c>+</c>, <c>-</c>, <c>*</c> of operands.</summary>
    private string? Operand(ExpressionSyntax expression, IParameterSymbol row, ColumnModel target, ITypeSymbol type, bool arithmetic)
    {
        switch (expression)
        {
            case ParenthesizedExpressionSyntax parenthesized:
                return Operand(parenthesized.Expression, row, target, type, arithmetic) is { } inner ? $"({inner})" : null;

            case BinaryExpressionSyntax binary when arithmetic && binary.Kind() is SyntaxKind.AddExpression or SyntaxKind.SubtractExpression or SyntaxKind.MultiplyExpression:
            {
                string? left = Operand(binary.Left, row, target, type, arithmetic);
                string? right = Operand(binary.Right, row, target, type, arithmetic);
                return left is null || right is null ? null : $"{left} {binary.OperatorToken.Text} {right}";
            }
        }

        if (ColumnOf(expression, row) is { } source)
        {
            return SymbolEqualityComparer.Default.Equals(DataVoLinqAnalyzer.StripNullable(source.Property.Type), type)
                ? source.Column.PropertyName
                : Fail($"'{Shorten(expression)}' mixes column {source.Column.PropertyName} with a column of another type");
        }

        if (expression.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>().Any(name => SymbolEqualityComparer.Default.Equals(Semantics.GetSymbolInfo(name, Cancellation).Symbol, row)))
        {
            return Fail($"'{Shorten(expression)}' is not a column, a value, or +, - or * of them");
        }

        if (!SymbolEqualityComparer.Default.Equals(DataVoLinqAnalyzer.StripNullable(Semantics.GetTypeInfo(expression, Cancellation).ConvertedType), type))
        {
            return Fail($"'{Shorten(expression)}' is not a {type.ToDisplayString()}, the type of column {target.PropertyName}");
        }

        return Semantics.GetConstantValue(expression, Cancellation) is { HasValue: true, Value: null }
            ? Fail($"'{Shorten(expression)}' computes with null")
            : Value(expression, target, type);
    }

    /// <summary>The stored column a <c>p.Column</c> expression reads, or null.</summary>
    private (ColumnModel Column, IPropertySymbol Property)? ColumnOf(ExpressionSyntax expression, IParameterSymbol row)
    {
        if (Unwrap(expression) is not MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax receiver } access
            || !SymbolEqualityComparer.Default.Equals(Semantics.GetSymbolInfo(receiver, Cancellation).Symbol, row)
            || Semantics.GetSymbolInfo(access, Cancellation).Symbol is not IPropertySymbol property
            || model.Columns.FirstOrDefault(candidate => candidate.PropertyName == property.Name) is not { } column)
        {
            return null;
        }

        return (column, property);
    }

    /// <summary>Adds the next <c>@u</c> parameter; <paramref name="binding"/> writes its C# from the name's string literal.</summary>
    private string Bind(Func<string, string> binding)
    {
        string name = "u" + Values.Count.ToString(CultureInfo.InvariantCulture);
        Values.Add(binding(SymbolDisplay.FormatLiteral(name, quote: true)));
        return "@" + name;
    }

    private static ExpressionSyntax Unwrap(ExpressionSyntax expression)
    {
        while (expression is ParenthesizedExpressionSyntax parenthesized)
        {
            expression = parenthesized.Expression;
        }

        return expression;
    }
}
