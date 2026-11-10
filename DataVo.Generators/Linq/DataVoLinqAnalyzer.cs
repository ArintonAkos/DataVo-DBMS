using System.Collections.Immutable;
using System.Globalization;
using System.Reflection;
using DataVo.Generators.Diagnostics;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace DataVo.Generators.Linq;

/// <summary>Translates a <c>DataVoTable&lt;T&gt;</c> query's lambda into a SQL condition and its parameter bindings.</summary>
internal static class DataVoLinqAnalyzer
{
    private const string TableAttribute = "DataVo.Core.Annotations.DataVoTableAttribute";

    private static readonly SymbolDisplayFormat TypeFormat =
        SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
            SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions |
            SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    // Roslyn 4.11 added the encoded location; the generator is built against 4.10 for Unity, so it is found at run time.
    private static readonly MethodInfo? GetInterceptableLocation = typeof(Microsoft.CodeAnalysis.CSharp.CSharpExtensions).GetMethod(
        "GetInterceptableLocation",
        [typeof(SemanticModel), typeof(InvocationExpressionSyntax), typeof(CancellationToken)]);

    /// <summary>Forces the path form of <c>[InterceptsLocation]</c>, as an older compiler would get. For tests.</summary>
    internal static bool UsePathLocations { get; set; }

    public static bool IsCandidate(SyntaxNode node) =>
        node is InvocationExpressionSyntax
        {
            Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: "Where" or "FirstOrDefault" or "SingleOrDefault" },
            ArgumentList.Arguments.Count: 1 or 2,
        };

    public static LinqInspection? Inspect(GeneratorSyntaxContext context, CancellationToken cancellation)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;
        SemanticModel semantics = context.SemanticModel;
        if (semantics.GetSymbolInfo(invocation, cancellation).Symbol is not IMethodSymbol method
            || method.ContainingType is not { Name: "DataVoTable", Arity: 1 } tableType
            || tableType.ContainingNamespace.ToDisplayString() != "DataVo.Core"
            || tableType.TypeArguments[0] is not INamedTypeSymbol modelType)
        {
            return null;
        }

        ExpressionSyntax argument = invocation.ArgumentList.Arguments[invocation.ArgumentList.Arguments.Count - 1].Expression;
        if (argument is not LambdaExpressionSyntax lambda)
        {
            return null; // a delegate built elsewhere is a deliberate scan
        }

        AttributeData? attribute = modelType.GetAttributes().FirstOrDefault(static data => data.AttributeClass?.ToDisplayString() == TableAttribute);
        if (attribute is null || DataVoTableInspector.Inspect(modelType, attribute.ConstructorArguments.FirstOrDefault().Value as string).Model is not { } model)
        {
            return null; // the table generator reports what is wrong with the model
        }

        Location at = lambda.GetLocation();
        FileLinePositionSpan lines = at.GetLineSpan();
        LinqInspection Result(LinqSite? site, DiagnosticInfo? diagnostic) => new(site, diagnostic, lambda.SyntaxTree, at.SourceSpan);
        LinqInspection Scan(DiagnosticDescriptor descriptor, string argumentText) => Result(null, new DiagnosticInfo(
            descriptor.Id, lines.Path ?? string.Empty, at.SourceSpan, lines.Span, new EquatableArray<string>([argumentText])));

        bool stateful = method.Parameters.Length == 2;
        ITypeSymbol stateType = stateful ? method.TypeArguments[0] : method.Parameters[0].Type;
        if (!IsNameable(stateType, semantics.Compilation))
        {
            return Scan(DataVoGeneratorDiagnostics.LinqNotTranslated, $"generated code cannot name the state type '{stateType.ToDisplayString()}'");
        }

        if (semantics.GetSymbolInfo(lambda, cancellation).Symbol is not IMethodSymbol { Parameters.Length: 1 or 2 } lambdaSymbol
            || lambda.ExpressionBody is not { } body)
        {
            return Scan(DataVoGeneratorDiagnostics.LinqNotTranslated, "the lambda must be a single expression");
        }

        var translator = new Translator(semantics, model, lambdaSymbol.Parameters[0], stateful ? lambdaSymbol.Parameters[1] : null, cancellation);
        string? where = translator.Condition(body, topLevel: true);
        if (translator.Failure is not null)
        {
            return Scan(DataVoGeneratorDiagnostics.LinqNotTranslated, translator.Failure);
        }

        if (translator.Captures.Count > 0)
        {
            return Scan(DataVoGeneratorDiagnostics.LinqCapturedVariable, string.Join(", ", translator.Captures.Select(static name => $"'{name}'")));
        }

        if (where is null || LocationOf(semantics, invocation, cancellation) is not { } location)
        {
            return Scan(DataVoGeneratorDiagnostics.LinqNotTranslated, "the expression is not supported");
        }

        return Result(
            new LinqSite(
                location,
                method.Name,
                stateful,
                modelType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                modelType.IsReferenceType,
                stateType.ToDisplayString(TypeFormat),
                where,
                translator.Unique,
                new EquatableArray<LinqParameter>([.. translator.Parameters])),
            null);
    }

    private static LinqLocation? LocationOf(SemanticModel semantics, InvocationExpressionSyntax invocation, CancellationToken cancellation)
    {
        SyntaxToken name = ((MemberAccessExpressionSyntax)invocation.Expression).Name.Identifier;
        FileLinePositionSpan span = name.GetLocation().GetLineSpan();
        string path = semantics.Compilation.Options.SourceReferenceResolver?.NormalizePath(invocation.SyntaxTree.FilePath, baseFilePath: null)
            ?? invocation.SyntaxTree.FilePath;
        int line = span.StartLinePosition.Line + 1;
        int column = span.StartLinePosition.Character + 1;

        if (!UsePathLocations && GetInterceptableLocation is not null)
        {
            object? encoded;
            try
            {
                encoded = GetInterceptableLocation.Invoke(null, [semantics, invocation, cancellation]);
            }
            catch (TargetInvocationException)
            {
                encoded = null;
            }

            if (encoded is null)
            {
                return null; // the compiler says this call cannot be intercepted
            }

            Type type = encoded.GetType();
            if (type.GetProperty("Version")?.GetValue(encoded) is int version && type.GetProperty("Data")?.GetValue(encoded) is string data)
            {
                return new LinqLocation(version, data, path, line, column);
            }
        }

        return new LinqLocation(0, string.Empty, path, line, column);
    }

    /// <summary>Whether generated code in the same assembly can write this type's name.</summary>
    private static bool IsNameable(ITypeSymbol type, Compilation compilation) => type switch
    {
        IArrayTypeSymbol array => IsNameable(array.ElementType, compilation),
        INamedTypeSymbol named => !named.IsAnonymousType
            && (named.IsTupleType || compilation.IsSymbolAccessibleWithin(named, compilation.Assembly))
            && named.TypeArguments.All(argument => IsNameable(argument, compilation)),
        _ => false,
    };

    private static ITypeSymbol? StripNullable(ITypeSymbol? type) =>
        type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T } nullable ? nullable.TypeArguments[0] : type;

    private sealed class Translator(SemanticModel semantics, TableModel model, IParameterSymbol row, IParameterSymbol? state, CancellationToken cancellation)
    {
        private const string NullLiteral = "\0null";

        public List<LinqParameter> Parameters { get; } = [];

        public List<string> Captures { get; } = [];

        public string? Failure { get; private set; }

        /// <summary>Whether the condition requires the primary key to equal one value.</summary>
        public bool Unique { get; private set; }

        public string? Condition(ExpressionSyntax expression, bool topLevel)
        {
            switch (expression)
            {
                case ParenthesizedExpressionSyntax parenthesized:
                    return Condition(parenthesized.Expression, topLevel) is { } inner ? $"({inner})" : null;

                case BinaryExpressionSyntax binary when binary.IsKind(SyntaxKind.LogicalAndExpression) || binary.IsKind(SyntaxKind.LogicalOrExpression):
                {
                    bool and = binary.IsKind(SyntaxKind.LogicalAndExpression);
                    string? left = Condition(binary.Left, topLevel && and);
                    string? right = Condition(binary.Right, topLevel && and);
                    return left is null || right is null ? null : $"{left} {(and ? "AND" : "OR")} {right}";
                }

                case BinaryExpressionSyntax binary when ComparisonOperator(binary) is { } comparison:
                    return Comparison(binary, comparison, topLevel);

                case PrefixUnaryExpressionSyntax not when not.IsKind(SyntaxKind.LogicalNotExpression) && AsColumn(not.Operand) is { } negated:
                    return BooleanColumn(negated.Column, negated.Property, "false");

                case IsPatternExpressionSyntax { Pattern: ConstantPatternSyntax { Expression: LiteralExpressionSyntax literal } } isNull
                    when literal.IsKind(SyntaxKind.NullLiteralExpression) && AsColumn(isNull.Expression) is { } tested:
                    return $"{tested.Column.PropertyName} IS NULL";

                case IsPatternExpressionSyntax { Pattern: UnaryPatternSyntax { Pattern: ConstantPatternSyntax { Expression: LiteralExpressionSyntax literal } } } isNotNull
                    when literal.IsKind(SyntaxKind.NullLiteralExpression) && AsColumn(isNotNull.Expression) is { } tested:
                    return $"{tested.Column.PropertyName} IS NOT NULL";

                default:
                    return AsColumn(expression) is { } column
                        ? BooleanColumn(column.Column, column.Property, "true")
                        : Fail($"'{Shorten(expression)}' is not a comparison between a column and a value");
            }
        }

        private static string? ComparisonOperator(BinaryExpressionSyntax binary) => binary.Kind() switch
        {
            SyntaxKind.EqualsExpression => "=",
            SyntaxKind.NotEqualsExpression => "<>",
            SyntaxKind.LessThanExpression => "<",
            SyntaxKind.LessThanOrEqualExpression => "<=",
            SyntaxKind.GreaterThanExpression => ">",
            SyntaxKind.GreaterThanOrEqualExpression => ">=",
            _ => null,
        };

        private static string Flip(string comparison) => comparison switch
        {
            "<" => ">",
            "<=" => ">=",
            ">" => "<",
            ">=" => "<=",
            _ => comparison,
        };

        private string? Comparison(BinaryExpressionSyntax binary, string comparison, bool topLevel)
        {
            (ColumnModel Column, IPropertySymbol Property)? left = AsColumn(binary.Left);
            (ColumnModel Column, IPropertySymbol Property)? right = AsColumn(binary.Right);
            if (left is null == right is null)
            {
                return Fail($"'{Shorten(binary)}' must compare one column with one value");
            }

            (ColumnModel column, IPropertySymbol property) = (left ?? right)!.Value;
            ExpressionSyntax columnSide = left is null ? binary.Right : binary.Left;
            ExpressionSyntax valueSide = left is null ? binary.Left : binary.Right;
            if (left is null)
            {
                comparison = Flip(comparison);
            }

            ITypeSymbol columnType = StripNullable(property.Type)!;
            if (!SymbolEqualityComparer.Default.Equals(StripNullable(semantics.GetTypeInfo(columnSide, cancellation).ConvertedType), columnType))
            {
                return Fail($"'{Shorten(binary)}' converts column {column.PropertyName} to another type; compare it with a value of type {columnType.ToDisplayString()}");
            }

            string? parameter = Value(valueSide, columnType, column);
            if (parameter is null)
            {
                return null;
            }

            string name = column.PropertyName;
            if (parameter == NullLiteral)
            {
                return comparison switch
                {
                    "=" => $"{name} IS NULL",
                    "<>" => $"{name} IS NOT NULL",
                    _ => Fail($"'{Shorten(binary)}' orders a column against null"),
                };
            }

            if (comparison == "=" && column.IsPrimaryKey && topLevel)
            {
                Unique = true;
            }

            // SQL's NULL <> value is unknown, while C# counts a null column as different from the value.
            return comparison == "<>" && column.IsNullable
                ? $"({name} <> {parameter} OR {name} IS NULL)"
                : $"{name} {comparison} {parameter}";
        }

        private string? BooleanColumn(ColumnModel column, IPropertySymbol property, string literal)
        {
            if (property.Type.SpecialType != SpecialType.System_Boolean)
            {
                return Fail($"column {column.PropertyName} is not a bool, so it needs a comparison");
            }

            return $"{column.PropertyName} = {Add(new LinqParameter(literal, null, false))}";
        }

        private (ColumnModel Column, IPropertySymbol Property)? AsColumn(ExpressionSyntax expression)
        {
            while (expression is ParenthesizedExpressionSyntax parenthesized)
            {
                expression = parenthesized.Expression;
            }

            if (expression is not MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax receiver } access
                || !IsRow(receiver)
                || semantics.GetSymbolInfo(access, cancellation).Symbol is not IPropertySymbol property)
            {
                return null;
            }

            ColumnModel? column = model.Columns.FirstOrDefault(candidate => candidate.PropertyName == property.Name);
            return column is null ? null : (column, property);
        }

        private bool IsRow(IdentifierNameSyntax identifier) =>
            SymbolEqualityComparer.Default.Equals(semantics.GetSymbolInfo(identifier, cancellation).Symbol, row);

        /// <summary>Binds a value expression to a new parameter and returns its SQL name.</summary>
        private string? Value(ExpressionSyntax expression, ITypeSymbol columnType, ColumnModel column)
        {
            while (expression is ParenthesizedExpressionSyntax parenthesized)
            {
                expression = parenthesized.Expression;
            }

            if (expression.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>().Any(IsRow))
            {
                return Fail($"'{Shorten(expression)}' computes a value from the row; only a plain column can be compared");
            }

            string cast = columnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            Optional<object?> constant = semantics.GetConstantValue(expression, cancellation);
            if (constant.HasValue)
            {
                if (constant.Value is null)
                {
                    return NullLiteral;
                }

                return Literal(constant.Value, columnType) is { } literal
                    ? Add(new LinqParameter(literal, null, column.Type.UsesObjectParameter))
                    : Fail($"the constant '{Shorten(expression)}' cannot be written as a {columnType.ToDisplayString()}");
            }

            Microsoft.CodeAnalysis.TypeInfo type = semantics.GetTypeInfo(expression, cancellation);
            if (!SymbolEqualityComparer.Default.Equals(StripNullable(type.ConvertedType), columnType))
            {
                return Fail($"'{Shorten(expression)}' is not a {columnType.ToDisplayString()}, the type of column {column.PropertyName}");
            }

            string? text = ValueText(expression);
            if (text is null)
            {
                return null;
            }

            bool nullableValue = type.Type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T };
            if (nullableValue)
            {
                return Add(new LinqParameter($"({cast})({text}).GetValueOrDefault()", $"({text}).HasValue", column.Type.UsesObjectParameter));
            }

            return type.Type is { IsReferenceType: true }
                ? Add(new LinqParameter($"({cast})({text})", $"({text}) != null", column.Type.UsesObjectParameter))
                : Add(new LinqParameter($"({cast})({text})", null, column.Type.UsesObjectParameter));
        }

        /// <summary>Rewrites a value expression so it can run inside the interceptor, where only <c>state</c> and statics exist.</summary>
        private string? ValueText(ExpressionSyntax expression)
        {
            switch (expression)
            {
                case ParenthesizedExpressionSyntax parenthesized:
                    return ValueText(parenthesized.Expression);

                case ThisExpressionSyntax:
                    return Capture("this");

                case IdentifierNameSyntax identifier:
                {
                    ISymbol? symbol = semantics.GetSymbolInfo(identifier, cancellation).Symbol;
                    if (state is not null && SymbolEqualityComparer.Default.Equals(symbol, state))
                    {
                        return "state";
                    }

                    return symbol switch
                    {
                        IFieldSymbol or IPropertySymbol when symbol.IsStatic => StaticMember(symbol),
                        ILocalSymbol or IParameterSymbol or IFieldSymbol or IPropertySymbol => Capture(identifier.Identifier.ValueText),
                        _ => Fail($"'{Shorten(expression)}' is not a value the query can be given"),
                    };
                }

                case MemberAccessExpressionSyntax access when access.IsKind(SyntaxKind.SimpleMemberAccessExpression):
                {
                    ISymbol? symbol = semantics.GetSymbolInfo(access, cancellation).Symbol;
                    if (symbol is not (IFieldSymbol or IPropertySymbol))
                    {
                        return Fail($"'{Shorten(expression)}' is not a field or property");
                    }

                    if (symbol.IsStatic)
                    {
                        return StaticMember(symbol);
                    }

                    if (ValueText(access.Expression) is not { } receiver)
                    {
                        return null;
                    }

                    return IsReachable(symbol) ? $"{receiver}.{symbol.Name}" : Fail($"generated code cannot read '{symbol.Name}'");
                }

                default:
                    return Fail($"'{Shorten(expression)}' is not a constant, the state value, or a field or property of it");
            }
        }

        private string? StaticMember(ISymbol symbol) => IsReachable(symbol) && IsNameable(symbol.ContainingType, semantics.Compilation)
            ? $"{symbol.ContainingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}.{symbol.Name}"
            : Fail($"generated code cannot read '{symbol.Name}'; make it internal or pass it as state");

        private bool IsReachable(ISymbol symbol) =>
            symbol.ContainingType is { IsTupleType: true } || semantics.Compilation.IsSymbolAccessibleWithin(symbol, semantics.Compilation.Assembly);

        private static string? Literal(object value, ITypeSymbol columnType)
        {
            try
            {
                switch (columnType.SpecialType)
                {
                    case SpecialType.System_Int32:
                        return Convert.ToInt32(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
                    case SpecialType.System_Int64:
                        return Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture) + "L";
                    case SpecialType.System_Single:
                    {
                        float number = Convert.ToSingle(value, CultureInfo.InvariantCulture);
                        return float.IsNaN(number) || float.IsInfinity(number) ? null : number.ToString("R", CultureInfo.InvariantCulture) + "F";
                    }

                    case SpecialType.System_Double:
                    {
                        double number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                        return double.IsNaN(number) || double.IsInfinity(number) ? null : number.ToString("R", CultureInfo.InvariantCulture) + "D";
                    }

                    case SpecialType.System_Boolean:
                        return value is bool flag ? (flag ? "true" : "false") : null;
                    case SpecialType.System_String:
                        return value is string text ? SymbolDisplay.FormatLiteral(text, quote: true) : null;
                    default:
                        return null;
                }
            }
            catch (Exception exception) when (exception is InvalidCastException or FormatException or OverflowException)
            {
                return null;
            }
        }

        private string Add(LinqParameter parameter)
        {
            Parameters.Add(parameter);
            return "@p" + (Parameters.Count - 1).ToString(CultureInfo.InvariantCulture);
        }

        private string? Capture(string name)
        {
            if (!Captures.Contains(name))
            {
                Captures.Add(name);
            }

            return null;
        }

        private string? Fail(string reason)
        {
            Failure ??= reason;
            return null;
        }

        private static string Shorten(SyntaxNode node)
        {
            string text = node.ToString().Replace("\r", string.Empty).Replace("\n", " ");
            return text.Length <= 60 ? text : text.Substring(0, 57) + "...";
        }
    }
}
