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

    /// <summary>Fully qualified type names that keep <c>?</c> on nullable reference types.</summary>
    internal static readonly SymbolDisplayFormat TypeFormat =
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
        }
        or InvocationExpressionSyntax
        {
            Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: "Sum" or "Min" or "Max" or "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" or "Select" },
            ArgumentList.Arguments.Count: 1,
        }
        or InvocationExpressionSyntax
        {
            Expression: MemberAccessExpressionSyntax { Name.Identifier.ValueText: "ExecuteUpdate" },
            ArgumentList.Arguments.Count: 1 or 2,
        };

    public static LinqInspection? Inspect(GeneratorSyntaxContext context, CancellationToken cancellation)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;
        SemanticModel semantics = context.SemanticModel;
        if (semantics.GetSymbolInfo(invocation, cancellation).Symbol is IMethodSymbol { Name: "Sum", ContainingType: { Name: "DataVoQuery", Arity: 2 } queryType } sum
            && queryType.ContainingNamespace.ToDisplayString() == "DataVo.Core")
        {
            return InspectSum(semantics, invocation, sum, queryType, cancellation);
        }

        if (semantics.GetSymbolInfo(invocation, cancellation).Symbol is IMethodSymbol { Name: "Min" or "Max", Arity: 1, ContainingType: { Name: "DataVoQuery", Arity: 2 } foldedType } extreme
            && foldedType.ContainingNamespace.ToDisplayString() == "DataVo.Core")
        {
            return InspectExtreme(semantics, invocation, extreme, foldedType, cancellation);
        }

        if (semantics.GetSymbolInfo(invocation, cancellation).Symbol is IMethodSymbol shaped
            && DataVoLinqShapeAnalyzer.Handles(shaped.Name)
            && DataVoLinqShapeAnalyzer.ReceiverOf(shaped.ContainingType) is { } receiver)
        {
            return DataVoLinqShapeAnalyzer.Inspect(semantics, invocation, shaped, receiver, cancellation);
        }

        if (semantics.GetSymbolInfo(invocation, cancellation).Symbol is not IMethodSymbol { Name: "Where" or "FirstOrDefault" or "SingleOrDefault" } method
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

        if (ModelOf(modelType) is not { } model)
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

        var translator = new LinqConditionTranslator(semantics, model, lambdaSymbol.Parameters[0], stateful ? lambdaSymbol.Parameters[1] : null, cancellation);
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
                new EquatableArray<LinqParameter>([.. translator.Parameters]),
                translator.Unique ? null : translator.Kernel(body),
                IntegerKeyLookup: translator.Parameters.Count == 1 && where == translator.IntegerKeyCondition),
            null);
    }

    /// <summary>
    /// Translates <c>query.Sum(p =&gt; p.Column)</c> over an <c>int</c>, <c>long</c>, <c>float</c> or <c>double</c>
    /// column. Any other selector is left alone, silently: it is ordinary code that sums what it computes.
    /// </summary>
    private static LinqInspection? InspectSum(SemanticModel semantics, InvocationExpressionSyntax invocation, IMethodSymbol method, INamedTypeSymbol queryType, CancellationToken cancellation)
    {
        if (SelectedColumn(semantics, invocation, queryType, cancellation) is not { Column.IsNullable: false } selected)
        {
            return null;
        }

        ColumnModel column = selected.Column;
        LambdaExpressionSyntax lambda = selected.Lambda;
        INamedTypeSymbol modelType = selected.Model;

        // The selector's value must be the column's own type: a widening selector sums something else.
        (string Value, string Method)? shape = (column.Type.SqlType, method.ReturnType.SpecialType) switch
        {
            ("INT", SpecialType.System_Int32) => ("int", "SumInt32Column"),
            ("BIGINT", SpecialType.System_Int64) => ("long", "SumInt64Column"),
            ("FLOAT", SpecialType.System_Single) => ("float", "SumSingleColumn"),
            ("DOUBLE", SpecialType.System_Double) => ("double", "SumDoubleColumn"),
            _ => null,
        };
        if (shape is null || LocationOf(semantics, invocation, cancellation) is not { } location)
        {
            return null;
        }

        return new LinqInspection(
            null,
            null,
            lambda.SyntaxTree,
            lambda.GetLocation().SourceSpan,
            new LinqSumSite(
                location,
                modelType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                queryType.TypeArguments[1].ToDisplayString(TypeFormat),
                shape.Value.Value,
                shape.Value.Method,
                column.PropertyName));
    }

    /// <summary>
    /// Translates <c>query.Min(p =&gt; p.Column)</c> and <c>query.Max(p =&gt; p.Column)</c> over an <c>int</c>,
    /// <c>long</c>, <c>float</c>, <c>double</c> or date column, nullable or not. Any other selector is left alone,
    /// silently: it is ordinary code that compares what it computes.
    /// </summary>
    private static LinqInspection? InspectExtreme(SemanticModel semantics, InvocationExpressionSyntax invocation, IMethodSymbol method, INamedTypeSymbol queryType, CancellationToken cancellation)
    {
        if (SelectedColumn(semantics, invocation, queryType, cancellation) is not { } selected)
        {
            return null;
        }

        // The value compared must be the column's own type: a widening call compares something else.
        ITypeSymbol value = method.TypeArguments[0];
        if (!SymbolEqualityComparer.Default.Equals(value, selected.Property.Type))
        {
            return null;
        }

        bool nullable = value is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T };
        ITypeSymbol core = nullable ? ((INamedTypeSymbol)value).TypeArguments[0] : value;
        if (nullable != selected.Column.IsNullable)
        {
            return null;
        }

        (char Kind, string Result)? shape = (selected.Column.Type.SqlType, core.SpecialType, core.ToDisplayString()) switch
        {
            ("INT", SpecialType.System_Int32, _) => ('i', "(int)totals.Whole"),
            ("BIGINT", SpecialType.System_Int64, _) => ('l', "totals.Whole"),
            ("FLOAT", SpecialType.System_Single, _) => ('f', "(float)totals.Real"),
            ("DOUBLE", SpecialType.System_Double, _) => ('d', "totals.Real"),
            ("DATE", _, "DataVo.Core.DataVoDate") => ('t', "global::DataVo.Core.DataVoDate.FromDayNumber((int)totals.Whole)"),
            ("DATE", _, "System.DateOnly") => ('t', "global::System.DateOnly.FromDayNumber((int)totals.Whole)"),
            ("DATETIME", _, "System.DateTime") => ('l', "new global::System.DateTime(totals.Whole, global::System.DateTimeKind.Utc)"),
            ("DATETIME", _, "System.DateTimeOffset") => ('l', "new global::System.DateTimeOffset(totals.Whole, global::System.TimeSpan.Zero)"),
            _ => null,
        };
        if (shape is null || LocationOf(semantics, invocation, cancellation) is not { } location)
        {
            return null;
        }

        return new LinqInspection(
            null,
            null,
            selected.Lambda.SyntaxTree,
            selected.Lambda.GetLocation().SourceSpan,
            Extreme: new LinqExtremeSite(
                location,
                method.Name == "Max",
                selected.Model.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                queryType.TypeArguments[1].ToDisplayString(TypeFormat),
                core.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) + (nullable ? "?" : string.Empty),
                nullable,
                shape.Value.Kind,
                shape.Value.Result,
                selected.Column.PropertyName));
    }

    /// <summary>
    /// The column an aggregate's selector reads, when the selector is exactly one mapped property of the row
    /// (<c>p =&gt; p.Gold</c>) and generated code can name the query's types.
    /// </summary>
    private static (LambdaExpressionSyntax Lambda, INamedTypeSymbol Model, IPropertySymbol Property, ColumnModel Column)? SelectedColumn(
        SemanticModel semantics, InvocationExpressionSyntax invocation, INamedTypeSymbol queryType, CancellationToken cancellation)
    {
        if (invocation.ArgumentList.Arguments[0].Expression is not LambdaExpressionSyntax { ExpressionBody: { } body } lambda
            || semantics.GetSymbolInfo(lambda, cancellation).Symbol is not IMethodSymbol { Parameters.Length: 1 } lambdaSymbol
            || queryType.TypeArguments[0] is not INamedTypeSymbol modelType
            || !IsNameable(queryType.TypeArguments[1], semantics.Compilation))
        {
            return null;
        }

        while (body is ParenthesizedExpressionSyntax parenthesized)
        {
            body = parenthesized.Expression;
        }

        if (body is not MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax receiver } access
            || !SymbolEqualityComparer.Default.Equals(semantics.GetSymbolInfo(receiver, cancellation).Symbol, lambdaSymbol.Parameters[0])
            || semantics.GetSymbolInfo(access, cancellation).Symbol is not IPropertySymbol property)
        {
            return null;
        }

        if (ModelOf(modelType) is not { } model
            || model.Columns.FirstOrDefault(candidate => candidate.PropertyName == property.Name) is not { } column)
        {
            return null;
        }

        return (lambda, modelType, property, column);
    }

    /// <summary>The model a <c>[DataVoTable]</c> type describes, or null when it is not one or is not valid.</summary>
    internal static TableModel? ModelOf(INamedTypeSymbol modelType)
    {
        AttributeData? attribute = modelType.GetAttributes().FirstOrDefault(static data => data.AttributeClass?.ToDisplayString() == TableAttribute);
        return attribute is null ? null : DataVoTableInspector.Inspect(modelType, attribute.ConstructorArguments.FirstOrDefault().Value as string).Model;
    }

    internal static LinqLocation? LocationOf(SemanticModel semantics, InvocationExpressionSyntax invocation, CancellationToken cancellation)
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
    internal static bool IsNameable(ITypeSymbol type, Compilation compilation) => type switch
    {
        IArrayTypeSymbol array => IsNameable(array.ElementType, compilation),
        INamedTypeSymbol named => !named.IsAnonymousType
            && (named.IsTupleType || compilation.IsSymbolAccessibleWithin(named, compilation.Assembly))
            && named.TypeArguments.All(argument => IsNameable(argument, compilation)),
        _ => false,
    };

    internal static ITypeSymbol? StripNullable(ITypeSymbol? type) =>
        type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T } nullable ? nullable.TypeArguments[0] : type;
}
