using DataVo.Generators.Diagnostics;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace DataVo.Generators.Linq;

/// <summary>
/// Inspects the calls that shape a table query after its filter: sort keys, <c>Select</c> projections and
/// <c>ExecuteUpdate</c> setters. Each becomes a site to intercept, or a diagnostic saying why it runs another way.
/// </summary>
internal static class DataVoLinqShapeAnalyzer
{
    /// <summary>The method names this analyzer handles.</summary>
    public static bool Handles(string name) => name is "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" or "Select" or "ExecuteUpdate";

    /// <summary>Which DataVo type a method belongs to, or null when it belongs to another type.</summary>
    public static LinqReceiver? ReceiverOf(INamedTypeSymbol type)
    {
        if (type.ContainingNamespace?.ToDisplayString() != "DataVo.Core")
        {
            return null;
        }

        return (type.Name, type.Arity) switch
        {
            ("DataVoTable", 1) => LinqReceiver.Table,
            ("DataVoQuery", 2) => LinqReceiver.Query,
            ("DataVoOrderedQuery", 2) => LinqReceiver.OrderedQuery,
            ("DataVoPagedQuery", 2) => LinqReceiver.PagedQuery,
            _ => null,
        };
    }

    /// <summary>Inspects a call this analyzer handles; returns null for calls it leaves alone without a word.</summary>
    /// <param name="semantics">The semantic model of the call's file.</param>
    /// <param name="invocation">The call.</param>
    /// <param name="method">The method it calls.</param>
    /// <param name="receiver">The DataVo type the method belongs to.</param>
    /// <param name="cancellation">Cancels the analysis.</param>
    /// <returns>The inspection, or null.</returns>
    public static LinqInspection? Inspect(SemanticModel semantics, InvocationExpressionSyntax invocation, IMethodSymbol method, LinqReceiver receiver, CancellationToken cancellation)
    {
        if (method.ContainingType.TypeArguments[0] is not INamedTypeSymbol modelType || DataVoLinqAnalyzer.ModelOf(modelType) is not { } model)
        {
            return null; // the table generator reports what is wrong with the model
        }

        ExpressionSyntax last = invocation.ArgumentList.Arguments[invocation.ArgumentList.Arguments.Count - 1].Expression;
        var context = new ShapeContext(semantics, invocation, method, receiver, modelType, model, cancellation);
        return method.Name switch
        {
            "OrderBy" or "OrderByDescending" when receiver is LinqReceiver.Table or LinqReceiver.Query && method.Parameters.Length == 1 => InspectOrder(context, last),
            "ThenBy" or "ThenByDescending" when receiver is LinqReceiver.OrderedQuery && method.Parameters.Length == 1 => InspectOrder(context, last),
            "Select" when method.Parameters.Length == 1 => InspectProjection(context, last),
            "ExecuteUpdate" when receiver is LinqReceiver.Table or LinqReceiver.Query => InspectUpdate(context, last),
            _ => null,
        };
    }

    /// <summary>A sort key compiles when it is <c>p =&gt; p.Column</c> of the column's own type and the column can be ordered.</summary>
    private static LinqInspection? InspectOrder(ShapeContext context, ExpressionSyntax argument)
    {
        if (argument is not LambdaExpressionSyntax lambda)
        {
            return null; // a key built elsewhere is a deliberate in-memory sort
        }

        string step = context.Method.Name;
        if (!context.ReceiverIsNameable)
        {
            return null; // the filter already reported why its state cannot be named
        }

        if (SingleParameter(context, lambda) is not { } row || lambda.ExpressionBody is not { } body)
        {
            return context.Report(DataVoGeneratorDiagnostics.LinqStepInMemory, lambda, step, "the key must be a lambda with one expression, such as p => p.Column");
        }

        if (ColumnOf(context, body, row) is not { } key)
        {
            return context.Report(DataVoGeneratorDiagnostics.LinqStepInMemory, lambda, step, $"'{body}' is not one column; sort by p => p.Column so the engine can sort");
        }

        if (key.Column.Type.IsVector3)
        {
            return context.Report(DataVoGeneratorDiagnostics.LinqStepInMemory, lambda, step, $"column {key.Column.PropertyName} is a 3D position, which has no order");
        }

        if (key.Column.Type.IsVector)
        {
            return context.Report(DataVoGeneratorDiagnostics.LinqStepInMemory, lambda, step, $"column {key.Column.PropertyName} is a vector, which has no order; rank by distance with SearchNearest instead");
        }

        ITypeSymbol keyType = context.Method.TypeArguments[0];
        if (!SymbolEqualityComparer.Default.Equals(keyType, key.Property.Type))
        {
            return context.Report(DataVoGeneratorDiagnostics.LinqStepInMemory, lambda, step, $"the key converts column {key.Column.PropertyName} to {keyType.ToDisplayString()}");
        }

        if (context.Location() is not { } call)
        {
            return null;
        }

        return context.Result(lambda, order: new LinqOrderSite(
            call,
            step,
            keyType.ToDisplayString(DataVoLinqAnalyzer.TypeFormat),
            key.Column.PropertyName,
            step.EndsWith("Descending", StringComparison.Ordinal)));
    }

    /// <summary>A projection compiles when its body can be rewritten over the columns it reads; see <see cref="LinqProjectionWriter"/>.</summary>
    private static LinqInspection? InspectProjection(ShapeContext context, ExpressionSyntax argument)
    {
        if (argument is not LambdaExpressionSyntax lambda)
        {
            return null; // a selector built elsewhere deliberately reads models
        }

        if (SingleParameter(context, lambda) is not { } row || lambda.ExpressionBody is not { } body)
        {
            return context.Report(DataVoGeneratorDiagnostics.LinqStepInMemory, lambda, "Select", "the selector must be a lambda with one expression");
        }

        var writer = new LinqProjectionWriter(context.Semantics, context.Model, row, context.Cancellation);
        string? written = writer.Write(body);
        if (writer.Captures.Count > 0)
        {
            return context.Report(DataVoGeneratorDiagnostics.LinqStepInMemory, lambda, "Select", $"the selector captures {string.Join(", ", writer.Captures.Select(static name => $"'{name}'"))}");
        }

        if (written is null || context.Location() is not { } call)
        {
            return written is null
                ? context.Report(DataVoGeneratorDiagnostics.LinqStepInMemory, lambda, "Select", writer.Failure ?? "the selector cannot be compiled")
                : null;
        }

        if (writer.Columns.Count == 0)
        {
            writer.Columns.Add(context.Model.Columns.First(static column => column.IsPrimaryKey).PropertyName); // SELECT needs a column
        }

        var reactiveWriter = new LinqProjectionWriter(context.Semantics, context.Model, row, context.Cancellation, useReactiveRowRef: true);
        string reactiveWritten = reactiveWriter.Write(body) ?? written;

        ITypeSymbol result = context.Method.TypeArguments[0];
        bool named = context.ReceiverIsNameable && DataVoLinqAnalyzer.IsNameable(result, context.Semantics.Compilation);
        return context.Result(lambda, projection: new LinqProjectionSite(
            call,
            named ? result.ToDisplayString(DataVoLinqAnalyzer.TypeFormat) : null,
            new EquatableArray<string>([.. writer.Columns]),
            written,
            reactiveWritten));
    }

    /// <summary>An update compiles when its setters translate; otherwise the build fails, since nothing else can run it.</summary>
    private static LinqInspection InspectUpdate(ShapeContext context, ExpressionSyntax argument)
    {
        bool stateful = context.Method.Parameters.Length == 2;
        if (argument is not LambdaExpressionSyntax { ExpressionBody: { } body } lambda
            || context.Semantics.GetSymbolInfo(lambda, context.Cancellation).Symbol is not IMethodSymbol { Parameters.Length: 1 or 2 } setters)
        {
            return context.ReportUpdate(argument, "write the setters as a lambda with one expression at the call, such as set => set.Set(p => p.Gold, 0)");
        }

        ITypeSymbol? stateType = stateful ? context.Method.TypeArguments[0] : null;
        if (!context.ReceiverIsNameable || (stateType is not null && !DataVoLinqAnalyzer.IsNameable(stateType, context.Semantics.Compilation)))
        {
            return context.ReportUpdate(lambda, "generated code cannot name the query's or the call's state type; pass a named type, such as a tuple, as state");
        }

        var translator = new LinqUpdateTranslator(context.Semantics, context.Model, setters.Parameters[0], stateful ? setters.Parameters[1] : null, context.Cancellation);
        string? assignments = translator.Translate(body);
        if (translator.Captures.Count > 0)
        {
            string names = string.Join(", ", translator.Captures.Select(static name => $"'{name}'"));
            return context.ReportUpdate(lambda, $"the setters capture {names}; pass the value as state with a static lambda, such as ExecuteUpdate(value, static (set, v) => set.Set(p => p.Column, v))");
        }

        if (assignments is null)
        {
            return context.ReportUpdate(lambda, translator.Failure ?? "the setters cannot be translated");
        }

        if (context.Location() is not { } call)
        {
            return context.ReportUpdate(lambda, "the compiler cannot intercept this call");
        }

        return context.Result(lambda, update: new LinqUpdateSite(
            call,
            stateType?.ToDisplayString(DataVoLinqAnalyzer.TypeFormat),
            assignments,
            new EquatableArray<string>([.. translator.Values])));
    }

    /// <summary>The lambda's only parameter, or null when it has another number of parameters.</summary>
    private static IParameterSymbol? SingleParameter(ShapeContext context, LambdaExpressionSyntax lambda) =>
        context.Semantics.GetSymbolInfo(lambda, context.Cancellation).Symbol is IMethodSymbol { Parameters.Length: 1 } symbol ? symbol.Parameters[0] : null;

    /// <summary>The stored column a <c>p.Column</c> expression (in any parentheses) reads, or null.</summary>
    private static (ColumnModel Column, IPropertySymbol Property)? ColumnOf(ShapeContext context, ExpressionSyntax expression, IParameterSymbol row)
    {
        while (expression is ParenthesizedExpressionSyntax parenthesized)
        {
            expression = parenthesized.Expression;
        }

        if (expression is not MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax receiver } access
            || !SymbolEqualityComparer.Default.Equals(context.Semantics.GetSymbolInfo(receiver, context.Cancellation).Symbol, row)
            || context.Semantics.GetSymbolInfo(access, context.Cancellation).Symbol is not IPropertySymbol property
            || context.Model.Columns.FirstOrDefault(candidate => candidate.PropertyName == property.Name) is not { } column)
        {
            return null;
        }

        return (column, property);
    }

    /// <summary>What every shape inspection needs about its call, and the helpers that build its outcome.</summary>
    private sealed class ShapeContext(
        SemanticModel semantics,
        InvocationExpressionSyntax invocation,
        IMethodSymbol method,
        LinqReceiver receiver,
        INamedTypeSymbol modelType,
        TableModel model,
        CancellationToken cancellation)
    {
        public SemanticModel Semantics { get; } = semantics;

        public IMethodSymbol Method { get; } = method;

        public TableModel Model { get; } = model;

        public CancellationToken Cancellation { get; } = cancellation;

        /// <summary>Whether generated code can name the receiver's type, which a non-generic interceptor must.</summary>
        public bool ReceiverIsNameable => DataVoLinqAnalyzer.IsNameable(Method.ContainingType, Semantics.Compilation);

        /// <summary>Describes the call for the emitter, or returns null when the compiler cannot intercept it.</summary>
        public LinqCall? Location() =>
            DataVoLinqAnalyzer.LocationOf(Semantics, invocation, Cancellation) is { } location
                ? new LinqCall(
                    location,
                    receiver,
                    Method.ContainingType.ToDisplayString(DataVoLinqAnalyzer.TypeFormat),
                    Method.ReturnType.ToDisplayString(DataVoLinqAnalyzer.TypeFormat),
                    modelType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat))
                : null;

        public LinqInspection Result(SyntaxNode at, LinqOrderSite? order = null, LinqProjectionSite? projection = null, LinqUpdateSite? update = null) =>
            new(null, null, at.SyntaxTree.FilePath, at.Span, Order: order, Projection: projection, Update: update, RequiresInterception: update is not null);

        public LinqInspection Report(DiagnosticDescriptor descriptor, SyntaxNode at, params string[] arguments)
        {
            FileLinePositionSpan lines = at.GetLocation().GetLineSpan();
            return new LinqInspection(
                null,
                new DiagnosticInfo(descriptor.Id, lines.Path ?? string.Empty, at.Span, lines.Span, new EquatableArray<string>([.. arguments])),
                at.SyntaxTree.FilePath,
                at.Span,
                RequiresInterception: descriptor.Id == DataVoGeneratorDiagnostics.LinqUpdateNotCompiled.Id);
        }

        public LinqInspection ReportUpdate(SyntaxNode at, string reason) => Report(DataVoGeneratorDiagnostics.LinqUpdateNotCompiled, at, reason);
    }
}
