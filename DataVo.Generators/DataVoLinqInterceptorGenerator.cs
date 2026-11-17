using System.Collections.Immutable;
using DataVo.Generators.Diagnostics;
using DataVo.Generators.Linq;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace DataVo.Generators;

/// <summary>
/// Intercepts <c>DataVoTable&lt;T&gt;</c> queries at compile time and routes each one to a statement prepared from
/// its lambda, so no expression tree is built or compiled when the program runs.
/// </summary>
[Generator]
public sealed class DataVoLinqInterceptorGenerator : IIncrementalGenerator
{
    private static readonly DiagnosticDescriptor[] Descriptors =
    [
        DataVoGeneratorDiagnostics.LinqCapturedVariable,
        DataVoGeneratorDiagnostics.LinqNotTranslated,
        DataVoGeneratorDiagnostics.LinqStepInMemory,
        DataVoGeneratorDiagnostics.LinqUpdateNotCompiled,
    ];

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        IncrementalValueProvider<LinqOptions> options = context.ParseOptionsProvider.Select(static (parse, _) => OptionsOf(parse));

        IncrementalValuesProvider<LinqInspection> calls = context.SyntaxProvider
            .CreateSyntaxProvider(static (node, _) => DataVoLinqAnalyzer.IsCandidate(node), DataVoLinqAnalyzer.Inspect)
            .Where(static inspection => inspection is not null)!;

        context.RegisterSourceOutput(calls.Collect().Combine(options), static (output, pair) => Emit(output, pair.Left, pair.Right));
    }

    private static void Emit(SourceProductionContext output, ImmutableArray<LinqInspection> calls, LinqOptions options)
    {
        if (options.Mode == LinqMode.Unsupported || calls.IsDefaultOrEmpty)
        {
            return;
        }

        if (options.Mode == LinqMode.Disabled)
        {
            LinqInspection first = calls[0];
            output.ReportDiagnostic(Diagnostic.Create(DataVoGeneratorDiagnostics.LinqInterceptorsDisabled, Location.Create(first.Tree, first.Span)));
            ReportUncompiledUpdates(output, calls);
            return;
        }

        var sites = new List<LinqSite>();
        var sums = new List<LinqSumSite>();
        var extremes = new List<LinqExtremeSite>();
        var orders = new List<LinqOrderSite>();
        var projections = new List<LinqProjectionSite>();
        var updates = new List<LinqUpdateSite>();
        foreach (LinqInspection call in calls)
        {
            if (call.Order is { } order)
            {
                orders.Add(order);
            }

            if (call.Projection is { } projection)
            {
                projections.Add(projection);
            }

            if (call.Update is { } update)
            {
                updates.Add(update);
            }

            if (call.Diagnostic is { } info)
            {
                DiagnosticDescriptor descriptor = Descriptors.First(candidate => candidate.Id == info.DescriptorId);
                output.ReportDiagnostic(Diagnostic.Create(descriptor, Location.Create(call.Tree, call.Span), info.Arguments.Cast<object?>().ToArray()));
            }

            if (call.Site is { } site)
            {
                sites.Add(site);
            }

            if (call.Sum is { } sum)
            {
                sums.Add(sum);
            }

            if (call.Extreme is { } extreme)
            {
                extremes.Add(extreme);
            }
        }

        var shapes = new LinqShapes(orders, projections, updates);
        if (sites.Count > 0 || sums.Count > 0 || extremes.Count > 0 || !shapes.IsEmpty)
        {
            output.AddSource("DataVoLinqInterceptors.g.cs", SourceText.From(DataVoLinqEmitter.Emit(sites, sums, extremes, shapes, options.FileLocalTypes), System.Text.Encoding.UTF8));
        }
    }

    /// <summary>
    /// Fails the build at each <c>ExecuteUpdate</c> call when interceptors are disabled: nothing else can run it, so it
    /// would otherwise throw when the program reaches it.
    /// </summary>
    private static void ReportUncompiledUpdates(SourceProductionContext output, ImmutableArray<LinqInspection> calls)
    {
        foreach (LinqInspection call in calls.Where(static call => call.RequiresInterception))
        {
            output.ReportDiagnostic(Diagnostic.Create(
                DataVoGeneratorDiagnostics.LinqUpdateNotCompiled,
                Location.Create(call.Tree, call.Span),
                "interceptors are not enabled for this project; add <InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces> to the project file"));
        }
    }

    private static LinqOptions OptionsOf(ParseOptions parse)
    {
        LanguageVersion version = parse is CSharpParseOptions csharp ? csharp.LanguageVersion : LanguageVersion.Default;
        bool enabled = Lists(parse, "InterceptorsNamespaces") || Lists(parse, "InterceptorsPreviewNamespaces");
        LinqMode mode = enabled ? LinqMode.Enabled : version >= LanguageVersion.CSharp12 ? LinqMode.Disabled : LinqMode.Unsupported;
        return new LinqOptions(mode, version >= LanguageVersion.CSharp11);
    }

    /// <summary>Whether a compiler feature lists the generated namespace or one that contains it.</summary>
    private static bool Lists(ParseOptions parse, string feature) =>
        parse.Features.TryGetValue(feature, out string? namespaces)
        && namespaces.Split(';', ',').Select(static name => name.Trim()).Any(static name =>
            name == DataVoLinqEmitter.Namespace || (name.Length > 0 && DataVoLinqEmitter.Namespace.StartsWith(name + ".", StringComparison.Ordinal)));
}
