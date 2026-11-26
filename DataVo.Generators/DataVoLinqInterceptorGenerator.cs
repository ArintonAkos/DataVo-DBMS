using System.Collections.Immutable;
using DataVo.Generators.Diagnostics;
using DataVo.Generators.Linq;
using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
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
        IncrementalValueProvider<LinqOptions> options = context.ParseOptionsProvider
            .Combine(context.AnalyzerConfigOptionsProvider)
            .Select(static (pair, _) => OptionsOf(pair.Left, pair.Right.GlobalOptions));

        IncrementalValuesProvider<LinqInspection> calls = context.SyntaxProvider
            .CreateSyntaxProvider(static (node, _) => DataVoLinqAnalyzer.IsCandidate(node), DataVoLinqAnalyzer.Inspect)
            .Where(static inspection => inspection is not null)!;

        IncrementalValueProvider<(ImmutableArray<LinqInspection> Calls, LinqOptions Options)> collected = calls.Collect().Combine(options);
        context.RegisterSourceOutput(collected, static (output, pair) => Emit(output, pair.Calls, pair.Options));

        // Diagnostics are mapped back to the syntax tree of their file, so #pragma warning can suppress them, without
        // the models holding trees: each file's tree meets only the reports that name its path.
        IncrementalValueProvider<EquatableArray<LinqReport>> reports = collected.Select(static (pair, _) => ReportsOf(pair.Calls, pair.Options));
        IncrementalValuesProvider<(SyntaxTree Tree, EquatableArray<LinqReport> Reports)> perFile = context.SyntaxProvider
            .CreateSyntaxProvider(static (node, _) => node is CompilationUnitSyntax, static (syntax, _) => syntax.Node.SyntaxTree)
            .Combine(reports)
            .Select(static (pair, _) => (pair.Left, new EquatableArray<LinqReport>([.. pair.Right.Where(report => report.FilePath == pair.Left.FilePath)])))
            .Where(static pair => pair.Item2.Count > 0);
        context.RegisterSourceOutput(perFile, static (output, file) =>
        {
            foreach (LinqReport report in file.Reports)
            {
                DiagnosticDescriptor descriptor = AllDescriptors.First(candidate => candidate.Id == report.DescriptorId);
                output.ReportDiagnostic(Diagnostic.Create(descriptor, Location.Create(file.Tree, report.Span), report.Arguments.Cast<object?>().ToArray()));
            }
        });
    }

    /// <summary>One diagnostic to report, located by file path and span.</summary>
    private sealed record LinqReport(string DescriptorId, string FilePath, TextSpan Span, EquatableArray<string> Arguments);

    private static readonly DiagnosticDescriptor[] AllDescriptors =
    [
        .. Descriptors,
        DataVoGeneratorDiagnostics.LinqInterceptorsDisabled,
    ];

    /// <summary>The diagnostics of the compilation's calls: per-call reports, or the disabled-interceptor ones.</summary>
    private static EquatableArray<LinqReport> ReportsOf(ImmutableArray<LinqInspection> calls, LinqOptions options)
    {
        if (options.Mode == LinqMode.Unsupported || calls.IsDefaultOrEmpty)
        {
            return default;
        }

        var reports = new List<LinqReport>();
        if (options.Mode == LinqMode.Disabled)
        {
            LinqInspection first = calls[0];
            reports.Add(new LinqReport(DataVoGeneratorDiagnostics.LinqInterceptorsDisabled.Id, first.FilePath, first.Span, default));

            // Fails the build at each ExecuteUpdate: nothing else can run it, so it would otherwise throw when reached.
            foreach (LinqInspection call in calls.Where(static call => call.RequiresInterception))
            {
                reports.Add(new LinqReport(
                    DataVoGeneratorDiagnostics.LinqUpdateNotCompiled.Id,
                    call.FilePath,
                    call.Span,
                    new EquatableArray<string>(["interceptors are not enabled for this project; add <InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces> to the project file"])));
            }

            return new EquatableArray<LinqReport>([.. reports]);
        }

        foreach (LinqInspection call in calls)
        {
            if (call.Diagnostic is { } info)
            {
                reports.Add(new LinqReport(info.DescriptorId, call.FilePath, call.Span, info.Arguments));
            }
        }

        return new EquatableArray<LinqReport>([.. reports]);
    }

    private static void Emit(SourceProductionContext output, ImmutableArray<LinqInspection> calls, LinqOptions options)
    {
        if (options.Mode != LinqMode.Enabled || calls.IsDefaultOrEmpty)
        {
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

        if (options.UsePathLocations)
        {
            sites = [.. sites.Select(static site => site with { Location = site.Location.AsPathForm() })];
            sums = [.. sums.Select(static sum => sum with { Location = sum.Location.AsPathForm() })];
            extremes = [.. extremes.Select(static extreme => extreme with { Location = extreme.Location.AsPathForm() })];
            orders = [.. orders.Select(static order => order with { Call = order.Call with { Location = order.Call.Location.AsPathForm() } })];
            projections = [.. projections.Select(static projection => projection with { Call = projection.Call with { Location = projection.Call.Location.AsPathForm() } })];
            updates = [.. updates.Select(static update => update with { Call = update.Call with { Location = update.Call.Location.AsPathForm() } })];
        }

        var shapes = new LinqShapes(orders, projections, updates);
        if (sites.Count > 0 || sums.Count > 0 || extremes.Count > 0 || !shapes.IsEmpty)
        {
            output.AddSource("DataVoLinqInterceptors.g.cs", SourceText.From(DataVoLinqEmitter.Emit(sites, sums, extremes, shapes, options.FileLocalTypes), System.Text.Encoding.UTF8));
        }
    }

    private static LinqOptions OptionsOf(ParseOptions parse, AnalyzerConfigOptions global)
    {
        bool usePathLocations = global.TryGetValue("build_property.DataVoUsePathLocations", out string? value)
            && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
        LanguageVersion version = parse is CSharpParseOptions csharp ? csharp.LanguageVersion : LanguageVersion.Default;
        bool enabled = Lists(parse, "InterceptorsNamespaces") || Lists(parse, "InterceptorsPreviewNamespaces");
        LinqMode mode = enabled ? LinqMode.Enabled : version >= LanguageVersion.CSharp12 ? LinqMode.Disabled : LinqMode.Unsupported;
        return new LinqOptions(mode, version >= LanguageVersion.CSharp11, usePathLocations);
    }

    /// <summary>Whether a compiler feature lists the generated namespace or one that contains it.</summary>
    private static bool Lists(ParseOptions parse, string feature) =>
        parse.Features.TryGetValue(feature, out string? namespaces)
        && namespaces.Split(';', ',').Select(static name => name.Trim()).Any(static name =>
            name == DataVoLinqEmitter.Namespace || (name.Length > 0 && DataVoLinqEmitter.Namespace.StartsWith(name + ".", StringComparison.Ordinal)));
}
