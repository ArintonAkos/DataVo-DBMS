using DataVo.Generators.Tables;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace DataVo.Generators.Linq;

/// <summary>One value bound to a statement parameter, as C# source over the interceptor's <c>state</c> argument.</summary>
/// <param name="Value">The expression that produces the value.</param>
/// <param name="Guard">An expression that is true when the value is not null, or null when it never is.</param>
/// <param name="UsesObjectParameter">Whether the value is bound through <c>DataVoQueryParameter.FromObject</c>.</param>
/// <param name="KernelType">The C# type a compiled condition holds the value as, or null when the type has no direct reader.</param>
internal sealed record LinqParameter(string Value, string? Guard, bool UsesObjectParameter, string? KernelType = null);

/// <summary>A condition as C# over a <c>RowRef</c>, compiled into the scan loop for queries that only count or test rows.</summary>
/// <param name="Expression">The condition; columns appear as <c>c0</c>, <c>c1</c>... and values as <c>p0</c>, <c>p1</c>...</param>
/// <param name="Columns">The columns the condition reads, in <c>c</c> order.</param>
internal sealed record LinqKernel(string Expression, EquatableArray<string> Columns);

/// <summary>Where a call is, in both forms the compiler accepts for <c>[InterceptsLocation]</c>.</summary>
/// <param name="Version">The encoding version, or 0 when only the path form is available.</param>
/// <param name="Data">The encoded location for <paramref name="Version"/>.</param>
internal sealed record LinqLocation(int Version, string Data, string Path, int Line, int Column);

/// <summary>A table query the generator translated, ready to emit as an interceptor.</summary>
internal sealed record LinqSite(
    LinqLocation Location,
    string Method,
    bool Stateful,
    string ModelType,
    bool ModelIsReferenceType,
    string StateType,
    string Where,
    bool Unique,
    EquatableArray<LinqParameter> Parameters,
    LinqKernel? Kernel = null);

/// <summary>The outcome of inspecting one call: a translated site, or a diagnostic explaining why it runs as a scan.</summary>
/// <param name="Tree">The file the call is in. Kept so a diagnostic lands in source, where <c>#pragma warning</c> can reach it.</param>
/// <param name="Span">The lambda's position in <paramref name="Tree"/>.</param>
internal sealed record LinqInspection(LinqSite? Site, DiagnosticInfo? Diagnostic, SyntaxTree Tree, TextSpan Span);

/// <summary>Whether the compilation can take interceptors, and which syntax the output may use.</summary>
internal sealed record LinqOptions(LinqMode Mode, bool FileLocalTypes);

internal enum LinqMode
{
    /// <summary>The compiler predates interceptors (Unity); queries run through the fallback, silently.</summary>
    Unsupported,

    /// <summary>The compiler supports interceptors but the project has not enabled the DataVo namespace.</summary>
    Disabled,

    /// <summary>Interceptors are emitted.</summary>
    Enabled,
}
