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
/// <param name="BytesExpression">The condition over a row's stored bytes, with each column's value at offset <c>o0</c>, <c>o1</c>...</param>
/// <param name="CellsExpression">The condition over a typed row's cells, indexed by <c>c0</c>, <c>c1</c>...</param>
/// <param name="Columns">The columns the condition reads, in <c>c</c> order.</param>
/// <param name="Kinds">One letter per column: how the condition reads it (<c>i l f d b g</c>), or <c>n</c> for a null test only.</param>
internal sealed record LinqKernel(string Expression, string BytesExpression, string CellsExpression, EquatableArray<string> Columns, string Kinds);

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

/// <summary>A <c>Sum</c> over a query whose selector is one numeric column, summed inside the scan when the query's plan can.</summary>
/// <param name="ValueType">The C# type summed: <c>int</c>, <c>long</c>, <c>float</c> or <c>double</c>.</param>
/// <param name="RuntimeMethod">The <c>DataVoQuery</c> method that sums a column of that type.</param>
/// <param name="Column">The summed column.</param>
internal sealed record LinqSumSite(LinqLocation Location, string ModelType, string StateType, string ValueType, string RuntimeMethod, string Column);

/// <summary>
/// A <c>Min</c> or <c>Max</c> over a query whose selector is one numeric or date column, found inside the scan when
/// the query's plan can.
/// </summary>
/// <param name="Largest">Whether the call is <c>Max</c> rather than <c>Min</c>.</param>
/// <param name="ValueType">The C# type compared, with its <c>?</c> when the column is nullable.</param>
/// <param name="Nullable">Whether the column is nullable, so an empty result is null rather than an exception.</param>
/// <param name="Kind">How the scan reads the column: <c>i l f d</c>, or <c>t</c> for a date.</param>
/// <param name="Result">The C# expression that turns the scan's <c>totals</c> into the value.</param>
/// <param name="Column">The column.</param>
internal sealed record LinqExtremeSite(LinqLocation Location, bool Largest, string ModelType, string StateType, string ValueType, bool Nullable, char Kind, string Result, string Column);

/// <summary>The outcome of inspecting one call: a translated site, or a diagnostic explaining why it runs as a scan.</summary>
/// <param name="Tree">The file the call is in. Kept so a diagnostic lands in source, where <c>#pragma warning</c> can reach it.</param>
/// <param name="Span">The lambda's position in <paramref name="Tree"/>.</param>
/// <param name="Sum">A translated <c>Sum</c> call, when the call is one.</param>
/// <param name="Extreme">A translated <c>Min</c> or <c>Max</c> call, when the call is one.</param>
internal sealed record LinqInspection(LinqSite? Site, DiagnosticInfo? Diagnostic, SyntaxTree Tree, TextSpan Span, LinqSumSite? Sum = null, LinqExtremeSite? Extreme = null);

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
