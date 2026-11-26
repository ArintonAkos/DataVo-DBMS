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
/// <param name="Describe">The condition as calls on a <c>ColumnarPredicateBuilder</c> named <c>builder</c>, in postfix order, for tables with a columnar index.</param>
internal sealed record LinqKernel(string Expression, string BytesExpression, string CellsExpression, EquatableArray<string> Columns, string Kinds, EquatableArray<string> Describe);

/// <summary>Where a call is, in both forms the compiler accepts for <c>[InterceptsLocation]</c>.</summary>
/// <param name="Version">The encoding version, or 0 when only the path form is available.</param>
/// <param name="Data">The encoded location for <paramref name="Version"/>.</param>
internal sealed record LinqLocation(int Version, string Data, string Path, int Line, int Column)
{
    /// <summary>This location in the path, line and column form an older compiler needs.</summary>
    public LinqLocation AsPathForm() => Version == 0 ? this : this with { Version = 0, Data = string.Empty };
}

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
    LinqKernel? Kernel = null,
    bool IntegerKeyLookup = false);

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

/// <summary>Which DataVo type a composed call is made on, which fixes the interceptor's receiver.</summary>
internal enum LinqReceiver
{
    /// <summary><c>DataVoTable&lt;T&gt;</c>, a class.</summary>
    Table,

    /// <summary><c>DataVoQuery&lt;T, TState&gt;</c>, a struct.</summary>
    Query,

    /// <summary><c>DataVoOrderedQuery&lt;T, TState&gt;</c>, a struct.</summary>
    OrderedQuery,

    /// <summary><c>DataVoPagedQuery&lt;T, TState&gt;</c>, a struct.</summary>
    PagedQuery,
}

/// <summary>The receiver and return type of an intercepted call on a DataVo table or query.</summary>
/// <param name="Kind">The DataVo type the call is made on.</param>
/// <param name="ReceiverType">The receiver's fully qualified type.</param>
/// <param name="ReturnType">The called method's fully qualified return type.</param>
/// <param name="ModelType">The model's fully qualified type.</param>
internal sealed record LinqCall(LinqLocation Location, LinqReceiver Kind, string ReceiverType, string ReturnType, string ModelType);

/// <summary>An <c>OrderBy</c>, <c>OrderByDescending</c>, <c>ThenBy</c> or <c>ThenByDescending</c> whose key is one column.</summary>
/// <param name="Method">The intercepted method's name.</param>
/// <param name="KeyType">The key's fully qualified type: the column's property type.</param>
/// <param name="Column">The column rows are sorted by.</param>
/// <param name="Descending">Whether the call sorts from largest to smallest.</param>
internal sealed record LinqOrderSite(LinqCall Call, string Method, string KeyType, string Column, bool Descending);

/// <summary>A <c>Select</c> whose selector was rewritten to build its result from the columns it reads.</summary>
/// <param name="ResultType">The projected type, or null when generated code cannot name it (an anonymous type), so the interceptor is generic.</param>
/// <param name="Columns">The columns the projection reads, each once, in the order the selector first reads them.</param>
/// <param name="Body">The selector's body as C# over a <c>DataVoRow</c> named <c>row</c>.</param>
/// <param name="ReactiveBody">The selector's body as C# over a reactive <c>RowRef</c> named <c>row</c> reading columns by constant ordinal.</param>
internal sealed record LinqProjectionSite(LinqCall Call, string? ResultType, EquatableArray<string> Columns, string Body, string ReactiveBody);

/// <summary>An <c>ExecuteUpdate</c> whose setters were translated into a SET list.</summary>
/// <param name="StateType">The state value's fully qualified type, or null for the overload without state.</param>
/// <param name="Assignments">The SET list over <c>@u0</c>, <c>@u1</c>...</param>
/// <param name="Values">The C# that binds each <c>@u</c> parameter, in order.</param>
internal sealed record LinqUpdateSite(LinqCall Call, string? StateType, string Assignments, EquatableArray<string> Values);

/// <summary>The outcome of inspecting one call: a translated site, or a diagnostic explaining why it runs as a scan.</summary>
/// <param name="FilePath">The path of the file the call is in. The generator maps it back to the file's syntax tree when it
/// reports a diagnostic (so <c>#pragma warning</c> can reach it); the model itself holds no syntax tree, so cached
/// models never keep old trees alive.</param>
/// <param name="Span">The lambda's position in the file.</param>
/// <param name="Sum">A translated <c>Sum</c> call, when the call is one.</param>
/// <param name="Extreme">A translated <c>Min</c> or <c>Max</c> call, when the call is one.</param>
/// <param name="Order">A translated sort key, when the call is one.</param>
/// <param name="Projection">A translated <c>Select</c>, when the call is one.</param>
/// <param name="Update">A translated <c>ExecuteUpdate</c>, when the call is one.</param>
/// <param name="RequiresInterception">Whether the call fails at run time unless it is intercepted, as <c>ExecuteUpdate</c> does.</param>
internal sealed record LinqInspection(
    LinqSite? Site,
    DiagnosticInfo? Diagnostic,
    string FilePath,
    TextSpan Span,
    LinqSumSite? Sum = null,
    LinqExtremeSite? Extreme = null,
    LinqOrderSite? Order = null,
    LinqProjectionSite? Projection = null,
    LinqUpdateSite? Update = null,
    bool RequiresInterception = false);

/// <summary>The sort keys, projections and updates of one compilation, in the order they were found.</summary>
internal sealed record LinqShapes(IReadOnlyList<LinqOrderSite> Orders, IReadOnlyList<LinqProjectionSite> Projections, IReadOnlyList<LinqUpdateSite> Updates)
{
    /// <summary>Gets whether there is nothing to emit.</summary>
    public bool IsEmpty => Orders.Count == 0 && Projections.Count == 0 && Updates.Count == 0;
}

/// <summary>Whether the compilation can take interceptors, and which syntax the output may use.</summary>
/// <param name="UsePathLocations">Emit the path, line and column form of <c>[InterceptsLocation]</c> even when the compiler
/// offers the encoded form (the <c>DataVoUsePathLocations</c> MSBuild property).</param>
internal sealed record LinqOptions(LinqMode Mode, bool FileLocalTypes, bool UsePathLocations = false);

internal enum LinqMode
{
    /// <summary>The compiler predates interceptors (Unity); queries run through the fallback, silently.</summary>
    Unsupported,

    /// <summary>The compiler supports interceptors but the project has not enabled the DataVo namespace.</summary>
    Disabled,

    /// <summary>Interceptors are emitted.</summary>
    Enabled,
}
