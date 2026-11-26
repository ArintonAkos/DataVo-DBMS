using DataVo.Generators.Tables;

namespace DataVo.Generators.Sql;

/// <summary>How a <c>[DataVoQuery]</c> method executes.</summary>
internal enum QueryExecutionShape
{
    SelectSingle,
    SelectMany,
    Insert,
    Update,
}

/// <summary>
/// Everything the emitter needs about one <c>[DataVoQuery]</c> method, as plain strings: it holds no compiler symbols
/// or syntax, so the incremental pipeline can compare it between builds and skip unchanged methods.
/// </summary>
/// <param name="HintName">A file name unique across namespaces, nested types and overloads.</param>
/// <param name="Namespace">The containing namespace, or null for the global namespace.</param>
/// <param name="ContainingTypes">The declarations of the containing types, outermost first.</param>
/// <param name="MethodSignature">The implementing declaration: accessibility, <c>static partial</c>, return type, name and parameters.</param>
/// <param name="MemberSuffix">A suffix that keeps the generated helper members of overloads apart.</param>
/// <param name="DbParameter">The name of the <c>DataVoContext</c> parameter.</param>
/// <param name="SqlParameters">The SQL parameter names, in binding order.</param>
/// <param name="MethodArguments">The method parameters bound to <paramref name="SqlParameters"/>, in the same order.</param>
/// <param name="RowType">The fully qualified row type of a SELECT; null otherwise.</param>
/// <param name="TypedGetters">The <c>CompiledRowReader</c> getter per constructor argument, when every argument has one.</param>
/// <param name="DictionaryArguments">The constructor arguments read from a result dictionary (the fallback mapper).</param>
internal sealed record QueryMethodModel(
    string HintName,
    string? Namespace,
    EquatableArray<string> ContainingTypes,
    string MethodSignature,
    string MemberSuffix,
    QueryExecutionShape Shape,
    string TableName,
    EquatableArray<string> ProjectedColumns,
    string? WhereColumn,
    string? WhereParameterName,
    EquatableArray<string> InsertColumns,
    EquatableArray<string> InsertParameterNames,
    EquatableArray<string> AssignmentColumns,
    EquatableArray<string> AssignmentParameters,
    string DbParameter,
    EquatableArray<string> SqlParameters,
    EquatableArray<string> MethodArguments,
    string? RowType,
    EquatableArray<string> TypedGetters,
    EquatableArray<string> DictionaryArguments)
{
    /// <summary>Gets whether every constructor argument has a typed getter.</summary>
    public bool HasTypedGetters => RowType is not null && TypedGetters.Count > 0;
}

/// <summary>The outcome of inspecting one method: a model to emit, or a diagnostic.</summary>
internal sealed record QueryMethodInspection(QueryMethodModel? Model, DiagnosticInfo? Diagnostic);
