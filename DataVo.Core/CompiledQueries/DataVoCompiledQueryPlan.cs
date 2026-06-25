namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Describes a supported compiled-query runtime plan emitted by source-generated code.
/// </summary>
public sealed class DataVoCompiledQueryPlan
{
    private DataVoCompiledQueryPlan(
        DataVoCompiledQueryKind kind,
        string tableName,
        IReadOnlyList<string> projectedColumns,
        string? whereColumn,
        string? whereParameterName,
        IReadOnlyList<string> insertColumns,
        IReadOnlyList<string> insertParameterNames,
        IReadOnlyDictionary<string, string> assignments,
        CompiledAccessPath accessPath = CompiledAccessPath.RuntimeResolve,
        string? resolvedIndexName = null)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Compiled query table name cannot be null or whitespace.", nameof(tableName));
        }

        Kind = kind;
        TableName = tableName;
        ProjectedColumns = projectedColumns;
        WhereColumn = whereColumn;
        WhereParameterName = whereParameterName;
        InsertColumns = insertColumns;
        InsertParameterNames = insertParameterNames;
        Assignments = assignments;
        AccessPath = accessPath;
        ResolvedIndexName = resolvedIndexName;
    }

    /// <summary>Gets the compiled-query execution kind.</summary>
    public DataVoCompiledQueryKind Kind { get; }

    /// <summary>Gets the target table name.</summary>
    public string TableName { get; }

    /// <summary>Gets the columns projected by a select plan.</summary>
    public IReadOnlyList<string> ProjectedColumns { get; }

    /// <summary>Gets the column used by the equality predicate, when applicable.</summary>
    public string? WhereColumn { get; }

    /// <summary>Gets the parameter name used by the equality predicate, when applicable.</summary>
    public string? WhereParameterName { get; }

    /// <summary>Gets the columns written by an insert plan.</summary>
    public IReadOnlyList<string> InsertColumns { get; }

    /// <summary>Gets the parameter names used by an insert plan.</summary>
    public IReadOnlyList<string> InsertParameterNames { get; }

    /// <summary>Gets update column assignments mapped to parameter names.</summary>
    public IReadOnlyDictionary<string, string> Assignments { get; }

    /// <summary>Gets the access path pre-resolved at compile time, or <see cref="CompiledAccessPath.RuntimeResolve"/>.</summary>
    public CompiledAccessPath AccessPath { get; }

    /// <summary>Gets the index name resolved at compile time when <see cref="AccessPath"/> is <see cref="CompiledAccessPath.SingleColumnIndex"/>; otherwise null.</summary>
    public string? ResolvedIndexName { get; }

    /// <summary>
    /// Creates a plan that returns the first row matching an equality predicate.
    /// </summary>
    public static DataVoCompiledQueryPlan SelectSingle(
        string tableName,
        IReadOnlyList<string> projectedColumns,
        string whereColumn,
        string parameterName)
    {
        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.SelectSingle,
            tableName,
            projectedColumns ?? throw new ArgumentNullException(nameof(projectedColumns)),
            RequireIdentifier(whereColumn, nameof(whereColumn)),
            RequireIdentifier(parameterName, nameof(parameterName)),
            [],
            [],
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Creates a plan that returns all rows matching an equality predicate. When <paramref name="accessPath"/>
    /// is <see cref="CompiledAccessPath.SingleColumnIndex"/>, the runtime routes directly through
    /// <paramref name="resolvedIndexName"/>, falling back to runtime resolution if that index is missing.
    /// </summary>
    public static DataVoCompiledQueryPlan SelectMany(
        string tableName,
        IReadOnlyList<string> projectedColumns,
        string whereColumn,
        string parameterName,
        CompiledAccessPath accessPath = CompiledAccessPath.RuntimeResolve,
        string? resolvedIndexName = null)
    {
        if (accessPath == CompiledAccessPath.SingleColumnIndex && string.IsNullOrWhiteSpace(resolvedIndexName))
        {
            throw new ArgumentException(
                "A SingleColumnIndex access path requires a resolved index name.",
                nameof(resolvedIndexName));
        }

        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.SelectMany,
            tableName,
            projectedColumns ?? throw new ArgumentNullException(nameof(projectedColumns)),
            RequireIdentifier(whereColumn, nameof(whereColumn)),
            RequireIdentifier(parameterName, nameof(parameterName)),
            [],
            [],
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            accessPath,
            resolvedIndexName);
    }

    /// <summary>
    /// Creates a plan that inserts one row using column-to-parameter mapping.
    /// </summary>
    public static DataVoCompiledQueryPlan Insert(
        string tableName,
        IReadOnlyList<string> columns,
        IReadOnlyList<string> parameterNames)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(parameterNames);

        if (columns.Count != parameterNames.Count)
        {
            throw new ArgumentException("Compiled insert plans require the same number of columns and parameter names.");
        }

        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.Insert,
            tableName,
            [],
            null,
            null,
            columns.Select((column, index) => RequireIdentifier(column, $"{nameof(columns)}[{index}]")).ToArray(),
            parameterNames.Select((name, index) => RequireIdentifier(name, $"{nameof(parameterNames)}[{index}]")).ToArray(),
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Creates a plan that updates rows matching an equality predicate.
    /// </summary>
    public static DataVoCompiledQueryPlan Update(
        string tableName,
        IReadOnlyDictionary<string, string> assignments,
        string whereColumn,
        string whereParameterName)
    {
        ArgumentNullException.ThrowIfNull(assignments);
        if (assignments.Count == 0)
        {
            throw new ArgumentException("Compiled update plans require at least one assignment.", nameof(assignments));
        }

        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.Update,
            tableName,
            [],
            RequireIdentifier(whereColumn, nameof(whereColumn)),
            RequireIdentifier(whereParameterName, nameof(whereParameterName)),
            [],
            [],
            assignments.ToDictionary(
                pair => RequireIdentifier(pair.Key, nameof(assignments)),
                pair => RequireIdentifier(pair.Value, nameof(assignments)),
                StringComparer.OrdinalIgnoreCase));
    }

    private static string RequireIdentifier(string value, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Compiled query identifiers cannot be null or whitespace.", paramName);
        }

        return value;
    }
}
