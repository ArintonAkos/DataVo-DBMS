namespace DataVo.Core.CompiledQueries;

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
        IReadOnlyDictionary<string, string> assignments)
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
    }

    public DataVoCompiledQueryKind Kind { get; }

    public string TableName { get; }

    public IReadOnlyList<string> ProjectedColumns { get; }

    public string? WhereColumn { get; }

    public string? WhereParameterName { get; }

    public IReadOnlyList<string> InsertColumns { get; }

    public IReadOnlyList<string> InsertParameterNames { get; }

    public IReadOnlyDictionary<string, string> Assignments { get; }

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

    public static DataVoCompiledQueryPlan SelectMany(
        string tableName,
        IReadOnlyList<string> projectedColumns,
        string whereColumn,
        string parameterName)
    {
        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.SelectMany,
            tableName,
            projectedColumns ?? throw new ArgumentNullException(nameof(projectedColumns)),
            RequireIdentifier(whereColumn, nameof(whereColumn)),
            RequireIdentifier(parameterName, nameof(parameterName)),
            [],
            [],
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
    }

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
