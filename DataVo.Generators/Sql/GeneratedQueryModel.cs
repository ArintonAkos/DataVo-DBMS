namespace DataVo.Generators.Sql;

internal sealed class GeneratedQueryModel
{
    public GeneratedQueryModel(
        string kind,
        string tableName,
        string[] projectedColumns,
        string? whereColumn,
        string? whereParameterName,
        string[] insertColumns,
        string[] insertParameterNames,
        IReadOnlyDictionary<string, string> assignments)
    {
        Kind = kind;
        TableName = tableName;
        ProjectedColumns = projectedColumns;
        WhereColumn = whereColumn;
        WhereParameterName = whereParameterName;
        InsertColumns = insertColumns;
        InsertParameterNames = insertParameterNames;
        Assignments = assignments;
    }

    public string Kind { get; }

    public string TableName { get; }

    public string[] ProjectedColumns { get; }

    public string? WhereColumn { get; }

    public string? WhereParameterName { get; }

    public string[] InsertColumns { get; }

    public string[] InsertParameterNames { get; }

    public IReadOnlyDictionary<string, string> Assignments { get; }
}
