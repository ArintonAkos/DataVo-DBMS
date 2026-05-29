namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Describes the compiled-query execution shape expected by generated code.
/// </summary>
public enum DataVoCompiledQueryKind
{
    /// <summary>Infer the query shape from the SQL statement.</summary>
    Auto,
    /// <summary>Return the first matching row or a default value.</summary>
    SelectSingle,
    /// <summary>Return all matching rows.</summary>
    SelectMany,
    /// <summary>Insert a single row and return inserted row identifiers.</summary>
    Insert,
    /// <summary>Update matching rows and return the affected row count.</summary>
    Update
}
