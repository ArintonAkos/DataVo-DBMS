namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Marks a partial method as a DataVo compiled query source-generation target.
/// </summary>
/// <param name="sql">The SQL statement the generator should compile.</param>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class DataVoQueryAttribute(string sql) : Attribute
{
    /// <summary>
    /// Gets the SQL statement supplied to the source generator.
    /// </summary>
    public string Sql { get; } = sql;

    /// <summary>
    /// Gets or sets the intended compiled-query kind, or <see cref="DataVoCompiledQueryKind.Auto"/> to infer it from SQL.
    /// </summary>
    public DataVoCompiledQueryKind Kind { get; set; } = DataVoCompiledQueryKind.Auto;
}
