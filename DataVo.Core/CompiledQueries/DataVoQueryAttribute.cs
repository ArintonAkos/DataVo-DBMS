namespace DataVo.Core.CompiledQueries;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class DataVoQueryAttribute(string sql) : Attribute
{
    public string Sql { get; } = sql;

    public DataVoCompiledQueryKind Kind { get; set; } = DataVoCompiledQueryKind.Auto;
}
