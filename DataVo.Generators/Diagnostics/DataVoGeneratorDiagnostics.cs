using Microsoft.CodeAnalysis;

namespace DataVo.Generators.Diagnostics;

internal static class DataVoGeneratorDiagnostics
{
    public static readonly DiagnosticDescriptor UnsupportedSql = new(
        "DATAVOQ001",
        "Unsupported DataVo compiled query SQL",
        "SQL is not supported by the DataVo source generator: {0}",
        "DataVo.CompiledQueries",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor MissingParameter = new(
        "DATAVOQ002",
        "Missing DataVo compiled query parameter",
        "SQL parameter '{0}' has no matching method parameter",
        "DataVo.CompiledQueries",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor MissingPrimaryKey = Table(
        "DATAVOT001", "DataVo table has no primary key", "Model '{0}' needs exactly one property marked [PrimaryKey]");

    public static readonly DiagnosticDescriptor MultiplePrimaryKeys = Table(
        "DATAVOT002", "DataVo table has several primary keys", "Model '{0}' marks several properties [PrimaryKey]; composite keys are not supported");

    public static readonly DiagnosticDescriptor UnsupportedPropertyType = Table(
        "DATAVOT003", "Unsupported DataVo column type", "Property '{0}' has type '{1}', which DataVo cannot store without losing data. Supported types are int, long, float, double, bool, string, Guid, DataVoDate and DateOnly, and their nullable forms.");

    public static readonly DiagnosticDescriptor NullablePrimaryKey = Table(
        "DATAVOT004", "DataVo primary key is nullable", "Primary key '{0}' cannot be nullable");

    public static readonly DiagnosticDescriptor NoUsableConstructor = Table(
        "DATAVOT005", "DataVo model cannot be constructed", "Model '{0}' needs an accessible parameterless constructor, or one whose parameters all match columns");

    public static readonly DiagnosticDescriptor UnsupportedModelShape = Table(
        "DATAVOT006", "Unsupported DataVo model", "Model '{0}' cannot be generic, abstract, or private: {1}");

    public static readonly DiagnosticDescriptor InvalidTableName = Table(
        "DATAVOT007", "Invalid DataVo table name", "'{0}' is not a valid table or column name");

    public static readonly DiagnosticDescriptor LinqCapturedVariable = Linq(
        "DATAVOL001",
        "DataVo query captures a variable",
        "This predicate captures {0}, so it cannot be compiled ahead of time and reads the whole table. Pass the value as state with a static lambda, for example Where(value, static (row, v) => row.Column == v), to get a prepared lookup with no closure.");

    public static readonly DiagnosticDescriptor LinqNotTranslated = Linq(
        "DATAVOL002",
        "DataVo query is not compiled",
        "This predicate reads the whole table because it cannot be compiled ahead of time: {0}");

    public static readonly DiagnosticDescriptor LinqInterceptorsDisabled = Linq(
        "DATAVOL003",
        "DataVo query interceptors are disabled",
        "DataVoTable queries read the whole table because interceptors are not enabled for this project. Add <InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces> to the project file.");

    private static DiagnosticDescriptor Linq(string id, string title, string message) =>
        new(id, title, message, "DataVo.Linq", DiagnosticSeverity.Warning, isEnabledByDefault: true);

    private static DiagnosticDescriptor Table(string id, string title, string message) =>
        new(id, title, message, "DataVo.Tables", DiagnosticSeverity.Error, isEnabledByDefault: true);
}
