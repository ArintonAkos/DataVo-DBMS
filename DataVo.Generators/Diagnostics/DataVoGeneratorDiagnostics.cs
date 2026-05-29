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
}
