namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Represents a named parameter value passed to a compiled query runtime plan.
/// </summary>
/// <param name="Name">The SQL parameter name without an <c>@</c> prefix.</param>
/// <param name="Value">The runtime value for the parameter.</param>
public readonly record struct DataVoCompiledQueryParameter(string Name, object? Value);
