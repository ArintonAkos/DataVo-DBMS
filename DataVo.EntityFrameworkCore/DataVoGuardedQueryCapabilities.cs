namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Describes the currently supported and blocked LINQ operators for guarded
/// DataVo query execution.
/// </summary>
public sealed record DataVoGuardedQueryCapabilities(
    IReadOnlyList<string> SupportedOperators,
    IReadOnlyList<string> BlockedOperators,
    string Guidance);
