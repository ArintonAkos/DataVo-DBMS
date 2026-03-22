namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Provider-identity parity snapshot for the active DataVo EF bridge context.
/// </summary>
public sealed record DataVoProviderIdentityParityReport(
    DataVoProviderModeStatus ModeStatus,
    string? HostProviderName,
    bool HostProviderConfigured,
    IReadOnlyList<string> NativePreviewOperators,
    IReadOnlyList<string> BlockedOperators,
    int MappedEntityTypeCount,
    int QueryableEntityTypeCount,
    IReadOnlyList<string> MetadataWarnings)
{
    /// <summary>Whether metadata checks found no parity warnings.</summary>
    public bool MetadataParitySatisfied => MetadataWarnings.Count == 0;
}