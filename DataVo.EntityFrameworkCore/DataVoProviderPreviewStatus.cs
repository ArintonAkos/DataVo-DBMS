namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Snapshot of the DataVo provider-preview state attached to EF Core options or a live context.
/// </summary>
public sealed record DataVoProviderPreviewStatus(
    string? EffectiveConnectionString,
    bool ProviderIdentityPreviewEnabled,
    bool NativeQueryTranslationPreviewEnabled,
    bool IsBridgeOnlyMode);
