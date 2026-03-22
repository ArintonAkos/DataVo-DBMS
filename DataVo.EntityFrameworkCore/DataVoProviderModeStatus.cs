namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Represents the currently active DataVo EF operating mode.
/// </summary>
public enum DataVoProviderMode
{
    /// <summary>
    /// Bridge-only mode using a host provider and DataVo as a persistence/query-refresh bridge.
    /// </summary>
    BridgeOnly = 0,

    /// <summary>
    /// Provider identity preview mode is enabled.
    /// </summary>
    ProviderIdentityPreview = 1,

    /// <summary>
    /// Native query translation preview mode is enabled.
    /// </summary>
    NativeTranslationPreview = 2
}

/// <summary>
/// Snapshot of the active DataVo provider mode and preview flags.
/// </summary>
public sealed record DataVoProviderModeStatus(
    DataVoProviderMode Mode,
    bool ProviderIdentityPreviewEnabled,
    bool NativeQueryTranslationPreviewEnabled)
{
    /// <summary>Whether the bridge is still operating in bridge-only mode.</summary>
    public bool IsBridgeOnlyMode => Mode == DataVoProviderMode.BridgeOnly;
}
