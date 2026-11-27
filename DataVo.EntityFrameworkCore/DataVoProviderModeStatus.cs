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
    /// Native query translation preview mode is enabled: supported no-tracking LINQ shapes run as
    /// DataVo SQL instead of refreshing the EF mirror.
    /// </summary>
    NativeTranslationPreview = 2
}

/// <summary>
/// Snapshot of the active DataVo provider mode.
/// </summary>
public sealed record DataVoProviderModeStatus(DataVoProviderMode Mode)
{
    /// <summary>Whether native query translation preview is enabled.</summary>
    public bool NativeQueryTranslationPreviewEnabled => Mode == DataVoProviderMode.NativeTranslationPreview;

    /// <summary>Whether the bridge is still operating in bridge-only mode.</summary>
    public bool IsBridgeOnlyMode => Mode == DataVoProviderMode.BridgeOnly;
}
