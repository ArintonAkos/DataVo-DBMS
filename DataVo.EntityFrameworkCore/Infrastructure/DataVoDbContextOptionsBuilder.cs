using DataVo.EntityFrameworkCore.Infrastructure.Internal;

namespace DataVo.EntityFrameworkCore.Infrastructure;

/// <summary>
/// Builder for DataVo-specific EF Core options used by <c>UseDataVo</c>.
/// </summary>
public sealed class DataVoDbContextOptionsBuilder
{
    private readonly DataVoOptionsExtension _extension;

    internal DataVoDbContextOptionsBuilder(DataVoOptionsExtension extension)
    {
        _extension = extension;
    }

    // ------------------------------------------------------------------ storage mode

    /// <summary>
    /// Configures DataVo to use disk-based (persistent) storage.
    /// Equivalent to <c>StorageMode=Disk</c> in the connection string.
    /// </summary>
    public DataVoDbContextOptionsBuilder UseDiskStorage()
    {
        _extension.WithStorageMode(DataVoStorageMode.Disk);
        return this;
    }

    /// <summary>
    /// Configures DataVo to use in-memory (non-persistent) storage.
    /// Equivalent to <c>StorageMode=InMemory</c> in the connection string.
    /// </summary>
    public DataVoDbContextOptionsBuilder UseInMemoryStorage()
    {
        _extension.WithStorageMode(DataVoStorageMode.InMemory);
        return this;
    }

    /// <summary>
    /// Configures which storage mode DataVo should use.
    /// </summary>
    public DataVoDbContextOptionsBuilder UseStorageMode(DataVoStorageMode mode)
    {
        _extension.WithStorageMode(mode);
        return this;
    }

    // ------------------------------------------------------------------ data source

    /// <summary>
    /// Sets the database name / data-source used by DataVo.
    /// When combined with <see cref="UseStorageMode"/> this fully replaces the need to
    /// write a manual connection string.
    /// </summary>
    public DataVoDbContextOptionsBuilder WithDataSource(string dataSource)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataSource);
        _extension.WithDataSource(dataSource);
        return this;
    }

    // ------------------------------------------------------------------ diagnostics

    /// <summary>
    /// Enables generation of detailed provider bootstrap diagnostics.
    /// </summary>
    public DataVoDbContextOptionsBuilder EnableBootstrapDiagnostics(bool enabled = true)
    {
        _extension.WithBootstrapDiagnostics(enabled);
        return this;
    }

    /// <summary>
    /// Enables the provider-identity preview surface. This does not yet replace the host
    /// provider, but exposes incremental preview behavior and status APIs for future work.
    /// </summary>
    public DataVoDbContextOptionsBuilder EnableProviderIdentityPreview(bool enabled = true)
    {
        _extension.WithProviderIdentityPreview(enabled);
        return this;
    }

    /// <summary>
    /// Enables the native query translation preview flag. This currently acts as a preview
    /// capability toggle and implies provider identity preview.
    /// </summary>
    public DataVoDbContextOptionsBuilder EnableNativeQueryTranslationPreview(bool enabled = true)
    {
        _extension.WithNativeQueryTranslationPreview(enabled);

        if (enabled)
        {
            _extension.WithProviderIdentityPreview(true);
        }

        return this;
    }
}

/// <summary>
/// Public snapshot of DataVo bootstrap options attached to EF Core options.
/// </summary>
public sealed record DataVoBootstrapOptions(
    string? ConnectionString,
    DataVoStorageMode? StorageMode,
    string? DataSource,
    bool BootstrapDiagnosticsEnabled,
    bool ProviderIdentityPreviewEnabled,
    bool NativeQueryTranslationPreviewEnabled)
{
    /// <summary>Returns the effective connection string used at runtime.</summary>
    public string? EffectiveConnectionString =>
        StorageMode is not null && DataSource is { Length: > 0 }
            ? $"StorageMode={(StorageMode == DataVoStorageMode.InMemory ? "InMemory" : "Disk")};DataSource={DataSource}"
            : ConnectionString;

    /// <summary>Whether the bridge is still operating in bridge-only mode.</summary>
    public bool IsBridgeOnlyMode => !ProviderIdentityPreviewEnabled && !NativeQueryTranslationPreviewEnabled;
}

