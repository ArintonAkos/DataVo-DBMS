using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Immutable EF Core options extension holding the DataVo bridge configuration. Every <c>With*</c>
/// method returns a modified copy, as EF Core's options contract requires.
/// </summary>
internal sealed class DataVoOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DataVoOptionsExtension()
    {
    }

    private DataVoOptionsExtension(DataVoOptionsExtension copyFrom)
    {
        ConnectionString = copyFrom.ConnectionString;
        StorageMode = copyFrom.StorageMode;
        DataSource = copyFrom.DataSource;
        NativeQueryTranslationPreviewEnabled = copyFrom.NativeQueryTranslationPreviewEnabled;
    }

    /// <summary>Full ADO.NET connection string as supplied to <c>UseDataVo</c>.</summary>
    public string? ConnectionString { get; private init; }

    /// <summary>
    /// Typed storage mode override.
    /// When set, <c>BuildEffectiveConnectionString</c> synthesises the connection string from
    /// <see cref="StorageMode"/> and <see cref="DataSource"/> instead of using the raw string.
    /// </summary>
    public DataVoStorageMode? StorageMode { get; private init; }

    /// <summary>
    /// Typed data-source / database name override (e.g. "mydb").
    /// Used together with <see cref="StorageMode"/> to build the effective connection string.
    /// </summary>
    public string? DataSource { get; private init; }

    /// <summary>Whether supported LINQ shapes are translated to DataVo SQL instead of refreshing the EF mirror.</summary>
    public bool NativeQueryTranslationPreviewEnabled { get; private init; }

    public DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    // ------------------------------------------------------------------ service registration

    public void ApplyServices(IServiceCollection services)
    {
        services.AddEntityFrameworkDataVo();
    }

    public void Validate(IDbContextOptions options)
    {
        // If typed fluent options are partially set, validate they are complete.
        if (StorageMode is not null && string.IsNullOrWhiteSpace(DataSource))
        {
            throw new InvalidOperationException(
                "DataVo configuration error: UseStorageMode() was set but WithDataSource() was not called. " +
                "Provide a DataSource name (e.g. UseDataVo(o => o.UseLsmStorage().WithDataSource(\"mydb\"))).");
        }

        // DataVo is an EF bridge extension (not a standalone EF provider),
        // so an actual host provider (e.g. InMemory) must also be configured.
        bool hasHostProvider = options.Extensions.Any(extension =>
            extension.Info.IsDatabaseProvider &&
            extension is not DataVoOptionsExtension);

        if (!hasHostProvider)
        {
            throw new InvalidOperationException(
                "DataVo EF bridge requires a host EF provider. Configure one (for example " +
                "UseInMemoryDatabase(...)) before calling UseDataVo(...). " +
                "DataVo is currently a persistence bridge, not a standalone LINQ provider.");
        }
    }

    // ------------------------------------------------------------------ immutable copies

    public DataVoOptionsExtension WithConnectionString(string connectionString)
        => new(this) { ConnectionString = connectionString };

    public DataVoOptionsExtension WithStorageMode(DataVoStorageMode mode)
        => new(this) { StorageMode = mode };

    public DataVoOptionsExtension WithDataSource(string dataSource)
        => new(this) { DataSource = dataSource };

    public DataVoOptionsExtension WithNativeQueryTranslationPreview(bool enabled)
        => new(this) { NativeQueryTranslationPreviewEnabled = enabled };

    // ------------------------------------------------------------------ effective connection string

    /// <summary>
    /// Returns the connection string to use at runtime.
    /// If <see cref="StorageMode"/> and <see cref="DataSource"/> are both set they take priority
    /// (letting callers configure storage purely through the typed fluent API without writing a
    /// raw connection string).  Otherwise falls back to <see cref="ConnectionString"/>.
    /// </summary>
    public string? BuildEffectiveConnectionString()
        => BuildEffectiveConnectionString(ConnectionString, StorageMode, DataSource);

    /// <summary>Builds the effective connection string from raw and typed options.</summary>
    internal static string? BuildEffectiveConnectionString(string? connectionString, DataVoStorageMode? storageMode, string? dataSource)
    {
        if (storageMode is not null && dataSource is { Length: > 0 })
        {
            string modeToken = storageMode == DataVoStorageMode.InMemory ? "InMemory" : "Lsm";
            return $"StorageMode={modeToken};DataSource={dataSource}";
        }

        return connectionString;
    }

    /// <summary>Returns the provider mode these options select.</summary>
    public DataVoProviderModeStatus GetModeStatus()
        => new(NativeQueryTranslationPreviewEnabled ? DataVoProviderMode.NativeTranslationPreview : DataVoProviderMode.BridgeOnly);

    // ------------------------------------------------------------------ extension info

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        private new DataVoOptionsExtension Extension => (DataVoOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => false;

        public override string LogFragment
        {
            get
            {
                string nativeTranslationFlag = Extension.NativeQueryTranslationPreviewEnabled ? " nativeTranslationPreview=on" : string.Empty;
                string cs = Extension.BuildEffectiveConnectionString() ?? "(none)";
                return $"using DataVo({cs}){nativeTranslationFlag}";
            }
        }

        public override int GetServiceProviderHashCode()
        {
            // Reuse EF internal service providers across different *values* (for example,
            // different DataVo database names), but keep distinct option *shapes* separate so
            // validation still runs for materially different configurations.
            return HashCode.Combine(
                Extension.StorageMode is not null,
                string.IsNullOrWhiteSpace(Extension.DataSource),
                string.IsNullOrWhiteSpace(Extension.ConnectionString),
                Extension.NativeQueryTranslationPreviewEnabled);
        }

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["DataVo:ConnectionString"] = Extension.BuildEffectiveConnectionString() ?? string.Empty;
            debugInfo["DataVo:StorageMode"] = Extension.StorageMode?.ToString() ?? "unset";
            debugInfo["DataVo:DataSource"] = Extension.DataSource ?? string.Empty;
            debugInfo["DataVo:NativeQueryTranslationPreview"] = Extension.NativeQueryTranslationPreviewEnabled ? "1" : "0";
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
        {
            return other is ExtensionInfo otherInfo &&
                   (Extension.StorageMode is not null) == (otherInfo.Extension.StorageMode is not null) &&
                   string.IsNullOrWhiteSpace(Extension.DataSource) == string.IsNullOrWhiteSpace(otherInfo.Extension.DataSource) &&
                   string.IsNullOrWhiteSpace(Extension.ConnectionString) == string.IsNullOrWhiteSpace(otherInfo.Extension.ConnectionString) &&
                   Extension.NativeQueryTranslationPreviewEnabled == otherInfo.Extension.NativeQueryTranslationPreviewEnabled;
        }
    }
}
