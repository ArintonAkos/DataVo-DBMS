using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

internal sealed class DataVoOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    /// <summary>Full ADO.NET connection string as supplied to <c>UseDataVo</c>.</summary>
    public string? ConnectionString { get; private set; }

    /// <summary>
    /// Typed storage mode override.
    /// When set, <c>BuildEffectiveConnectionString</c> synthesises the connection string from
    /// <see cref="StorageMode"/> and <see cref="DataSource"/> instead of using the raw string.
    /// </summary>
    public DataVoStorageMode? StorageMode { get; private set; }

    /// <summary>
    /// Typed data-source / database name override (e.g. "mydb").
    /// Used together with <see cref="StorageMode"/> to build the effective connection string.
    /// </summary>
    public string? DataSource { get; private set; }

    public bool BootstrapDiagnosticsEnabled { get; private set; }

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
                "Provide a DataSource name (e.g. UseDataVo(o => o.UseDiskStorage().WithDataSource(\"mydb\"))).");
        }

        // DataVo is currently an EF bridge extension (not a standalone EF provider),
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

    // ------------------------------------------------------------------ fluent mutation API

    public DataVoOptionsExtension WithConnectionString(string connectionString)
    {
        ConnectionString = connectionString;
        return this;
    }

    public DataVoOptionsExtension WithStorageMode(DataVoStorageMode mode)
    {
        StorageMode = mode;
        return this;
    }

    public DataVoOptionsExtension WithDataSource(string dataSource)
    {
        DataSource = dataSource;
        return this;
    }

    public DataVoOptionsExtension WithBootstrapDiagnostics(bool enabled)
    {
        BootstrapDiagnosticsEnabled = enabled;
        return this;
    }

    // ------------------------------------------------------------------ effective connection string

    /// <summary>
    /// Returns the connection string to use at runtime.
    /// If <see cref="StorageMode"/> and <see cref="DataSource"/> are both set they take priority
    /// (letting callers configure storage purely through the typed fluent API without writing a
    /// raw connection string).  Otherwise falls back to <see cref="ConnectionString"/>.
    /// </summary>
    public string? BuildEffectiveConnectionString()
    {
        if (StorageMode is not null && DataSource is { Length: > 0 })
        {
            string modeToken = StorageMode == DataVoStorageMode.InMemory ? "InMemory" : "Disk";
            return $"StorageMode={modeToken};DataSource={DataSource}";
        }

        return ConnectionString;
    }

    // ------------------------------------------------------------------ extension info

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        private new DataVoOptionsExtension Extension => (DataVoOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => false;

        public override string LogFragment
        {
            get
            {
                string diagnosticsFlag = Extension.BootstrapDiagnosticsEnabled ? " diagnostics=on" : string.Empty;
                string cs = Extension.BuildEffectiveConnectionString() ?? "(none)";
                return $"using DataVo({cs}){diagnosticsFlag}";
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
                string.IsNullOrWhiteSpace(Extension.ConnectionString));
        }

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["DataVo:ConnectionString"] = Extension.BuildEffectiveConnectionString() ?? string.Empty;
            debugInfo["DataVo:StorageMode"] = Extension.StorageMode?.ToString() ?? "unset";
            debugInfo["DataVo:DataSource"] = Extension.DataSource ?? string.Empty;
            debugInfo["DataVo:Diagnostics"] = Extension.BootstrapDiagnosticsEnabled ? "1" : "0";
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
        {
            return other is ExtensionInfo otherInfo &&
                   (Extension.StorageMode is not null) == (otherInfo.Extension.StorageMode is not null) &&
                   string.IsNullOrWhiteSpace(Extension.DataSource) == string.IsNullOrWhiteSpace(otherInfo.Extension.DataSource) &&
                   string.IsNullOrWhiteSpace(Extension.ConnectionString) == string.IsNullOrWhiteSpace(otherInfo.Extension.ConnectionString);
        }
    }
}
