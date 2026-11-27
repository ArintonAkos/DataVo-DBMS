using DataVo.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace DataVo.EntityFrameworkCore.Infrastructure;

/// <summary>
/// Builder for DataVo-specific EF Core options used by <c>UseDataVo</c>.
/// </summary>
public sealed class DataVoDbContextOptionsBuilder
{
    private readonly DbContextOptionsBuilder _optionsBuilder;

    internal DataVoDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        _optionsBuilder = optionsBuilder;
    }

    // ------------------------------------------------------------------ storage mode

    /// <summary>
    /// Configures DataVo to persist data with the LSM storage engine.
    /// Equivalent to <c>StorageMode=Lsm</c> in the connection string.
    /// </summary>
    public DataVoDbContextOptionsBuilder UseLsmStorage()
        => UseStorageMode(DataVoStorageMode.Lsm);

    /// <summary>
    /// Configures DataVo to use in-memory (non-persistent) storage.
    /// Equivalent to <c>StorageMode=InMemory</c> in the connection string. Every context configured
    /// with the same data source shares one in-memory database for the life of the process, until
    /// <c>Database.EnsureDeleted()</c> drops it.
    /// </summary>
    public DataVoDbContextOptionsBuilder UseInMemoryStorage()
        => UseStorageMode(DataVoStorageMode.InMemory);

    /// <summary>
    /// Configures which storage mode DataVo should use.
    /// </summary>
    public DataVoDbContextOptionsBuilder UseStorageMode(DataVoStorageMode mode)
        => WithOption(extension => extension.WithStorageMode(mode));

    // ------------------------------------------------------------------ data source

    /// <summary>
    /// Sets the database name / data-source used by DataVo.
    /// When combined with <see cref="UseStorageMode"/> this fully replaces the need to
    /// write a manual connection string.
    /// </summary>
    public DataVoDbContextOptionsBuilder WithDataSource(string dataSource)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataSource);
        return WithOption(extension => extension.WithDataSource(dataSource));
    }

    // ------------------------------------------------------------------ query translation

    /// <summary>
    /// Enables native query translation preview: supported no-tracking LINQ shapes passed to
    /// <c>QueryFromDataVo</c> and its siblings run as DataVo SQL instead of refreshing the EF mirror.
    /// </summary>
    public DataVoDbContextOptionsBuilder EnableNativeQueryTranslationPreview(bool enabled = true)
        => WithOption(extension => extension.WithNativeQueryTranslationPreview(enabled));

    // Options extensions are immutable: each change registers a modified copy.
    private DataVoDbContextOptionsBuilder WithOption(Func<DataVoOptionsExtension, DataVoOptionsExtension> change)
    {
        var current = _optionsBuilder.Options.FindExtension<DataVoOptionsExtension>() ?? new DataVoOptionsExtension();
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(change(current));
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
    bool NativeQueryTranslationPreviewEnabled)
{
    /// <summary>Returns the effective connection string used at runtime.</summary>
    public string? EffectiveConnectionString =>
        DataVoOptionsExtension.BuildEffectiveConnectionString(ConnectionString, StorageMode, DataSource);

    /// <summary>Whether the bridge is still operating in bridge-only mode.</summary>
    public bool IsBridgeOnlyMode => !NativeQueryTranslationPreviewEnabled;
}
