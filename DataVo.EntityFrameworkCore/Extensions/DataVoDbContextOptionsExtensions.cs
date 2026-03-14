using DataVo.EntityFrameworkCore.Infrastructure;
using DataVo.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Extension methods for configuring DataVo with EF Core.
/// </summary>
public static class DataVoDbContextOptionsExtensions
{
    private static readonly DataVoSaveChangesInterceptor SharedSaveChangesInterceptor = new();

    // ------------------------------------------------------------------ snapshot accessor

    /// <summary>
    /// Returns the DataVo bootstrap options attached to the EF Core options, if configured.
    /// </summary>
    public static DataVoBootstrapOptions? GetDataVoBootstrapOptions(this DbContextOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var extension = options.FindExtension<DataVoOptionsExtension>();
        if (extension is null)
        {
            return null;
        }

        return new DataVoBootstrapOptions(
            extension.ConnectionString,
            extension.StorageMode,
            extension.DataSource,
            extension.BootstrapDiagnosticsEnabled);
    }

    // ------------------------------------------------------------------ UseDataVo – connection-string overloads

    /// <summary>
    /// Configures the context to use DataVo for persistence.
    /// </summary>
    /// <param name="optionsBuilder">The builder being configured.</param>
    /// <param name="connectionString">
    /// A DataVo connection string, e.g. <c>"StorageMode=Disk;DataSource=mydb"</c>.
    /// </param>
    /// <param name="dataVoOptionsAction">
    /// Optional action to configure additional DataVo-specific options such as
    /// <see cref="DataVoDbContextOptionsBuilder.UseInMemoryStorage"/> or
    /// <see cref="DataVoDbContextOptionsBuilder.WithDataSource"/>.
    /// </param>
    public static DbContextOptionsBuilder UseDataVo(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<DataVoDbContextOptionsBuilder>? dataVoOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        return ApplyDataVoOptions(optionsBuilder, connectionString, dataVoOptionsAction);
    }

    /// <summary>
    /// Configures the context to use DataVo for persistence using typed fluent options only.
    /// Use this overload to specify <see cref="DataVoStorageMode"/> and a data-source name
    /// without writing a raw connection string.
    /// <code>
    /// UseDataVo(o => o.UseDiskStorage().WithDataSource("mydb"))
    /// UseDataVo(o => o.UseInMemoryStorage().WithDataSource("testdb"))
    /// </code>
    /// </summary>
    public static DbContextOptionsBuilder UseDataVo(
        this DbContextOptionsBuilder optionsBuilder,
        Action<DataVoDbContextOptionsBuilder> dataVoOptionsAction)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(dataVoOptionsAction);

        return ApplyDataVoOptions(optionsBuilder, connectionString: null, dataVoOptionsAction);
    }

    // ------------------------------------------------------------------ UseDataVo – typed context overloads

    /// <inheritdoc cref="UseDataVo(DbContextOptionsBuilder, string, Action{DataVoDbContextOptionsBuilder}?)"/>
    public static DbContextOptionsBuilder<TContext> UseDataVo<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string connectionString,
        Action<DataVoDbContextOptionsBuilder>? dataVoOptionsAction = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        UseDataVo((DbContextOptionsBuilder)optionsBuilder, connectionString, dataVoOptionsAction);
        return optionsBuilder;
    }

    /// <inheritdoc cref="UseDataVo(DbContextOptionsBuilder, Action{DataVoDbContextOptionsBuilder})"/>
    public static DbContextOptionsBuilder<TContext> UseDataVo<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Action<DataVoDbContextOptionsBuilder> dataVoOptionsAction)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        UseDataVo((DbContextOptionsBuilder)optionsBuilder, dataVoOptionsAction);
        return optionsBuilder;
    }

    // ------------------------------------------------------------------ shared core

    private static DbContextOptionsBuilder ApplyDataVoOptions(
        DbContextOptionsBuilder optionsBuilder,
        string? connectionString,
        Action<DataVoDbContextOptionsBuilder>? dataVoOptionsAction)
    {
        var extension = optionsBuilder.Options.FindExtension<DataVoOptionsExtension>()
            ?? new DataVoOptionsExtension();

        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            extension.WithConnectionString(connectionString);
        }

        var dataVoOptionsBuilder = new DataVoDbContextOptionsBuilder(extension);
        dataVoOptionsAction?.Invoke(dataVoOptionsBuilder);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        // Register a shared interceptor instance so repeated context construction does not
        // force EF to build excessive internal service providers.
        optionsBuilder.AddInterceptors(SharedSaveChangesInterceptor);

        return optionsBuilder;
    }
}

