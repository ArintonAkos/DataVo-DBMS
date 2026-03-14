using DataVo.Data;
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// EF Core <see cref="IDatabaseCreator"/> implementation for DataVo.
///
/// Registered by <see cref="DataVoServiceCollectionExtensions.AddEntityFrameworkDataVo"/> so that
/// the standard EF APIs <c>Database.EnsureCreated()</c> and <c>Database.EnsureDeleted()</c> route
/// directly to DataVo.
/// </summary>
internal sealed class DataVoDatabaseCreator : IDatabaseCreator
{
    private readonly ICurrentDbContext _currentDbContext;
    private readonly IDbContextOptions _options;

    public DataVoDatabaseCreator(
        ICurrentDbContext currentDbContext,
        IDbContextOptions options)
    {
        _currentDbContext = currentDbContext;
        _options = options;
    }

    // ------------------------------------------------------------------ EnsureCreated

    /// <inheritdoc />
    /// <remarks>
    /// Generates DataVo <c>CREATE TABLE IF NOT EXISTS</c> statements from the EF model and
    /// executes them against the configured DataVo storage.
    ///
    /// Returns <c>true</c> when storage appears newly created, <c>false</c> when it already
    /// existed (disk mode only). In-memory mode always returns <c>true</c>.
    /// </remarks>
    public bool EnsureCreated()
    {
        string connectionString = ResolveConnectionString();

        bool existedBefore = StorageExists(connectionString);
        _currentDbContext.Context.EnsureDataVoCreated(connectionString);

        var builder = new DataVoConnectionStringBuilder(connectionString);
        if (builder.StorageMode != Core.StorageEngine.Config.StorageMode.Disk)
        {
            return true;
        }

        return !existedBefore;
    }

    /// <inheritdoc />
    public Task<bool> EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        bool result = EnsureCreated();
        return Task.FromResult(result);
    }

    // ------------------------------------------------------------------ EnsureDeleted

    /// <inheritdoc />
    /// <remarks>
    /// Deletes the DataVo database directory.  Returns <c>true</c> if it existed and was deleted,
    /// <c>false</c> if it was already absent.
    /// </remarks>
    public bool EnsureDeleted()
    {
        string connectionString = ResolveConnectionString();
        return DeleteStorage(connectionString);
    }

    /// <inheritdoc />
    public Task<bool> EnsureDeletedAsync(CancellationToken cancellationToken = default)
    {
        bool result = EnsureDeleted();
        return Task.FromResult(result);
    }

    // ------------------------------------------------------------------ CanConnect

    /// <inheritdoc />
    public bool CanConnect()
    {
        try
        {
            string connectionString = ResolveConnectionString();
            using var connection = new DataVoConnection(connectionString);
            connection.Open();
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <inheritdoc />
    public Task<bool> CanConnectAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(CanConnect());
    }

    // ------------------------------------------------------------------ helpers

    private string ResolveConnectionString()
    {
        var extension = _options.FindExtension<DataVoOptionsExtension>();
        string? connectionString = extension?.BuildEffectiveConnectionString();

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "No DataVo connection string is configured. " +
                "Call UseDataVo(connectionString) or the typed fluent API " +
                "(e.g. UseDataVo(o => o.UseStorageMode(DataVoStorageMode.Disk).WithDataSource(\"mydb\"))).");
        }

        return connectionString;
    }

    private static bool DeleteStorage(string connectionString)
    {
        // Parse DataSource from the connection string to locate the storage directory.
        var builder = new DataVoConnectionStringBuilder(connectionString);

        if (builder.StorageMode != Core.StorageEngine.Config.StorageMode.Disk)
        {
            return true;
        }

        string? dataSource = builder.DataSource;

        if (string.IsNullOrWhiteSpace(dataSource))
        {
            return false;
        }

        // DataVo stores disk databases in a directory named after the DataSource relative to cwd.
        string directoryPath = Path.IsPathRooted(dataSource)
            ? dataSource
            : Path.Combine(Directory.GetCurrentDirectory(), dataSource);

        if (!Directory.Exists(directoryPath))
        {
            return false;
        }

        Directory.Delete(directoryPath, recursive: true);
        return true;
    }

    private static bool StorageExists(string connectionString)
    {
        var builder = new DataVoConnectionStringBuilder(connectionString);
        if (builder.StorageMode != Core.StorageEngine.Config.StorageMode.Disk)
        {
            return false;
        }

        string dataSource = builder.DataSource;
        string directoryPath = Path.IsPathRooted(dataSource)
            ? dataSource
            : Path.Combine(Directory.GetCurrentDirectory(), dataSource);

        return Directory.Exists(directoryPath);
    }
}
