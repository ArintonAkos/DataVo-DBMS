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
    /// executes them against the configured DataVo storage. Returns <c>true</c> when at least one
    /// table was created, <c>false</c> when the whole schema already existed.
    /// </remarks>
    public bool EnsureCreated()
    {
        return DataVoDbContextExtensions.EnsureSchema(_currentDbContext.Context, ResolveConnectionString());
    }

    /// <inheritdoc />
    public Task<bool> EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        bool result = EnsureCreated();
        return Task.FromResult(result);
    }

    // ------------------------------------------------------------------ EnsureDeleted

    /// <inheritdoc />
    /// <remarks>
    /// Deletes the DataVo storage: the Lsm data directory or the shared in-memory database.
    /// Returns <c>true</c> if it existed and was deleted, <c>false</c> if it was already absent.
    /// </remarks>
    public bool EnsureDeleted()
    {
        return DataVoBridgeStorage.EnsureDeleted(ResolveConnectionString());
    }

    /// <inheritdoc />
    public Task<bool> EnsureDeletedAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        bool result = EnsureDeleted();
        return Task.FromResult(result);
    }

    // ------------------------------------------------------------------ CanConnect

    /// <inheritdoc />
    public bool CanConnect()
    {
        try
        {
            using var connection = DataVoBridgeStorage.Open(ResolveConnectionString());
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
                "(e.g. UseDataVo(o => o.UseStorageMode(DataVoStorageMode.Lsm).WithDataSource(\"mydb\"))).");
        }

        return connectionString;
    }
}
