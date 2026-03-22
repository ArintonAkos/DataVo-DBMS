using DataVo.Data;
using DataVo.Core.StorageEngine.Config;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using System.Data;
using System.Linq.Expressions;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Experimental DatabaseFacade helpers for DataVo bridge operations.
/// </summary>
public static class DataVoDatabaseFacadeExtensions
{
    /// <summary>
    /// Ensures DataVo schema is created for the current context using <c>UseDataVo(...)</c> options.
    /// </summary>
    public static bool EnsureDataVoCreated(this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetDbContext();
        context.EnsureDataVoCreated();
        return true;
    }

    /// <summary>
    /// Ensures DataVo schema is created for the current context using an explicit connection string.
    /// </summary>
    public static bool EnsureDataVoCreated(this DatabaseFacade database, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var context = database.GetDbContext();
        context.EnsureDataVoCreated(connectionString);
        return true;
    }

    /// <summary>
    /// Deletes DataVo disk storage directory for the configured connection string.
    /// </summary>
    public static bool EnsureDataVoDeleted(this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetDbContext();
        string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(context);
        return EnsureDeletedByConnectionString(connectionString);
    }

    /// <summary>
    /// Deletes DataVo disk storage directory for an explicit connection string.
    /// </summary>
    public static bool EnsureDataVoDeleted(this DatabaseFacade database, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        return EnsureDeletedByConnectionString(connectionString);
    }

    private static bool EnsureDeletedByConnectionString(string connectionString)
    {
        var builder = new DataVoConnectionStringBuilder(connectionString);

        if (builder.StorageMode != StorageMode.Disk)
        {
            return true;
        }

        if (!Directory.Exists(builder.DataSource))
        {
            return false;
        }

        Directory.Delete(builder.DataSource, recursive: true);
        return true;
    }

    private static DbContext GetDbContext(this DatabaseFacade database)
    {
        var currentDbContext = database.GetService<ICurrentDbContext>();
        return currentDbContext.Context;
    }

    // ------------------------------------------------------------------ raw SQL

    /// <summary>
    /// Executes a raw SQL statement directly against DataVo, bypassing EF's InMemory pipeline.
    /// </summary>
    /// <returns>Number of rows affected.</returns>
    /// <exception cref="DataVoEfException">Thrown when the SQL execution fails.</exception>
    public static int ExecuteDataVoSqlRaw(this DatabaseFacade database, string sql)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        var context = database.GetDbContext();
        string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(context);

        try
        {
            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = sql;
            int affected = command.ExecuteNonQuery();

            if (sql.TrimStart().StartsWith("INSERT", StringComparison.OrdinalIgnoreCase) && affected == 0)
            {
                throw new DataVoEfException(
                    DataVoEfOperation.RawSql,
                    $"DataVo INSERT affected 0 rows. SQL may be invalid or rejected: {sql}");
            }

            return affected;
        }
        catch (Exception ex) when (ex is not DataVoEfException)
        {
            throw new DataVoEfException(
                DataVoEfOperation.RawSql,
                $"DataVo raw SQL execution failed. SQL: {sql}",
                ex);
        }
    }

    // ------------------------------------------------------------------ data load

    /// <summary>
    /// Reads all DataVo tables and attaches the rows into the EF change tracker so
    /// LINQ queries return live DataVo data.
    ///
    /// <para>Delegates to <see cref="DataVoDbContext.LoadFromDataVo"/> when the context
    /// is a <see cref="DataVoDbContext"/>; otherwise invokes the materializer directly.
    /// </para>
    /// </summary>
    /// <exception cref="DataVoEfException">Thrown when the load fails.</exception>
    public static void LoadFromDataVo(this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetDbContext();

        if (context is DataVoDbContext dvc)
        {
            dvc.LoadFromDataVo();
        }
        else
        {
            string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(context);
            Infrastructure.Internal.DataVoEntityMaterializer.LoadIntoContext(context, connectionString);
        }
    }

    // ------------------------------------------------------------------ capability profile

    /// <summary>
    /// Returns the guarded-query capability profile for the current bridge layer.
    /// </summary>
    /// <remarks>
    /// This is a static capability declaration — it does not require <paramref name="database"/>
    /// to be connected.
    /// </remarks>
    public static DataVoGuardedQueryCapabilities GetDataVoGuardedQueryCapabilities(
        this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);
        return DataVoDbContext.GetGuardedQueryCapabilities();
    }

    /// <summary>
    /// Returns the current DataVo provider preview status for the underlying context options.
    /// </summary>
    public static DataVoProviderPreviewStatus? GetDataVoProviderPreviewStatus(
        this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetDbContext();
        return context.GetService<IDbContextOptions>().GetDataVoProviderPreviewStatus();
    }

    /// <summary>
    /// Returns the active DataVo provider mode status for the underlying context.
    /// </summary>
    public static DataVoProviderModeStatus? GetDataVoProviderModeStatus(
        this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetDbContext();
        return context.GetService<IDbContextOptions>().GetDataVoProviderModeStatus();
    }

    /// <summary>
    /// Returns structured translation diagnostics for the provided query shape.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the underlying context is not a <see cref="DataVoDbContext"/>.
    /// </exception>
    public static DataVoQueryTranslationDiagnostics ExplainDataVoQuery<TEntity>(
        this DatabaseFacade database,
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetDbContext();
        if (context is not DataVoDbContext dataVoContext)
        {
            throw new InvalidOperationException(
                $"ExplainDataVoQuery requires a DataVoDbContext-derived context. The current context is '{context.GetType().Name}'.");
        }

        return dataVoContext.ExplainQueryFromDataVo(queryShape, asNoTracking);
    }

    /// <summary>
    /// Returns structured translation diagnostics for projection query shape.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the underlying context is not a <see cref="DataVoDbContext"/>.
    /// </exception>
    public static DataVoQueryTranslationDiagnostics ExplainDataVoProjectionQuery<TEntity, TDto>(
        this DatabaseFacade database,
        Expression<Func<TEntity, TDto>> selector,
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(selector);

        var context = database.GetDbContext();
        if (context is not DataVoDbContext dataVoContext)
        {
            throw new InvalidOperationException(
                $"ExplainDataVoProjectionQuery requires a DataVoDbContext-derived context. The current context is '{context.GetType().Name}'.");
        }

        return dataVoContext.ExplainProjectFromDataVo(selector, queryShape, asNoTracking);
    }

    /// <summary>
    /// Returns provider-identity parity checks for the current DataVo context.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the underlying context is not a <see cref="DataVoDbContext"/>.
    /// </exception>
    public static DataVoProviderIdentityParityReport GetDataVoProviderIdentityParityReport(
        this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetDbContext();
        if (context is not DataVoDbContext dataVoContext)
        {
            throw new InvalidOperationException(
                $"GetDataVoProviderIdentityParityReport requires a DataVoDbContext-derived context. The current context is '{context.GetType().Name}'.");
        }

        return dataVoContext.GetProviderIdentityParityReport();
    }

    /// <summary>
    /// Performs a non-executing preflight check: returns <c>true</c> when the provided LINQ
    /// shape is supported by the guarded query bridge, <c>false</c> otherwise.
    /// </summary>
    /// <typeparam name="TEntity">Entity type to test the shape against.</typeparam>
    /// <param name="database">The <see cref="DatabaseFacade"/> of a <see cref="DataVoDbContext"/>-derived context.</param>
    /// <param name="queryShape">LINQ composition function to validate.</param>
    /// <param name="reason">
    /// When this method returns <c>false</c>, contains a human-readable explanation of why the
    /// shape is not supported.  <c>null</c> when the method returns <c>true</c>.
    /// </param>
    /// <param name="asNoTracking">Same as the corresponding parameter on <see cref="DataVoDbContext.QueryFromDataVo{TEntity}"/>.</param>
    /// <returns><c>true</c> if the shape can be executed; <c>false</c> if it will be blocked.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the underlying context is not a <see cref="DataVoDbContext"/>.
    /// </exception>
    public static bool CanExecuteGuardedQuery<TEntity>(
        this DatabaseFacade database,
        Func<IQueryable<TEntity>, IQueryable<TEntity>> queryShape,
        out string? reason,
        bool asNoTracking = true)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(queryShape);

        var context = database.GetDbContext();

        if (context is not DataVoDbContext dataVoContext)
        {
            throw new InvalidOperationException(
                $"CanExecuteGuardedQuery requires a DataVoDbContext-derived context. " +
                $"The current context is '{context.GetType().Name}'.");
        }

        return dataVoContext.CanExecuteGuardedQuery(queryShape, out reason, asNoTracking);
    }

    // ------------------------------------------------------------------ CanConnect

    /// <summary>
    /// Returns <c>true</c> when a DataVo connection can be successfully opened.
    /// </summary>
    public static bool DataVoCanConnect(this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetDbContext();
        var options = context.GetService<IDbContextOptions>();
        var extension = options.FindExtension<Infrastructure.Internal.DataVoOptionsExtension>();
        string? connectionString = extension?.BuildEffectiveConnectionString();

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return false;
        }

        try
        {
            using var connection = new DataVoConnection(connectionString);
            connection.Open();
            return true;
        }
        catch
        {
            return false;
        }
    }
}
