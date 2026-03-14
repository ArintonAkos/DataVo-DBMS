using DataVo.Data;
using DataVo.Core.StorageEngine.Config;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using System.Data;

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
