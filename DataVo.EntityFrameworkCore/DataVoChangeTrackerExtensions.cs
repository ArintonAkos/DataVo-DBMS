using DataVo.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// EF Core change-tracker bridge for applying tracked CRUD operations to DataVo.
/// </summary>
public static class DataVoChangeTrackerExtensions
{
    /// <summary>
    /// Applies tracked CRUD operations to DataVo using the connection string configured via <c>UseDataVo(...)</c>.
    /// Schema is created first (idempotent <c>IF NOT EXISTS</c>).
    /// </summary>
    public static int SaveChangesToDataVo(this DbContext context, bool acceptAllChangesOnSuccess = true)
    {
        ArgumentNullException.ThrowIfNull(context);

        string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(context);
        return SaveChangesToDataVo(context, connectionString, acceptAllChangesOnSuccess);
    }

    /// <summary>
    /// Applies tracked Added/Modified/Deleted entities to DataVo in one transaction using parameterized
    /// SQL commands. Schema is created first (idempotent <c>IF NOT EXISTS</c>).
    /// </summary>
    /// <remarks>
    /// When <c>UseDataVo</c> is configured and <c>SaveChanges()</c> is called on the context the
    /// built-in <see cref="Infrastructure.Internal.DataVoSaveChangesInterceptor"/> handles execution
    /// automatically — there is no need to call this method explicitly in that case.
    /// </remarks>
    public static int SaveChangesToDataVo(this DbContext context, string connectionString, bool acceptAllChangesOnSuccess = true)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        context.EnsureDataVoCreated(connectionString);

        int affected = ExecuteDataVoCrud(context, connectionString);

        if (acceptAllChangesOnSuccess)
        {
            context.ChangeTracker.AcceptAllChanges();
        }

        return affected;
    }

    /// <summary>
    /// Executes INSERT / UPDATE / DELETE commands against DataVo for all pending change-tracker
    /// entries in a single DataVo transaction, without touching schema or calling
    /// <c>AcceptAllChanges</c>. If any statement fails, the whole batch is rolled back.
    /// Called by both <see cref="SaveChangesToDataVo(DbContext, string, bool)"/> and the
    /// automatic <see cref="Infrastructure.Internal.DataVoSaveChangesInterceptor"/>.
    /// </summary>
    /// <exception cref="DataVoEfException">
    /// Thrown with the <see cref="DataVoEfOperation"/> that failed when DataVo rejects a statement
    /// (e.g. duplicate key, FK violation, type mismatch) or the connection or commit fails.
    /// </exception>
    internal static int ExecuteDataVoCrud(DbContext context, string connectionString)
    {
        var entries = GetOrderedEntries(context);
        if (entries.Count == 0)
        {
            return 0;
        }

        DataVoEfOperation operation = DataVoEfOperation.Connection;
        try
        {
            using var connection = DataVoBridgeStorage.Open(connectionString);
            using var transaction = connection.Connection.BeginTransaction();

            int affectedRows = 0;
            foreach (EntityEntry entry in entries)
            {
                operation = GetOperation(entry.State);
                DataVoSqlStatement? statement = BuildStatement(entry);
                if (statement is not null)
                {
                    affectedRows += ExecuteEntryStatement(entry, operation, statement, connection);
                }
            }

            transaction.Commit();
            return affectedRows;
        }
        catch (DataVoEfException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DataVoEfException(
                operation,
                $"DataVo {operation} failed while saving changes; no changes were committed. Connection: {connectionString}. See inner exception.",
                ex);
        }
    }

    private static int ExecuteEntryStatement(
        EntityEntry entry,
        DataVoEfOperation operation,
        DataVoSqlStatement statement,
        DataVoBridgeConnection connection)
    {
        int affected;
        try
        {
            affected = connection.ExecuteNonQuery(statement);
        }
        catch (Exception ex)
        {
            string tableName = entry.Metadata.GetTableName() ?? entry.Metadata.DisplayName();
            throw new DataVoEfException(
                operation,
                $"DataVo {operation} failed on table '{tableName}' for entity '{entry.Metadata.DisplayName()}'; no changes were committed. " +
                $"SQL: {statement.Sql} — See inner exception for details.",
                ex);
        }

        if (operation == DataVoEfOperation.Insert && affected == 0)
        {
            throw new DataVoEfException(
                DataVoEfOperation.Insert,
                $"DataVo insert appears to have failed (rows affected = 0); no changes were committed. SQL: {statement.Sql}");
        }

        return affected;
    }

    private static DataVoEfOperation GetOperation(EntityState state) => state switch
    {
        EntityState.Added => DataVoEfOperation.Insert,
        EntityState.Modified => DataVoEfOperation.Update,
        EntityState.Deleted => DataVoEfOperation.Delete,
        _ => DataVoEfOperation.RawSql,
    };

    private static List<EntityEntry> GetOrderedEntries(DbContext context)
    {
        var trackableEntries = context.ChangeTracker
            .Entries()
            .Where(static entry =>
                entry.State is EntityState.Added or EntityState.Modified or EntityState.Deleted &&
                entry.Metadata.GetTableName() is not null)
            .ToList();

        var tableOrder = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (IEntityType entityType in DataVoModelOrdering.OrderPrincipalsFirst(context.Model.GetEntityTypes()))
        {
            tableOrder.TryAdd(DataVoModelOrdering.GetTableKey(entityType), tableOrder.Count);
        }

        int GetTableOrder(EntityEntry entry) => tableOrder[DataVoModelOrdering.GetTableKey(entry.Metadata)];

        var added = trackableEntries
            .Where(static entry => entry.State == EntityState.Added)
            .OrderBy(GetTableOrder);

        var modified = trackableEntries
            .Where(static entry => entry.State == EntityState.Modified);

        var deleted = trackableEntries
            .Where(static entry => entry.State == EntityState.Deleted)
            .OrderByDescending(GetTableOrder);

        return [.. added, .. modified, .. deleted];
    }

    private static DataVoSqlStatement? BuildStatement(EntityEntry entry)
    {
        string tableName = entry.Metadata.GetTableName()!;
        var table = StoreObjectIdentifier.Table(tableName, entry.Metadata.GetSchema());
        var parameters = new DataVoSqlParameterList();

        string? sql = entry.State switch
        {
            EntityState.Added => BuildInsertSql(entry, tableName, table, parameters),
            EntityState.Modified => BuildUpdateSql(entry, tableName, table, parameters),
            EntityState.Deleted => BuildDeleteSql(entry, tableName, table, parameters),
            _ => null
        };

        return sql is null ? null : new DataVoSqlStatement(sql, parameters.Parameters);
    }

    private static string BuildInsertSql(EntityEntry entry, string tableName, StoreObjectIdentifier table, DataVoSqlParameterList parameters)
    {
        var columns = new List<string>();
        var values = new List<string>();

        foreach (PropertyEntry propertyEntry in entry.Properties)
        {
            if (propertyEntry.Metadata.GetColumnName(table) is not { } columnName)
            {
                continue;
            }

            columns.Add(columnName);
            values.Add(parameters.Add(propertyEntry.CurrentValue));
        }

        return $"INSERT INTO {tableName} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)});";
    }

    private static string? BuildUpdateSql(EntityEntry entry, string tableName, StoreObjectIdentifier table, DataVoSqlParameterList parameters)
    {
        var assignments = new List<string>();

        foreach (PropertyEntry propertyEntry in entry.Properties)
        {
            if (propertyEntry.IsModified &&
                !propertyEntry.Metadata.IsPrimaryKey() &&
                propertyEntry.Metadata.GetColumnName(table) is { } columnName)
            {
                assignments.Add($"{columnName} = {parameters.Add(propertyEntry.CurrentValue)}");
            }
        }

        if (assignments.Count == 0)
        {
            return null;
        }

        string whereClause = BuildPrimaryKeyWhereClause(entry, table, parameters);
        return $"UPDATE {tableName} SET {string.Join(", ", assignments)} WHERE {whereClause};";
    }

    private static string BuildDeleteSql(EntityEntry entry, string tableName, StoreObjectIdentifier table, DataVoSqlParameterList parameters)
    {
        string whereClause = BuildPrimaryKeyWhereClause(entry, table, parameters);
        return $"DELETE FROM {tableName} WHERE {whereClause};";
    }

    private static string BuildPrimaryKeyWhereClause(EntityEntry entry, StoreObjectIdentifier table, DataVoSqlParameterList parameters)
    {
        IKey primaryKey = entry.Metadata.FindPrimaryKey()
            ?? throw new NotSupportedException($"Entity '{entry.Metadata.DisplayName()}' must declare a primary key for DataVo CRUD bridge operations.");

        var predicates = primaryKey.Properties.Select(property =>
        {
            string columnName = property.GetColumnName(table) ?? property.Name;
            object? value = entry.Property(property.Name).OriginalValue;

            return value is null
                ? $"{columnName} IS NULL"
                : $"{columnName} = {parameters.Add(value)}";
        });

        return string.Join(" AND ", predicates);
    }
}
