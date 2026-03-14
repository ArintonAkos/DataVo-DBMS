using DataVo.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Globalization;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Experimental EF Core change-tracker bridge for applying tracked CRUD operations to DataVo.
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
    /// Applies tracked Added/Modified/Deleted entities to DataVo using generated SQL commands.
    /// Schema is created first (idempotent <c>IF NOT EXISTS</c>).
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
    /// Executes INSERT / UPDATE / DELETE SQL against DataVo for all pending change-tracker
    /// entries without touching schema or calling <c>AcceptAllChanges</c>.
    /// Called by both <see cref="SaveChangesToDataVo(DbContext, string, bool)"/> and the
    /// automatic <see cref="Infrastructure.Internal.DataVoSaveChangesInterceptor"/>.
    /// </summary>
    /// <exception cref="DataVoEfException">
    /// Thrown with a specific <see cref="DataVoEfOperation"/> for each DML type when DataVo
    /// rejects the statement (e.g. duplicate key, FK violation, type mismatch).
    /// </exception>
    internal static int ExecuteDataVoCrud(DbContext context, string connectionString)
    {
        var entries = GetOrderedEntries(context);
        if (entries.Count == 0)
        {
            return 0;
        }

        try
        {
            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            int affectedRows = 0;

            foreach (EntityEntry entry in entries)
            {
                string? sql = BuildSql(entry);
                if (string.IsNullOrWhiteSpace(sql))
                {
                    continue;
                }

                try
                {
                    using var command = connection.CreateCommand();
                    command.CommandText = sql;
                    int statementAffected = command.ExecuteNonQuery();

                    if (entry.State == EntityState.Added && statementAffected == 0)
                    {
                        throw new DataVoEfException(
                            DataVoEfOperation.Insert,
                            $"DataVo insert appears to have failed (rows affected = 0). SQL: {sql}");
                    }

                    affectedRows += statementAffected;
                }
                catch (Exception ex) when (ex is not DataVoEfException)
                {
                    var operation = entry.State switch
                    {
                        EntityState.Added => DataVoEfOperation.Insert,
                        EntityState.Modified => DataVoEfOperation.Update,
                        EntityState.Deleted => DataVoEfOperation.Delete,
                        _ => DataVoEfOperation.RawSql
                    };

                    string tableName = entry.Metadata.GetTableName() ?? entry.Metadata.DisplayName();

                    throw new DataVoEfException(
                        operation,
                        $"DataVo {operation} failed on table '{tableName}' for entity '{entry.Metadata.DisplayName()}'. " +
                        $"SQL: {sql} — See inner exception for details.",
                        ex);
                }
            }

            return affectedRows;
        }
        catch (DataVoEfException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DataVoEfException(
                DataVoEfOperation.Insert,
                $"DataVo CRUD execution failed. Connection: {connectionString}. See inner exception.",
                ex);
        }
    }

    private static List<EntityEntry> GetOrderedEntries(DbContext context)
    {
        var trackableEntries = context.ChangeTracker
            .Entries()
            .Where(static entry =>
                entry.State is EntityState.Added or EntityState.Modified or EntityState.Deleted &&
                entry.Metadata.GetTableName() is not null)
            .ToList();

        var mappedEntityTypes = context.Model
            .GetEntityTypes()
            .Where(static entityType => entityType.GetTableName() is not null)
            .ToList();

        var orderedEntityTypes = OrderEntityTypesForCreation(mappedEntityTypes);
        var tableOrderLookup = orderedEntityTypes
            .Select((entityType, index) => new { TableName = entityType.GetTableName()!, index })
            .ToDictionary(static x => x.TableName, static x => x.index, StringComparer.Ordinal);

        var added = trackableEntries
            .Where(static entry => entry.State == EntityState.Added)
            .OrderBy(entry => tableOrderLookup.GetValueOrDefault(entry.Metadata.GetTableName() ?? string.Empty, int.MaxValue))
            .ToList();

        var modified = trackableEntries
            .Where(static entry => entry.State == EntityState.Modified)
            .ToList();

        var deleted = trackableEntries
            .Where(static entry => entry.State == EntityState.Deleted)
            .OrderByDescending(entry => tableOrderLookup.GetValueOrDefault(entry.Metadata.GetTableName() ?? string.Empty, int.MinValue))
            .ToList();

        return [.. added, .. modified, .. deleted];
    }

    private static string? BuildSql(EntityEntry entry)
    {
        string tableName = entry.Metadata.GetTableName()
            ?? throw new InvalidOperationException($"Entity '{entry.Metadata.DisplayName()}' is not mapped to a table.");

        var tableIdentifier = StoreObjectIdentifier.Table(tableName, entry.Metadata.GetSchema());
        return entry.State switch
        {
            EntityState.Added => BuildInsertSql(entry, tableName, tableIdentifier),
            EntityState.Modified => BuildUpdateSql(entry, tableName, tableIdentifier),
            EntityState.Deleted => BuildDeleteSql(entry, tableName, tableIdentifier),
            _ => null
        };
    }

    private static string BuildInsertSql(EntityEntry entry, string tableName, StoreObjectIdentifier tableIdentifier)
    {
        var mappedProperties = entry.Metadata
            .GetProperties()
            .Where(property => property.GetColumnName(tableIdentifier) is not null)
            .ToList();

        string[] columns = mappedProperties
            .Select(property => property.GetColumnName(tableIdentifier)!)
            .ToArray();

        string[] values = mappedProperties
            .Select(property => FormatSqlLiteral(entry.Property(property.Name).CurrentValue))
            .ToArray();

        return $"INSERT INTO {tableName} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)});";
    }

    private static string? BuildUpdateSql(EntityEntry entry, string tableName, StoreObjectIdentifier tableIdentifier)
    {
        var setAssignments = entry.Properties
            .Where(propertyEntry =>
                propertyEntry.IsModified &&
                !propertyEntry.Metadata.IsPrimaryKey() &&
                propertyEntry.Metadata.GetColumnName(tableIdentifier) is not null)
            .Select(propertyEntry =>
            {
                string columnName = propertyEntry.Metadata.GetColumnName(tableIdentifier)!;
                return $"{columnName} = {FormatSqlLiteral(propertyEntry.CurrentValue)}";
            })
            .ToArray();

        if (setAssignments.Length == 0)
        {
            return null;
        }

        string whereClause = BuildPrimaryKeyWhereClause(entry, tableIdentifier, useOriginalValues: true);
        return $"UPDATE {tableName} SET {string.Join(", ", setAssignments)} WHERE {whereClause};";
    }

    private static string BuildDeleteSql(EntityEntry entry, string tableName, StoreObjectIdentifier tableIdentifier)
    {
        string whereClause = BuildPrimaryKeyWhereClause(entry, tableIdentifier, useOriginalValues: true);
        return $"DELETE FROM {tableName} WHERE {whereClause};";
    }

    private static string BuildPrimaryKeyWhereClause(EntityEntry entry, StoreObjectIdentifier tableIdentifier, bool useOriginalValues)
    {
        IKey primaryKey = entry.Metadata.FindPrimaryKey()
            ?? throw new NotSupportedException($"Entity '{entry.Metadata.DisplayName()}' must declare a primary key for DataVo CRUD bridge operations.");

        string[] predicates = primaryKey.Properties
            .Select(property =>
            {
                string columnName = property.GetColumnName(tableIdentifier) ?? property.Name;
                object? value = useOriginalValues ? entry.Property(property.Name).OriginalValue : entry.Property(property.Name).CurrentValue;

                return value is null
                    ? $"{columnName} IS NULL"
                    : $"{columnName} = {FormatSqlLiteral(value)}";
            })
            .ToArray();

        return string.Join(" AND ", predicates);
    }

    private static string FormatSqlLiteral(object? value)
    {
        return value switch
        {
            null or DBNull => "NULL",
            bool booleanValue => booleanValue ? "true" : "false",
            string stringValue => $"'{stringValue.Replace("'", "''")}'",
            DateOnly dateOnlyValue => $"'{dateOnlyValue:yyyy-MM-dd}'",
            DateTime dateTimeValue => $"'{dateTimeValue:yyyy-MM-dd}'",
            DateTimeOffset dateTimeOffsetValue => $"'{dateTimeOffsetValue:yyyy-MM-dd}'",
            Guid guidValue => $"'{guidValue}'",
            Enum enumValue => Convert.ToInt32(enumValue).ToString(CultureInfo.InvariantCulture),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => $"'{value.ToString()?.Replace("'", "''")}'"
        };
    }

    private static IReadOnlyList<IEntityType> OrderEntityTypesForCreation(IReadOnlyList<IEntityType> entityTypes)
    {
        var tableLookup = entityTypes.ToDictionary(static entityType => entityType.GetTableName()!);
        var dependencies = entityTypes.ToDictionary(
            static entityType => entityType,
            static entityType => entityType
                .GetForeignKeys()
                .Select(static foreignKey => foreignKey.PrincipalEntityType)
                .Where(principalEntityType => principalEntityType.GetTableName() is not null)
                .Distinct()
                .ToHashSet());

        var ordered = new List<IEntityType>(entityTypes.Count);
        var remaining = new HashSet<IEntityType>(entityTypes);

        while (remaining.Count > 0)
        {
            var ready = remaining
                .Where(entityType => dependencies[entityType].All(dependency => !remaining.Contains(dependency) || !tableLookup.ContainsKey(dependency.GetTableName()!)))
                .OrderBy(static entityType => entityType.GetTableName(), StringComparer.Ordinal)
                .ToList();

            if (ready.Count == 0)
            {
                string cycleTables = string.Join(", ", remaining.Select(static entityType => entityType.GetTableName()));
                throw new NotSupportedException($"DataVo EF change bridge does not yet support cyclic table dependencies: {cycleTables}.");
            }

            foreach (IEntityType entityType in ready)
            {
                ordered.Add(entityType);
                remaining.Remove(entityType);
            }
        }

        return ordered;
    }
}
