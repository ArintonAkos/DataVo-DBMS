using DataVo.Data;
using DataVo.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Globalization;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Experimental EF Core helpers for generating DataVo schema from an EF model.
/// This is the first integration slice before a full EF Core provider is implemented.
/// </summary>
public static class DataVoDbContextExtensions
{
    /// <summary>
    /// Generates DataVo <c>CREATE TABLE</c> statements from the current EF Core model.
    /// </summary>
    /// <param name="context">The EF Core context whose model is inspected.</param>
    /// <returns>An ordered list of DataVo DDL statements.</returns>
    public static IReadOnlyList<string> GenerateDataVoCreateStatements(this DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var entityTypes = context.Model
            .GetEntityTypes()
            .Where(static entityType =>
                !entityType.IsOwned() &&
                entityType.FindPrimaryKey() is not null &&
                entityType.GetTableName() is not null)
            .ToList();

        var duplicateTables = entityTypes
            .GroupBy(static entityType => new { Name = entityType.GetTableName(), Schema = entityType.GetSchema() })
            .Where(static group => group.Count() > 1)
            .ToList();

        if (duplicateTables.Count > 0)
        {
            string duplicateList = string.Join(", ", duplicateTables.Select(static group => group.Key.Schema is null ? group.Key.Name : $"{group.Key.Schema}.{group.Key.Name}"));
            throw new NotSupportedException($"DataVo EF schema generation does not yet support multiple entity types mapped to the same table: {duplicateList}.");
        }

        return OrderEntityTypesForCreation(entityTypes)
            .Select(BuildCreateTableStatement)
            .ToList();
    }

    /// <summary>
    /// Generates a single DataVo schema script from the current EF Core model.
    /// </summary>
    /// <param name="context">The EF Core context whose model is inspected.</param>
    /// <returns>A newline-delimited script containing DataVo <c>CREATE TABLE</c> statements.</returns>
    public static string GenerateDataVoCreateScript(this DbContext context)
    {
        return string.Join(Environment.NewLine, context.GenerateDataVoCreateStatements());
    }

    /// <summary>
    /// Creates DataVo tables for the EF Core model using the supplied DataVo connection string.
    /// </summary>
    /// <param name="context">The EF Core context whose model is inspected.</param>
    /// <param name="connectionString">Connection string used to execute generated DDL.</param>
    public static void EnsureDataVoCreated(this DbContext context, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        using var connection = new DataVoConnection(connectionString);
        connection.Open();

        foreach (string statement in context.GenerateDataVoCreateStatements())
        {
            using var command = connection.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
    }

    /// <summary>
    /// Applies DataVo schema creation using the connection string configured via <c>UseDataVo(...)</c>.
    /// </summary>
    public static void EnsureDataVoCreated(this DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        string connectionString = ResolveConfiguredConnectionString(context);
        EnsureDataVoCreated(context, connectionString);
    }

    internal static string ResolveConfiguredConnectionString(DbContext context)
    {
        var options = context.GetService<IDbContextOptions>();
        var extension = options.FindExtension<DataVoOptionsExtension>();
        var connectionString = extension?.BuildEffectiveConnectionString();

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "No DataVo connection string is configured. Call UseDataVo(connectionString) " +
                "or use UseDataVo with the typed options (e.g. o.UseStorageMode(...).WithDataSource(...)).");
        }

        return connectionString;
    }

    private static string BuildCreateTableStatement(IEntityType entityType)
    {
        string tableName = entityType.GetTableName()!;
        var tableIdentifier = StoreObjectIdentifier.Table(tableName, entityType.GetSchema());
        var primaryKey = entityType.FindPrimaryKey();
        var foreignKeyLookup = BuildForeignKeyLookup(entityType);

        string[] columnDefinitions = entityType
            .GetProperties()
            .Where(property => property.GetColumnName(tableIdentifier) is not null)
            .Select(property => BuildColumnDefinition(property, tableIdentifier, primaryKey, foreignKeyLookup))
            .ToArray();

        return $"CREATE TABLE IF NOT EXISTS {tableName} ({string.Join(", ", columnDefinitions)});";
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
                throw new NotSupportedException($"DataVo EF schema generation does not yet support cyclic table dependencies: {cycleTables}.");
            }

            foreach (IEntityType entityType in ready)
            {
                ordered.Add(entityType);
                remaining.Remove(entityType);
            }
        }

        return ordered;
    }

    private static Dictionary<IProperty, IForeignKey> BuildForeignKeyLookup(IEntityType entityType)
    {
        return entityType
            .GetForeignKeys()
            .Where(static fk => fk.Properties.Count == 1 && fk.PrincipalKey.Properties.Count == 1)
            .GroupBy(static fk => fk.Properties[0])
            .Where(static group => group.Count() == 1)
            .ToDictionary(static group => group.Key, static group => group.Single());
    }

    private static string BuildColumnDefinition(
        IProperty property,
        StoreObjectIdentifier tableIdentifier,
        IKey? primaryKey,
        IReadOnlyDictionary<IProperty, IForeignKey> foreignKeyLookup)
    {
        string columnName = property.GetColumnName(tableIdentifier) ?? property.Name;
        string sqlType = GetDataVoType(property);
        bool isPrimaryKey = primaryKey?.Properties.Contains(property) == true;

        string columnDefinition = $"{columnName} {sqlType}";

        if (isPrimaryKey)
        {
            columnDefinition += " PRIMARY KEY";
        }

        if (foreignKeyLookup.TryGetValue(property, out IForeignKey? foreignKey))
        {
            string principalTable = foreignKey.PrincipalEntityType.GetTableName()
                ?? throw new NotSupportedException($"Foreign key target for '{entityTypeName(property)}.{property.Name}' is not table-mapped.");
            var principalStoreObject = StoreObjectIdentifier.Table(principalTable, foreignKey.PrincipalEntityType.GetSchema());
            string principalColumn = foreignKey.PrincipalKey.Properties[0].GetColumnName(principalStoreObject)
                ?? foreignKey.PrincipalKey.Properties[0].Name;

            columnDefinition += $" REFERENCES {principalTable}({principalColumn})";
        }

        object? defaultValue = property.GetDefaultValue();
        if (defaultValue is not null)
        {
            columnDefinition += $" DEFAULT {FormatDefaultValue(defaultValue)}";
        }

        return columnDefinition;

        static string entityTypeName(IProperty sourceProperty)
            => sourceProperty.DeclaringType.DisplayName();
    }

    private static string GetDataVoType(IProperty property)
    {
        Type clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;

        if (clrType.IsEnum)
        {
            return "INT";
        }

        if (clrType == typeof(string) || clrType == typeof(Guid))
        {
            int? maxLength = property.GetMaxLength();
            return maxLength is > 0 ? $"VARCHAR({maxLength.Value})" : "VARCHAR";
        }

        if (clrType == typeof(bool))
        {
            return "BIT";
        }

        if (clrType == typeof(DateOnly) || clrType == typeof(DateTime) || clrType == typeof(DateTimeOffset))
        {
            return "DATE";
        }

        if (clrType == typeof(float) || clrType == typeof(double) || clrType == typeof(decimal))
        {
            return "FLOAT";
        }

        if (clrType == typeof(byte) || clrType == typeof(short) || clrType == typeof(int) || clrType == typeof(long))
        {
            return "INT";
        }

        throw new NotSupportedException($"The CLR type '{property.ClrType.Name}' on '{property.DeclaringType.DisplayName()}.{property.Name}' is not yet supported by DataVo EF schema generation.");
    }

    private static string FormatDefaultValue(object value)
    {
        return value switch
        {
            null => "NULL",
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
}
