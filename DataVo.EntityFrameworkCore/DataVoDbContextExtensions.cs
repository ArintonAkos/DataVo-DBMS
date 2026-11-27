using System.Reflection;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Data;
using DataVo.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// EF Core helpers for generating DataVo schema from an EF model.
/// </summary>
public static class DataVoDbContextExtensions
{
    /// <summary>
    /// Generates DataVo <c>CREATE TABLE</c> statements, each followed by the <c>CREATE INDEX</c>
    /// statements of its secondary indexes, from the current EF Core model.
    /// </summary>
    /// <param name="context">The EF Core context whose model is inspected.</param>
    /// <returns>An ordered list of DataVo DDL statements.</returns>
    /// <exception cref="NotSupportedException">
    /// The model uses a shape DataVo cannot store faithfully, such as an unsupported CLR type, a vector
    /// property without a dimension, or a multi-column unique constraint.
    /// </exception>
    public static IReadOnlyList<string> GenerateDataVoCreateStatements(this DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        return BuildTableDefinitions(context)
            .SelectMany(static table => table.CreateIndexStatements.Prepend(table.CreateTableStatement))
            .ToList();
    }

    /// <summary>
    /// Generates a single DataVo schema script from the current EF Core model.
    /// </summary>
    /// <param name="context">The EF Core context whose model is inspected.</param>
    /// <returns>A newline-delimited script of DataVo DDL statements.</returns>
    public static string GenerateDataVoCreateScript(this DbContext context)
    {
        return string.Join(Environment.NewLine, context.GenerateDataVoCreateStatements());
    }

    /// <summary>
    /// Creates DataVo tables (and the secondary indexes of newly created tables) for the EF Core model
    /// using the supplied DataVo connection string.
    /// </summary>
    /// <param name="context">The EF Core context whose model is inspected.</param>
    /// <param name="connectionString">Connection string used to execute generated DDL.</param>
    public static void EnsureDataVoCreated(this DbContext context, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        EnsureSchema(context, connectionString);
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

    /// <summary>
    /// Creates every missing table and the secondary indexes of the tables it created. Returns whether
    /// any table was created.
    /// </summary>
    internal static bool EnsureSchema(DbContext context, string connectionString)
    {
        var tables = BuildTableDefinitions(context);

        using var connection = DataVoBridgeStorage.Open(connectionString);
        var existingTables = ReadTableNames(connection);
        bool createdAny = false;

        foreach (DataVoTableDefinition table in tables)
        {
            connection.ExecuteNonQuery(DataVoSqlStatement.Text(table.CreateTableStatement));
            if (existingTables.Contains(table.TableName))
            {
                continue;
            }

            createdAny = true;
            foreach (string createIndex in table.CreateIndexStatements)
            {
                CreateIndex(connection, createIndex);
            }
        }

        return createdAny;
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

    private static HashSet<string> ReadTableNames(DataVoBridgeConnection connection)
    {
        var tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var command = connection.CreateCommand(DataVoSqlStatement.Text("SHOW TABLES;"));
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            if (reader.GetValue(0) is string tableName)
            {
                tables.Add(tableName);
            }
        }

        return tables;
    }

    // Another context may create the same new table concurrently; its index then already exists.
    private static void CreateIndex(DataVoBridgeConnection connection, string createIndex)
    {
        try
        {
            connection.ExecuteNonQuery(DataVoSqlStatement.Text(createIndex));
        }
        catch (DataVoException ex) when (ex.ErrorCode == DataVoErrorCodes.IndexExists)
        {
        }
    }

    // ------------------------------------------------------------------ schema model

    private static IReadOnlyList<DataVoTableDefinition> BuildTableDefinitions(DbContext context)
    {
        // Entity types sharing a table (TPH) are represented by the one with the most columns.
        var entityTypes = context.Model
            .GetEntityTypes()
            .Where(static entityType =>
                !entityType.IsOwned() &&
                entityType.FindPrimaryKey() is not null &&
                entityType.GetTableName() is not null)
            .GroupBy(DataVoModelOrdering.GetTableKey, StringComparer.OrdinalIgnoreCase)
            .Select(static group => group
                .OrderByDescending(static entityType => entityType.GetProperties().Count())
                .ThenBy(static entityType => entityType.DisplayName(), StringComparer.Ordinal)
                .First());

        var createdTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tables = new List<DataVoTableDefinition>();

        foreach (IEntityType entityType in DataVoModelOrdering.OrderPrincipalsFirst(entityTypes))
        {
            tables.Add(BuildTableDefinition(entityType, createdTables));
            createdTables.Add(DataVoModelOrdering.GetTableKey(entityType));
        }

        return tables;
    }

    private static DataVoTableDefinition BuildTableDefinition(IEntityType entityType, IReadOnlySet<string> createdTables)
    {
        string tableName = entityType.GetTableName()!;
        var table = StoreObjectIdentifier.Table(tableName, entityType.GetSchema());
        IKey primaryKey = entityType.FindPrimaryKey()!;
        bool compositePrimaryKey = primaryKey.Properties.Count > 1;

        var foreignKeys = BuildForeignKeyLookup(entityType);
        var uniqueColumns = CollectUniqueColumns(entityType, primaryKey);

        var columnDefinitions = entityType
            .GetProperties()
            .Where(property => property.GetColumnName(table) is not null)
            .Select(property => BuildColumnDefinition(
                property,
                table,
                isSinglePrimaryKey: !compositePrimaryKey && primaryKey.Properties.Contains(property),
                isUnique: uniqueColumns.Contains(property),
                foreignKeys,
                createdTables))
            .ToList();

        if (compositePrimaryKey)
        {
            columnDefinitions.Add($"PRIMARY KEY ({string.Join(", ", primaryKey.Properties.Select(property => GetColumnName(property, table)))})");
        }

        string createTable = $"CREATE TABLE IF NOT EXISTS {tableName} ({string.Join(", ", columnDefinitions)});";
        return new DataVoTableDefinition(tableName, createTable, BuildCreateIndexStatements(entityType, table));
    }

    private static string BuildColumnDefinition(
        IProperty property,
        StoreObjectIdentifier table,
        bool isSinglePrimaryKey,
        bool isUnique,
        IReadOnlyDictionary<IProperty, IForeignKey> foreignKeys,
        IReadOnlySet<string> createdTables)
    {
        var definition = new List<string> { GetColumnName(property, table), GetDataVoType(property) };

        if (!isSinglePrimaryKey && !property.IsColumnNullable(table))
        {
            definition.Add("NOT NULL");
        }

        // Only an explicitly configured default: GetDefaultValue() also reports the CLR default.
        object? defaultValue = property.FindAnnotation(RelationalAnnotationNames.DefaultValue)?.Value;
        if (defaultValue is not null and not DBNull)
        {
            definition.Add($"DEFAULT {DataVoBridgeValues.FormatLiteral(defaultValue)}");
        }

        if (isSinglePrimaryKey)
        {
            definition.Add("PRIMARY KEY");
        }

        if (isUnique)
        {
            definition.Add("UNIQUE");
        }

        if (foreignKeys.TryGetValue(property, out IForeignKey? foreignKey))
        {
            string principalTable = foreignKey.PrincipalEntityType.GetTableName()!;
            string? principalSchema = foreignKey.PrincipalEntityType.GetSchema();

            // A reference to a table created later (a dependency cycle) is left to the bridge to order.
            if (createdTables.Contains(DataVoModelOrdering.GetTableKey(principalTable, principalSchema)))
            {
                var principalStoreObject = StoreObjectIdentifier.Table(principalTable, principalSchema);
                string principalColumn = GetColumnName(foreignKey.PrincipalKey.Properties[0], principalStoreObject);
                definition.Add($"REFERENCES {principalTable}({principalColumn})");
            }
        }

        return string.Join(" ", definition);
    }

    private static Dictionary<IProperty, IForeignKey> BuildForeignKeyLookup(IEntityType entityType)
    {
        return entityType
            .GetForeignKeys()
            .Where(static fk => fk.Properties.Count == 1 && fk.PrincipalEntityType.GetTableName() is not null)
            .GroupBy(static fk => fk.Properties[0])
            .Where(static group => group.Count() == 1)
            .ToDictionary(static group => group.Key, static group => group.Single());
    }

    /// <summary>
    /// Returns the columns that need a <c>UNIQUE</c> constraint: single-column unique indexes and
    /// alternate keys. DataVo enforces uniqueness on single columns only, so a multi-column unique
    /// constraint is rejected rather than silently dropped.
    /// </summary>
    private static HashSet<IProperty> CollectUniqueColumns(IEntityType entityType, IKey primaryKey)
    {
        var uniqueSets = entityType.GetIndexes()
            .Where(static index => index.IsUnique)
            .Select(static index => index.Properties)
            .Concat(entityType.GetKeys()
                .Where(key => key != primaryKey)
                .Select(static key => key.Properties));

        var columns = new HashSet<IProperty>();
        foreach (IReadOnlyList<IProperty> properties in uniqueSets)
        {
            if (properties.Count != 1)
            {
                throw new NotSupportedException(
                    $"'{entityType.DisplayName()}' declares a unique constraint on ({string.Join(", ", properties.Select(static p => p.Name))}). " +
                    "DataVo enforces UNIQUE on single columns only; use a single-column unique index or a composite primary key.");
            }

            if (!primaryKey.Properties.SequenceEqual(properties))
            {
                columns.Add(properties[0]);
            }
        }

        return columns;
    }

    private static IReadOnlyList<string> BuildCreateIndexStatements(IEntityType entityType, StoreObjectIdentifier table)
    {
        return entityType.GetIndexes()
            .Where(static index => !index.IsUnique)
            .Select(index =>
            {
                var columns = index.Properties.Select(property => GetColumnName(property, table)).ToArray();
                string name = index.GetDatabaseName(table) ?? $"IX_{table.Name}_{string.Join("_", columns)}";
                return $"CREATE INDEX {name} ON {table.Name} ({string.Join(", ", columns)});";
            })
            .ToList();
    }

    private static string GetColumnName(IProperty property, StoreObjectIdentifier table)
        => property.GetColumnName(table) ?? property.Name;

    // ------------------------------------------------------------------ type mapping

    private static string GetDataVoType(IProperty property)
    {
        // Read the annotation directly: GetColumnType() needs a relational type mapping the host may not have.
        if (property.FindAnnotation(RelationalAnnotationNames.ColumnType)?.Value is string { Length: > 0 } explicitType)
        {
            return explicitType;
        }

        Type clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;

        if (clrType.IsEnum)
        {
            return DataVoBridgeValues.IsWideEnum(clrType) ? "BIGINT" : "INT";
        }

        if (clrType == typeof(string) || clrType == typeof(Guid))
        {
            int? maxLength = property.GetMaxLength();
            return maxLength is > 0 ? $"VARCHAR({maxLength.Value})" : "VARCHAR";
        }

        if (clrType == typeof(char))
        {
            return "VARCHAR(1)";
        }

        if (clrType == typeof(bool))
        {
            return "BIT";
        }

        if (clrType == typeof(DateOnly))
        {
            return "DATE";
        }

        if (clrType == typeof(DateTime) || clrType == typeof(DateTimeOffset) || clrType == typeof(TimeOnly))
        {
            return "DATETIME";
        }

        if (clrType == typeof(float[]) || clrType == typeof(ReadOnlyMemory<float>))
        {
            return $"VECTOR({ResolveVectorDimensions(property)})";
        }

        if (clrType == typeof(float))
        {
            return "FLOAT";
        }

        if (clrType == typeof(double) || clrType == typeof(decimal))
        {
            return "DOUBLE";
        }

        if (clrType == typeof(byte) ||
            clrType == typeof(sbyte) ||
            clrType == typeof(short) ||
            clrType == typeof(ushort) ||
            clrType == typeof(int))
        {
            return "INT";
        }

        if (clrType == typeof(uint) || clrType == typeof(long) || clrType == typeof(ulong))
        {
            return "BIGINT";
        }

        throw new NotSupportedException($"The CLR type '{property.ClrType.Name}' on '{property.DeclaringType.DisplayName()}.{property.Name}' is not yet supported by DataVo EF schema generation.");
    }

    /// <summary>
    /// Resolves a vector column's dimension from <c>HasDataVoVectorDimensions</c>, a
    /// <see cref="VectorDimensionsAttribute"/>, or <c>HasMaxLength</c>, in that order.
    /// </summary>
    private static int ResolveVectorDimensions(IProperty property)
    {
        int? dimensions = property.FindAnnotation(DataVoAnnotationNames.VectorDimensions)?.Value as int?
            ?? property.PropertyInfo?.GetCustomAttribute<VectorDimensionsAttribute>()?.Dimensions
            ?? property.GetMaxLength();

        if (dimensions is not > 0)
        {
            throw new NotSupportedException(
                $"The vector property '{property.DeclaringType.DisplayName()}.{property.Name}' needs a dimension for its DataVo VECTOR(n) column. " +
                "Configure it with HasDataVoVectorDimensions(n), [VectorDimensions(n)], HasMaxLength(n) or HasColumnType(\"VECTOR(n)\").");
        }

        return dimensions.Value;
    }

    private sealed record DataVoTableDefinition(
        string TableName,
        string CreateTableStatement,
        IReadOnlyList<string> CreateIndexStatements);
}
