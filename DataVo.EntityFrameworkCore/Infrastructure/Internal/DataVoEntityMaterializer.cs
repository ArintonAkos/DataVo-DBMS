using DataVo.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Globalization;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Bulk-loads all mapped entity types from a DataVo database into an EF Core context's
/// change tracker as <see cref="EntityState.Unchanged"/> entries.
///
/// After calling <see cref="LoadIntoContext"/>, standard EF LINQ queries
/// (e.g. <c>context.Tickets.ToList()</c>, <c>context.Tickets.Where(...)</c>) return
/// the data that lives inside DataVo, because the InMemory provider now mirrors it.
///
/// Relationship fix-up (navigation properties) is performed automatically by the EF
/// change tracker once all entities from all tables have been attached.
/// </summary>
internal static class DataVoEntityMaterializer
{
    // ------------------------------------------------------------------ public entry point

    /// <summary>
    /// Reads every mapped table from DataVo and attaches the rows as <c>Unchanged</c>
    /// entities into <paramref name="context"/>.
    /// </summary>
    /// <param name="context">The EF context that owns the InMemory store.</param>
    /// <param name="connectionString">DataVo connection string.</param>
    /// <exception cref="DataVoEfException">Thrown if the load fails.</exception>
    public static void LoadIntoContext(DbContext context, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var entityTypes = context.Model
            .GetEntityTypes()
            .Where(static et =>
                !et.IsOwned() &&
                et.FindPrimaryKey() is not null &&
                et.GetTableName() is not null &&
                et.ClrType is { IsAbstract: false })
            .ToList();

        if (entityTypes.Count == 0)
        {
            return;
        }

        // Load principals before dependents so EF's relationship fix-up can wire nav props.
        var ordered = DataVoModelOrdering.OrderPrincipalsFirst(entityTypes);

        try
        {
            // Detach tracked entities first: pending changes (for example from a failed SaveChanges)
            // must not be written into the mirror by the saves below.
            context.ChangeTracker.Clear();

            // Reset the in-memory mirror, so repeated LoadFromDataVo() calls on the
            // same DbContext instance do not hit duplicate-key errors when we SaveChanges().
            ClearInMemoryMirror(context, entityTypes);

            // Release the connection before saving: the mirror save may itself ensure DataVo schema.
            using (var connection = DataVoBridgeStorage.Open(connectionString))
            {
                foreach (var entityType in ordered)
                {
                    LoadEntityType(context, entityType, connection);
                }
            }

            if (context.ChangeTracker.HasChanges())
            {
                DataVoBridgeExecutionContext.SuppressDataVoWrites = true;
                try
                {
                    context.SaveChanges();
                }
                finally
                {
                    DataVoBridgeExecutionContext.SuppressDataVoWrites = false;
                }
            }
        }
        catch (DataVoEfException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DataVoEfException(
                DataVoEfOperation.DataLoad,
                $"DataVo EF data load failed while reading tables into change tracker. Connection: {connectionString}. See inner exception for details.",
                ex);
        }
    }

    // ------------------------------------------------------------------ clear existing mirror data

    /// <summary>
    /// Clears currently mirrored entities from the EF InMemory store without touching DataVo.
    /// This method runs with bridge write suppression enabled so the interceptor does not
    /// propagate delete operations back to DataVo.
    /// </summary>
    private static void ClearInMemoryMirror(DbContext context, IReadOnlyList<IEntityType> entityTypes)
    {
        DataVoBridgeExecutionContext.SuppressDataVoWrites = true;
        try
        {
            foreach (var entityType in entityTypes)
            {
                var setMethod = typeof(DbContext)
                    .GetMethods()
                    .Single(static m => m.Name == nameof(DbContext.Set) && m.IsGenericMethod && m.GetParameters().Length == 0)
                    .MakeGenericMethod(entityType.ClrType);

                var set = (IQueryable?)setMethod.Invoke(context, null);
                var existing = set?.Cast<object>().ToList() ?? [];

                if (existing.Count > 0)
                {
                    context.RemoveRange(existing);
                }
            }

            if (context.ChangeTracker.HasChanges())
            {
                context.SaveChanges();
            }

            context.ChangeTracker.Clear();
        }
        finally
        {
            DataVoBridgeExecutionContext.SuppressDataVoWrites = false;
        }
    }

    // ------------------------------------------------------------------ per-table load

    private static void LoadEntityType(
        DbContext context,
        IEntityType entityType,
        DataVoBridgeConnection connection)
    {
        string tableName = entityType.GetTableName()!;
        var tableIdentifier = StoreObjectIdentifier.Table(tableName, entityType.GetSchema());

        var mappedProperties = entityType
            .GetProperties()
            .Where(static p => p.PropertyInfo is not null)
            .Select(p => (
                Property: p,
                ColumnName: p.GetColumnName(tableIdentifier) ?? p.Name))
            .ToList();

        try
        {
            using var command = connection.CreateCommand(DataVoSqlStatement.Text($"SELECT * FROM {tableName};"));

            using var reader = command.ExecuteReader();

            while (reader.Read())
            {
                object entity = Activator.CreateInstance(entityType.ClrType)
                    ?? throw new DataVoEfException(
                        DataVoEfOperation.DataLoad,
                        $"Could not create an instance of '{entityType.ClrType.FullName}'. Ensure it has a public parameterless constructor.");

                foreach (var (property, columnName) in mappedProperties)
                {
                    int ordinal;
                    try
                    {
                        ordinal = reader.GetOrdinal(columnName);
                    }
                    catch (IndexOutOfRangeException)
                    {
                        // Column exists in model but not in the DataVo table (schema drift).
                        // Skip silently — the property stays at its CLR default.
                        continue;
                    }

                    object? raw = reader.GetValue(ordinal);

                    if (raw is null or DBNull)
                    {
                        // Leave as CLR default (null for reference/nullable, 0/false for value types).
                        continue;
                    }

                    try
                    {
                        object converted = ConvertToClrType(raw, property.ClrType);
                        property.PropertyInfo!.SetValue(entity, converted);
                    }
                    catch (Exception ex)
                    {
                        throw new DataVoEfException(
                            DataVoEfOperation.DataLoad,
                            $"Failed to convert DataVo value '{raw}' (type {raw.GetType().Name}) " +
                            $"to CLR type '{property.ClrType.Name}' for " +
                            $"'{entityType.DisplayName()}.{property.Name}' (column '{columnName}').",
                            ex);
                    }
                }

                // Add to EF change tracker so SaveChanges() can persist into the InMemory store.
                context.Add(entity);
            }
        }
        catch (DataVoEfException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DataVoEfException(
                DataVoEfOperation.DataLoad,
                $"Failed to load entity type '{entityType.DisplayName()}' from DataVo table '{tableName}'.",
                ex);
        }
    }

    // ------------------------------------------------------------------ CLR type conversion

    /// <summary>
    /// Converts a DataVo <see cref="object"/> value to the CLR property type expected by EF.
    /// DataVo returns: <c>int</c> for INT, <c>string</c> for VARCHAR, <c>bool</c>/<c>int</c>
    /// for BIT, <see cref="DataVoDate"/> for DATE, UTC <see cref="DateTime"/> for DATETIME,
    /// <c>float</c> for FLOAT and <c>float[]</c> for VECTOR.
    /// </summary>
    internal static object ConvertToClrType(object value, Type targetType)
    {
        Type underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Already the right type.
        if (underlying.IsAssignableFrom(value.GetType()))
        {
            return value;
        }

        // bool — DataVo may return bool or int (0/1).
        if (underlying == typeof(bool))
        {
            return value switch
            {
                bool b => b,
                int i => i != 0,
                long l => l != 0,
                string s => bool.Parse(s),
                _ => Convert.ToBoolean(value, CultureInfo.InvariantCulture)
            };
        }

        // DateTime.
        if (underlying == typeof(DateTime))
        {
            return value switch
            {
                DateTime dt => dt,
                DataVoDate d => d.ToDateTime(),
                DateOnly d => d.ToDateTime(TimeOnly.MinValue),
                string s => DateTime.Parse(s, CultureInfo.InvariantCulture),
                _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
            };
        }

        // DateOnly.
        if (underlying == typeof(DateOnly))
        {
            return value switch
            {
                DateOnly d => d,
                DataVoDate d => d.ToDateOnly(),
                DateTime dt => DateOnly.FromDateTime(dt),
                string s => DateOnly.Parse(s, CultureInfo.InvariantCulture),
                _ => DateOnly.FromDateTime(Convert.ToDateTime(value, CultureInfo.InvariantCulture))
            };
        }

        // DateTimeOffset.
        if (underlying == typeof(DateTimeOffset))
        {
            return value switch
            {
                DateTimeOffset dto => dto,
                DateTime dt => new DateTimeOffset(dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt),
                DataVoDate d => new DateTimeOffset(d.ToDateTime()),
                DateOnly d => new DateTimeOffset(d.ToDateTime(TimeOnly.MinValue)),
                string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture),
                _ => new DateTimeOffset(Convert.ToDateTime(value, CultureInfo.InvariantCulture))
            };
        }

        // TimeOnly — stored as a DATETIME instant on 0001-01-01.
        if (underlying == typeof(TimeOnly))
        {
            return value switch
            {
                DateTime dt => TimeOnly.FromDateTime(dt),
                TimeSpan ts => TimeOnly.FromTimeSpan(ts),
                string s => TimeOnly.Parse(s, CultureInfo.InvariantCulture),
                _ => TimeOnly.FromDateTime(Convert.ToDateTime(value, CultureInfo.InvariantCulture))
            };
        }

        // Vectors — DataVo returns float[] for VECTOR columns.
        if (underlying == typeof(ReadOnlyMemory<float>) && value is float[] vector)
        {
            return new ReadOnlyMemory<float>(vector);
        }

        // Guid.
        if (underlying == typeof(Guid))
        {
            return value switch
            {
                Guid g => g,
                string s => Guid.Parse(s),
                _ => Guid.Parse(value.ToString()!)
            };
        }

        // Enum — stored as INT, or BIGINT for 64-bit and unsigned 32-bit enums.
        if (underlying.IsEnum)
        {
            return Enum.ToObject(underlying, Convert.ToInt64(value, CultureInfo.InvariantCulture));
        }

        // String.
        if (underlying == typeof(string))
        {
            return value.ToString()!;
        }

        // Numeric primitives — DataVo always returns int for INT columns;
        // we must widen/narrow for short, byte, long, float, decimal, etc.
        return Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture);
    }
}
