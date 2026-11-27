using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Orders mapped entity types by their foreign-key dependencies, shared by schema creation, the
/// change-tracker bridge and data loading.
/// </summary>
internal static class DataVoModelOrdering
{
    /// <summary>
    /// Returns <paramref name="entityTypes"/> with principal tables before dependent tables. Entity types
    /// that share a table stay together; self references are ignored. A dependency cycle is broken at the
    /// table that sorts first, so the result always contains every entity type.
    /// </summary>
    internal static IReadOnlyList<IEntityType> OrderPrincipalsFirst(IEnumerable<IEntityType> entityTypes)
    {
        var byTable = entityTypes
            .Where(static entityType => entityType.GetTableName() is not null)
            .GroupBy(GetTableKey, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                static group => group.Key,
                static group => group.OrderBy(static entityType => entityType.DisplayName(), StringComparer.Ordinal).ToList(),
                StringComparer.OrdinalIgnoreCase);

        var dependencies = byTable.ToDictionary(
            static pair => pair.Key,
            pair => pair.Value
                .SelectMany(static entityType => entityType.GetForeignKeys())
                .Select(static foreignKey => foreignKey.PrincipalEntityType)
                .Where(static principal => principal.GetTableName() is not null)
                .Select(GetTableKey)
                .Where(table => byTable.ContainsKey(table) && !string.Equals(table, pair.Key, StringComparison.OrdinalIgnoreCase))
                .ToHashSet(StringComparer.OrdinalIgnoreCase),
            StringComparer.OrdinalIgnoreCase);

        var remaining = new HashSet<string>(byTable.Keys, StringComparer.OrdinalIgnoreCase);
        var ordered = new List<IEntityType>();

        while (remaining.Count > 0)
        {
            var ready = remaining
                .Where(table => dependencies[table].All(dependency => !remaining.Contains(dependency)))
                .OrderBy(static table => table, StringComparer.Ordinal)
                .ToList();

            if (ready.Count == 0)
            {
                ready.Add(remaining.OrderBy(static table => table, StringComparer.Ordinal).First());
            }

            foreach (string table in ready)
            {
                ordered.AddRange(byTable[table]);
                remaining.Remove(table);
            }
        }

        return ordered;
    }

    /// <summary>Returns the schema-qualified table key of a table-mapped entity type.</summary>
    internal static string GetTableKey(IEntityType entityType)
        => GetTableKey(entityType.GetTableName()!, entityType.GetSchema());

    /// <summary>Returns the schema-qualified key of a table.</summary>
    internal static string GetTableKey(string tableName, string? schema)
        => schema is null ? tableName : $"{schema}.{tableName}";
}
