using System.Text;
using System.Collections.Concurrent;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;

namespace DataVo.Core.StorageEngine.Serialization;

/// <summary>
/// Serializes row dictionaries to binary payloads and deserializes them back using the active catalog schema.
/// </summary>
/// <remarks>
/// The serializer is schema-aware. Column order, null handling, and primitive encodings are driven by
/// catalog metadata rather than by the order of values in the supplied dictionary.
/// </remarks>
public static class RowSerializer
{
    /// <summary>
    /// Caches schema snapshots per engine/database/table combination.
    /// </summary>
    private sealed class SchemaCacheEntry
    {
        /// <summary>
        /// Gets the schema version associated with the cached columns.
        /// </summary>
        public int Version { get; init; }

        /// <summary>
        /// Gets the cached column metadata.
        /// </summary>
        public List<Column> Columns { get; init; } = [];
    }

    /// <summary>
    /// Stores schema cache entries by engine/database/table key.
    /// </summary>
    private static readonly ConcurrentDictionary<string, SchemaCacheEntry> _schemaCache = new();

    /// <summary>
    /// Serializes a dictionary of column names and values into a tight binary format
    /// based on the schema order defined in the table's Catalog.
    /// </summary>
    public static byte[] Serialize(string databaseName, string tableName, Dictionary<string, dynamic> row)
    {
        var columns = GetCachedSchemaColumns(databaseName, tableName);
        using var memoryStream = new MemoryStream();
        using var writer = new BinaryWriter(memoryStream, Encoding.UTF8, leaveOpen: true);

        foreach (var column in columns)
        {
            if (!row.TryGetValue(column.Name, out var value) || value == null)
            {
                writer.Write(true);
                continue;
            }

            writer.Write(false);
            WriteNonNullValue(writer, column, value);
        }

        writer.Flush();
        return memoryStream.ToArray();
    }

    /// <summary>
    /// Deserializes a raw binary payload back into a dictionary of column names and typed values
    /// using the schema defined in the table's Catalog.
    /// </summary>
    public static Dictionary<string, dynamic> Deserialize(string databaseName, string tableName, byte[] data)
    {
        return Deserialize(databaseName, tableName, data, selectedColumns: null);
    }

    /// <summary>
    /// Deserializes a raw binary payload back into a dictionary of column names and typed values,
    /// optionally projecting only selected columns.
    /// </summary>
    public static Dictionary<string, dynamic> Deserialize(string databaseName, string tableName, byte[] data, HashSet<string>? selectedColumns)
    {
        var columns = GetCachedSchemaColumns(databaseName, tableName);
        var row = new Dictionary<string, dynamic>();

        using var memoryStream = new MemoryStream(data);
        using var reader = new BinaryReader(memoryStream, Encoding.UTF8, leaveOpen: true);

        foreach (var column in columns)
        {
            bool includeColumn = selectedColumns == null || selectedColumns.Contains(column.Name);
            bool isNull = reader.ReadBoolean();
            if (isNull)
            {
                if (includeColumn)
                {
                    row[column.Name] = null!;
                }
                continue;
            }

            var value = ReadNonNullValue(reader, column);
            if (includeColumn)
            {
                row[column.Name] = value;
            }
        }

        return row;
    }

    /// <summary>
    /// Gets schema columns from the cache or refreshes them from the catalog when the schema version changes.
    /// </summary>
    private static List<Column> GetCachedSchemaColumns(string databaseName, string tableName)
    {
        string cacheKey = BuildSchemaCacheKey(databaseName, tableName);
        var catalog = DataVoEngine.Current().Catalog;
        int currentVersion = catalog.GetTableSchemaVersion(tableName, databaseName);

        if (_schemaCache.TryGetValue(cacheKey, out var cachedEntry) && cachedEntry.Version == currentVersion)
        {
            return cachedEntry.Columns;
        }

        var columns = catalog.GetTableColumns(tableName, databaseName);
        _schemaCache[cacheKey] = new SchemaCacheEntry
        {
            Version = currentVersion,
            Columns = columns,
        };

        return columns;
    }

    /// <summary>
    /// Writes a non-null value using the column type encoding.
    /// </summary>
    private static void WriteNonNullValue(BinaryWriter writer, Column column, dynamic value)
    {
        string type = column.Type.ToUpperInvariant();
        if (type == "INT")
        {
            writer.Write(Convert.ToInt32(value));
            return;
        }

        if (type == "FLOAT")
        {
            writer.Write(Convert.ToSingle(value));
            return;
        }

        if (type == "BIT")
        {
            writer.Write(Convert.ToBoolean(value));
            return;
        }

        if (type == "DATE" || type == "DATETIME")
        {
            writer.Write(ToBinaryDateValue(value));
            return;
        }

        writer.Write(value?.ToString() ?? string.Empty);
    }

    /// <summary>
    /// Reads a non-null value using the column type encoding.
    /// </summary>
    private static dynamic ReadNonNullValue(BinaryReader reader, Column column)
    {
        string type = column.Type.ToUpperInvariant();
        if (type == "INT")
        {
            return reader.ReadInt32();
        }

        if (type == "FLOAT")
        {
            return reader.ReadSingle();
        }

        if (type == "BIT")
        {
            return reader.ReadBoolean();
        }

        if (type == "DATE")
        {
            return DateOnly.FromDateTime(DateTime.FromBinary(reader.ReadInt64()));
        }

        if (type == "DATETIME")
        {
            return DateTime.FromBinary(reader.ReadInt64());
        }

        return reader.ReadString();
    }

    /// <summary>
    /// Converts supported date representations to the binary format used by the serializer.
    /// </summary>
    private static long ToBinaryDateValue(dynamic value)
    {
        if (value is DateOnly dateOnly)
        {
            return dateOnly.ToDateTime(TimeOnly.MinValue).ToBinary();
        }

        if (value is DateTime dateTime)
        {
            return dateTime.ToBinary();
        }

        return Convert.ToDateTime(value).ToBinary();
    }

    /// <summary>
    /// Builds the cache key used for schema caching.
    /// </summary>
    private static string BuildSchemaCacheKey(string databaseName, string tableName)
    {
        return $"{DataVoEngine.Current().Id:N}::{databaseName}::{tableName}";
    }
}
