using System.Text;
using System.Collections.Concurrent;
using DataVo.Core.Models.Catalog;

namespace DataVo.Core.StorageEngine.Serialization;

public static class RowSerializer
{
    private sealed class SchemaCacheEntry
    {
        public int Version { get; init; }
        public List<Column> Columns { get; init; } = [];
    }

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
            // Null check (implementing basic null bitmap or prefix later if necessary, 
            // for now assume required matching the old mongodb behavior)
            if (!row.TryGetValue(column.Name, out var value) || value == null)
            {
                // Write a boolean "is null" flag for every field
                writer.Write(true);
                continue;
            }

            writer.Write(false); // Not null flag

            string type = column.Type.ToUpperInvariant();
            if (type == "INT")
            {
                writer.Write(Convert.ToInt32(value));
            }
            else if (type == "FLOAT")
            {
                writer.Write(Convert.ToSingle(value));
            }
            else if (type == "BIT")
            {
                writer.Write(Convert.ToBoolean(value));
            }
            else if (type == "DATE" || type == "DATETIME")
            {
                if (value is DateOnly dateOnly)
                {
                    writer.Write(dateOnly.ToDateTime(TimeOnly.MinValue).ToBinary());
                }
                else if (value is DateTime dateTime)
                {
                    writer.Write(dateTime.ToBinary());
                }
                else
                {
                    writer.Write(Convert.ToDateTime(value).ToBinary());
                }
            }
            else // VARCHAR etc
            {
                writer.Write(value!.ToString() ?? "");
            }
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

            string type = column.Type.ToUpperInvariant();
            if (type == "INT")
            {
                int value = reader.ReadInt32();
                if (includeColumn) row[column.Name] = value;
            }
            else if (type == "FLOAT")
            {
                float value = reader.ReadSingle();
                if (includeColumn) row[column.Name] = value;
            }
            else if (type == "BIT")
            {
                bool value = reader.ReadBoolean();
                if (includeColumn) row[column.Name] = value;
            }
            else if (type == "DATE")
            {
                DateOnly value = DateOnly.FromDateTime(DateTime.FromBinary(reader.ReadInt64()));
                if (includeColumn) row[column.Name] = value;
            }
            else if (type == "DATETIME")
            {
                DateTime value = DateTime.FromBinary(reader.ReadInt64());
                if (includeColumn) row[column.Name] = value;
            }
            else // VARCHAR
            {
                string value = reader.ReadString();
                if (includeColumn) row[column.Name] = value;
            }
        }

        return row;
    }

    private static List<Column> GetCachedSchemaColumns(string databaseName, string tableName)
    {
        string cacheKey = BuildSchemaCacheKey(databaseName, tableName);
        int currentVersion = Catalog.GetTableSchemaVersion(tableName, databaseName);

        if (_schemaCache.TryGetValue(cacheKey, out var cachedEntry) && cachedEntry.Version == currentVersion)
        {
            return cachedEntry.Columns;
        }

        var columns = Catalog.GetTableColumns(tableName, databaseName);
        _schemaCache[cacheKey] = new SchemaCacheEntry
        {
            Version = currentVersion,
            Columns = columns,
        };

        return columns;
    }

    private static string BuildSchemaCacheKey(string databaseName, string tableName)
    {
        return $"{databaseName}::{tableName}";
    }
}
