using System.Text;
using System.Collections.Concurrent;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Utils;

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

    [ThreadStatic] private static MemoryStream? _scratchStream;
    [ThreadStatic] private static BinaryWriter? _scratchWriter;

    /// <summary>
    /// Serializes a dictionary of column names and values into a tight binary format
    /// based on the schema order defined in the table's Catalog.
    /// </summary>
    public static byte[] Serialize(string databaseName, string tableName, Dictionary<string, object?> row)
    {
        DataVoEngine engine = DataVoEngine.Current();
        return Serialize(databaseName, tableName, row, engine.Catalog, engine.Id.ToString("N"));
    }

    /// <summary>
    /// Serializes a dictionary of column names and values using an explicit schema context.
    /// </summary>
    public static byte[] Serialize(
        string databaseName,
        string tableName,
        Dictionary<string, object?> row,
        EngineCatalog catalog,
        string schemaScopeKey)
    {
        var columns = GetCachedSchemaColumns(databaseName, tableName, catalog, schemaScopeKey);
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
            WriteNonNullValue(writer, column, (object)value);
        }

        writer.Flush();
        return memoryStream.ToArray();
    }

    /// <summary>
    /// Deserializes a raw binary payload back into a dictionary of column names and typed values
    /// using the schema defined in the table's Catalog.
    /// </summary>
    public static Dictionary<string, object?> Deserialize(string databaseName, string tableName, byte[] data)
    {
        return Deserialize(databaseName, tableName, data, selectedColumns: null);
    }

    /// <summary>
    /// Deserializes a raw binary payload back into a dictionary of column names and typed values,
    /// optionally projecting only selected columns.
    /// </summary>
    public static Dictionary<string, object?> Deserialize(string databaseName, string tableName, byte[] data, HashSet<string>? selectedColumns)
    {
        DataVoEngine engine = DataVoEngine.Current();
        return Deserialize(databaseName, tableName, data, selectedColumns, engine.Catalog, engine.Id.ToString("N"));
    }

    /// <summary>
    /// Deserializes a raw binary payload back into a dictionary of column names and typed values,
    /// optionally projecting only selected columns using an explicit schema context.
    /// </summary>
    public static Dictionary<string, object?> Deserialize(
        string databaseName,
        string tableName,
        byte[] data,
        HashSet<string>? selectedColumns,
        EngineCatalog catalog,
        string schemaScopeKey)
    {
        var columns = GetCachedSchemaColumns(databaseName, tableName, catalog, schemaScopeKey);
        var row = new Dictionary<string, object?>();

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
    /// Serializes typed cells (aligned by index with <paramref name="columns"/>) into the same binary
    /// format the dictionary <see cref="Serialize(string,string,Dictionary{string,object?},EngineCatalog,string)"/>
    /// produces — no dictionary intermediate.
    /// </summary>
    public static byte[] SerializeCells(IReadOnlyList<Column> columns, ReadOnlySpan<CellValue> cells)
    {
        if (cells.Length != columns.Count)
        {
            throw new ArgumentException(
                $"Row has {cells.Length} cells but schema has {columns.Count} columns.", nameof(cells));
        }

        MemoryStream stream = _scratchStream ??= new MemoryStream(256);
        BinaryWriter writer = _scratchWriter ??= new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        stream.Position = 0;
        stream.SetLength(0);

        for (int i = 0; i < columns.Count; i++)
        {
            CellValue cell = cells[i];
            if (cell.IsNull)
            {
                writer.Write(true);
                continue;
            }

            writer.Write(false);
            WriteTypedCell(writer, columns[i], cell);
        }

        writer.Flush();
        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a binary payload into typed cells aligned by index with <paramref name="columns"/>,
    /// reading the same wire format the dictionary path writes.
    /// </summary>
    public static CellValue[] DeserializeCells(byte[] data, IReadOnlyList<Column> columns)
    {
        var cells = new CellValue[columns.Count];

        using var memoryStream = new MemoryStream(data);
        using var reader = new BinaryReader(memoryStream, Encoding.UTF8, leaveOpen: true);

        for (int i = 0; i < columns.Count; i++)
        {
            cells[i] = reader.ReadBoolean() ? CellValue.Null : ReadTypedCell(reader, columns[i]);
        }

        return cells;
    }

    /// <summary>
    /// Gets schema columns from the cache or refreshes them from the catalog when the schema version changes.
    /// </summary>
    private static List<Column> GetCachedSchemaColumns(string databaseName, string tableName, EngineCatalog catalog, string schemaScopeKey)
    {
        string cacheKey = BuildSchemaCacheKey(schemaScopeKey, databaseName, tableName);
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
    private static void WriteNonNullValue(BinaryWriter writer, Column column, object value)
    {
        object boxed = value;
        string type = column.Type.ToUpperInvariant();
        if (type == "INT")
        {
            writer.Write(Convert.ToInt32(boxed));
            return;
        }

        if (type == "FLOAT")
        {
            float floatValue = Convert.ToSingle(boxed);
            writer.Write(BitConverter.SingleToInt32Bits(floatValue));
            return;
        }

        if (type == "BIT")
        {
            writer.Write(Convert.ToBoolean(boxed));
            return;
        }

        if (type == "DATE" || type == "DATETIME")
        {
            writer.Write(ToBinaryDateValue(boxed));
            return;
        }

        if (type == "VECTOR")
        {
            if (!VectorParser.TryCoerceToVector(boxed, out float[] vector))
            {
                throw new InvalidOperationException($"Column '{column.Name}' expects VECTOR data.");
            }

            writer.Write(vector.Length);
            foreach (float item in vector)
            {
                writer.Write(BitConverter.SingleToInt32Bits(item));
            }
            return;
        }

        writer.Write(boxed.ToString() ?? string.Empty);
    }

    /// <summary>
    /// Reads a non-null value using the column type encoding.
    /// </summary>
    private static object ReadNonNullValue(BinaryReader reader, Column column)
    {
        string type = column.Type.ToUpperInvariant();
        if (type == "INT")
        {
            return reader.ReadInt32();
        }

        if (type == "FLOAT")
        {
            return BitConverter.Int32BitsToSingle(reader.ReadInt32());
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

        if (type == "VECTOR")
        {
            int count = reader.ReadInt32();
            float[] vector = new float[count];
            for (int i = 0; i < count; i++)
            {
                vector[i] = BitConverter.Int32BitsToSingle(reader.ReadInt32());
            }

            return vector;
        }

        return reader.ReadString();
    }

    /// <summary>Writes a non-null typed cell using the column type encoding (matches <see cref="WriteNonNullValue"/>).</summary>
    private static void WriteTypedCell(BinaryWriter writer, Column column, CellValue cell)
    {
        switch (column.Type.ToUpperInvariant())
        {
            case "INT":
                writer.Write(cell.AsInt32());
                return;
            case "FLOAT":
                writer.Write(BitConverter.SingleToInt32Bits((float)cell.AsDouble()));
                return;
            case "BIT":
                writer.Write(cell.AsBoolean());
                return;
            case "DATE":
                writer.Write(cell.AsDate().ToDateTime(TimeOnly.MinValue).ToBinary());
                return;
            case "VECTOR":
            {
                float[] vector = cell.AsVector();
                writer.Write(vector.Length);
                foreach (float item in vector)
                {
                    writer.Write(BitConverter.SingleToInt32Bits(item));
                }

                return;
            }
            default:
                writer.Write(cell.AsString() ?? string.Empty);
                return;
        }
    }

    /// <summary>Reads a non-null typed cell using the column type encoding (matches <see cref="ReadNonNullValue"/>).</summary>
    private static CellValue ReadTypedCell(BinaryReader reader, Column column)
    {
        switch (column.Type.ToUpperInvariant())
        {
            case "INT":
                return CellValue.From(reader.ReadInt32());
            case "FLOAT":
                return CellValue.From((double)BitConverter.Int32BitsToSingle(reader.ReadInt32()));
            case "BIT":
                return CellValue.From(reader.ReadBoolean());
            case "DATE":
                return CellValue.From(DateOnly.FromDateTime(DateTime.FromBinary(reader.ReadInt64())));
            case "VECTOR":
            {
                int count = reader.ReadInt32();
                float[] vector = new float[count];
                for (int i = 0; i < count; i++)
                {
                    vector[i] = BitConverter.Int32BitsToSingle(reader.ReadInt32());
                }

                return CellValue.From(vector);
            }
            default:
                return CellValue.From(reader.ReadString());
        }
    }

    /// <summary>
    /// Converts supported date representations to the binary format used by the serializer.
    /// </summary>
    private static long ToBinaryDateValue(object value)
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
    private static string BuildSchemaCacheKey(string schemaScopeKey, string databaseName, string tableName)
    {
        return $"{schemaScopeKey}::{databaseName}::{tableName}";
    }
}
