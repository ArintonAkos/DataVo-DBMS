using System.Buffers.Binary;
using System.Text;
using System.Collections.Concurrent;
using DataVo.Core.CompiledQueries;
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
        var reader = new ByteSpanReader(data);

        for (int i = 0; i < columns.Count; i++)
        {
            cells[i] = reader.ReadBoolean() ? CellValue.Null : DecodeTypedCell(ref reader, columns[i]);
        }

        return cells;
    }

    /// <summary>
    /// Returns <see langword="true"/> when a column's storage type is fixed-width (<c>INT</c>, <c>FLOAT</c>,
    /// or <c>BIT</c>) — the types the zero-allocation update path can overwrite in place.
    /// </summary>
    public static bool IsFixedWidthType(string columnType)
    {
        StorageColumnType type = ClassifyColumnType(columnType);
        return type is StorageColumnType.Int or StorageColumnType.Float or StorageColumnType.Bit;
    }

    /// <summary>Returns <see langword="true"/> when a column's storage type is the 32-bit integer type.</summary>
    public static bool IsIntegerType(string columnType) => ClassifyColumnType(columnType) == StorageColumnType.Int;

    /// <summary>
    /// Overwrites a single fixed-width cell (<c>INT</c>/<c>FLOAT</c>/<c>BIT</c>) in an already-serialized row
    /// buffer, in place and without allocation. Returns <see langword="false"/> — leaving the buffer
    /// untouched up to the failure point — when the cell cannot be patched in place: the column is
    /// variable-width, the existing cell is null (no value slot), the new value is null, or the buffer is
    /// malformed. Callers treat a <see langword="false"/> result as "fall back to the dictionary path".
    /// </summary>
    /// <param name="rowBytes">The serialized row to patch.</param>
    /// <param name="columns">The table schema, in storage order.</param>
    /// <param name="ordinal">The storage-order index of the column to overwrite.</param>
    /// <param name="newValue">The replacement value; converted to the column's storage encoding.</param>
    public static bool TryOverwriteFixedWidthCell(Span<byte> rowBytes, IReadOnlyList<Column> columns, int ordinal, object? newValue)
    {
        if ((uint)ordinal >= (uint)columns.Count || newValue is null)
        {
            return false;
        }

        int offset = 0;
        for (int i = 0; i < ordinal; i++)
        {
            if (!TryAdvancePastCell(rowBytes, columns[i], ref offset))
            {
                return false;
            }
        }

        if (offset >= rowBytes.Length)
        {
            return false;
        }

        bool isNull = rowBytes[offset] != 0;
        offset += sizeof(bool);
        if (isNull)
        {
            return false;
        }

        switch (ClassifyColumnType(columns[ordinal].Type))
        {
            case StorageColumnType.Int:
                if (offset + sizeof(int) > rowBytes.Length)
                {
                    return false;
                }

                BinaryPrimitives.WriteInt32LittleEndian(rowBytes.Slice(offset, sizeof(int)), Convert.ToInt32(newValue));
                return true;
            case StorageColumnType.Float:
                if (offset + sizeof(int) > rowBytes.Length)
                {
                    return false;
                }

                BinaryPrimitives.WriteInt32LittleEndian(
                    rowBytes.Slice(offset, sizeof(int)),
                    BitConverter.SingleToInt32Bits(Convert.ToSingle(newValue)));
                return true;
            case StorageColumnType.Bit:
                if (offset + sizeof(bool) > rowBytes.Length)
                {
                    return false;
                }

                rowBytes[offset] = Convert.ToBoolean(newValue) ? (byte)1 : (byte)0;
                return true;
            default:
                return false;
        }
    }

    /// <summary>
    /// Overwrites a non-null fixed-width cell with an unboxed primitive value.
    /// </summary>
    public static bool TryOverwriteFixedWidthCell(
        Span<byte> rowBytes,
        IReadOnlyList<Column> columns,
        int ordinal,
        DataVoFixedWidthValue newValue)
    {
        if ((uint)ordinal >= (uint)columns.Count)
        {
            return false;
        }

        int offset = 0;
        for (int i = 0; i < ordinal; i++)
        {
            if (!TryAdvancePastCell(rowBytes, columns[i], ref offset))
            {
                return false;
            }
        }

        if (offset >= rowBytes.Length)
        {
            return false;
        }

        bool isNull = rowBytes[offset] != 0;
        offset += sizeof(bool);
        if (isNull)
        {
            return false;
        }

        switch (ClassifyColumnType(columns[ordinal].Type))
        {
            case StorageColumnType.Int:
                if (newValue.Type != DataVoFixedWidthValueType.Int32 || offset + sizeof(int) > rowBytes.Length)
                {
                    return false;
                }

                BinaryPrimitives.WriteInt32LittleEndian(rowBytes.Slice(offset, sizeof(int)), newValue.AsInt32());
                return true;
            case StorageColumnType.Float:
                if (newValue.Type != DataVoFixedWidthValueType.Double || offset + sizeof(int) > rowBytes.Length)
                {
                    return false;
                }

                BinaryPrimitives.WriteInt32LittleEndian(
                    rowBytes.Slice(offset, sizeof(int)),
                    BitConverter.SingleToInt32Bits((float)newValue.AsDouble()));
                return true;
            case StorageColumnType.Bit:
                if (newValue.Type != DataVoFixedWidthValueType.Boolean || offset + sizeof(bool) > rowBytes.Length)
                {
                    return false;
                }

                rowBytes[offset] = newValue.AsBoolean() ? (byte)1 : (byte)0;
                return true;
            default:
                return false;
        }
    }

    /// <summary>
    /// Advances <paramref name="offset"/> past one serialized cell (its null flag and any value bytes),
    /// matching the wire format <see cref="WriteTypedCell"/> produces. Returns <see langword="false"/> on a
    /// truncated/malformed buffer.
    /// </summary>
    private static bool TryAdvancePastCell(ReadOnlySpan<byte> rowBytes, Column column, ref int offset)
    {
        if (offset >= rowBytes.Length)
        {
            return false;
        }

        bool isNull = rowBytes[offset] != 0;
        offset += sizeof(bool);
        if (isNull)
        {
            return true;
        }

        switch (ClassifyColumnType(column.Type))
        {
            case StorageColumnType.Int:
            case StorageColumnType.Float:
                offset += sizeof(int);
                break;
            case StorageColumnType.Bit:
                offset += sizeof(bool);
                break;
            case StorageColumnType.Date:
            case StorageColumnType.DateTime:
                offset += sizeof(long);
                break;
            case StorageColumnType.Vector:
                if (offset + sizeof(int) > rowBytes.Length)
                {
                    return false;
                }

                int count = BinaryPrimitives.ReadInt32LittleEndian(rowBytes.Slice(offset, sizeof(int)));
                if (count < 0)
                {
                    return false;
                }

                offset += sizeof(int) + checked(count * sizeof(int));
                break;
            default:
                if (!TryAdvancePastString(rowBytes, ref offset))
                {
                    return false;
                }

                break;
        }

        return offset <= rowBytes.Length;
    }

    /// <summary>
    /// Advances past a <see cref="BinaryWriter"/>-encoded string: a 7-bit length prefix followed by its
    /// UTF-8 bytes.
    /// </summary>
    private static bool TryAdvancePastString(ReadOnlySpan<byte> rowBytes, ref int offset)
    {
        int length = 0;
        int shift = 0;
        while (true)
        {
            if (offset >= rowBytes.Length || shift > 35)
            {
                return false;
            }

            byte b = rowBytes[offset++];
            length |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                break;
            }

            shift += 7;
        }

        offset += length;
        return length >= 0 && offset <= rowBytes.Length;
    }

    /// <summary>Decodes one non-null typed cell from the span reader (mirrors <see cref="ReadTypedCell"/>).</summary>
    // Storage type categories, resolved without allocation. column.Type is stored in the catalog as the
    // DataTypes enum name (PascalCase, e.g. "Int"/"Varchar"), so a ToUpperInvariant() switch re-cased and
    // allocated a string per column, per row. OrdinalIgnoreCase comparison classifies in place — zero alloc.
    private enum StorageColumnType { Int, Float, Bit, Date, DateTime, Vector, String }

    private static StorageColumnType ClassifyColumnType(string type) =>
        string.Equals(type, "INT", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Int :
        string.Equals(type, "FLOAT", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Float :
        string.Equals(type, "BIT", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Bit :
        string.Equals(type, "DATE", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Date :
        string.Equals(type, "DATETIME", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.DateTime :
        string.Equals(type, "VECTOR", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Vector :
        StorageColumnType.String;

    private static CellValue DecodeTypedCell(ref ByteSpanReader reader, Column column)
    {
        switch (ClassifyColumnType(column.Type))
        {
            case StorageColumnType.Int:
                return CellValue.From(reader.ReadInt32());
            case StorageColumnType.Float:
                return CellValue.From((double)BitConverter.Int32BitsToSingle(reader.ReadInt32()));
            case StorageColumnType.Bit:
                return CellValue.From(reader.ReadBoolean());
            case StorageColumnType.Date:
                return CellValue.From(DateOnly.FromDateTime(DateTime.FromBinary(reader.ReadInt64())));
            case StorageColumnType.Vector:
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
    /// Decodes only the columns flagged in <paramref name="isProjected"/> into <paramref name="destination"/>
    /// (in storage order), advancing past the rest without allocating. <paramref name="destination"/> must have
    /// room for the number of projected columns. The forward-only wire format is walked once.
    /// </summary>
    public static void DecodeProjectedCells(
        ReadOnlySpan<byte> data,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<bool> isProjected,
        Span<CellValue> destination)
    {
        var reader = new ByteSpanReader(data);
        int next = 0;

        for (int i = 0; i < columns.Count; i++)
        {
            bool isNull = reader.ReadBoolean();
            if (isProjected[i])
            {
                destination[next++] = isNull ? CellValue.Null : DecodeTypedCell(ref reader, columns[i]);
            }
            else if (!isNull)
            {
                SkipTypedCell(ref reader, columns[i]);
            }
        }
    }

    /// <summary>Advances the reader past one non-null typed cell without materializing it.</summary>
    private static void SkipTypedCell(ref ByteSpanReader reader, Column column)
    {
        switch (ClassifyColumnType(column.Type))
        {
            case StorageColumnType.Int:
            case StorageColumnType.Float:
                reader.Skip(sizeof(int));
                return;
            case StorageColumnType.Bit:
                reader.Skip(sizeof(bool));
                return;
            case StorageColumnType.Date:
                reader.Skip(sizeof(long));
                return;
            case StorageColumnType.Vector:
                reader.Skip(reader.ReadInt32() * sizeof(int));
                return;
            default:
                reader.SkipString();
                return;
        }
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
        StorageColumnType type = ClassifyColumnType(column.Type);
        if (type == StorageColumnType.Int)
        {
            writer.Write(Convert.ToInt32(boxed));
            return;
        }

        if (type == StorageColumnType.Float)
        {
            float floatValue = Convert.ToSingle(boxed);
            writer.Write(BitConverter.SingleToInt32Bits(floatValue));
            return;
        }

        if (type == StorageColumnType.Bit)
        {
            writer.Write(Convert.ToBoolean(boxed));
            return;
        }

        if (type == StorageColumnType.Date || type == StorageColumnType.DateTime)
        {
            writer.Write(ToBinaryDateValue(boxed));
            return;
        }

        if (type == StorageColumnType.Vector)
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
        StorageColumnType type = ClassifyColumnType(column.Type);
        if (type == StorageColumnType.Int)
        {
            return reader.ReadInt32();
        }

        if (type == StorageColumnType.Float)
        {
            return BitConverter.Int32BitsToSingle(reader.ReadInt32());
        }

        if (type == StorageColumnType.Bit)
        {
            return reader.ReadBoolean();
        }

        if (type == StorageColumnType.Date)
        {
            return DateOnly.FromDateTime(DateTime.FromBinary(reader.ReadInt64()));
        }

        if (type == StorageColumnType.DateTime)
        {
            return DateTime.FromBinary(reader.ReadInt64());
        }

        if (type == StorageColumnType.Vector)
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
        switch (ClassifyColumnType(column.Type))
        {
            case StorageColumnType.Int:
                writer.Write(cell.AsInt32());
                return;
            case StorageColumnType.Float:
                writer.Write(BitConverter.SingleToInt32Bits((float)cell.AsDouble()));
                return;
            case StorageColumnType.Bit:
                writer.Write(cell.AsBoolean());
                return;
            case StorageColumnType.Date:
                writer.Write(cell.AsDate().ToDateTime(TimeOnly.MinValue).ToBinary());
                return;
            case StorageColumnType.Vector:
            {
                ReadOnlySpan<float> vector = cell.AsVectorReadOnlySpan();
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
        switch (ClassifyColumnType(column.Type))
        {
            case StorageColumnType.Int:
                return CellValue.From(reader.ReadInt32());
            case StorageColumnType.Float:
                return CellValue.From((double)BitConverter.Int32BitsToSingle(reader.ReadInt32()));
            case StorageColumnType.Bit:
                return CellValue.From(reader.ReadBoolean());
            case StorageColumnType.Date:
                return CellValue.From(DateOnly.FromDateTime(DateTime.FromBinary(reader.ReadInt64())));
            case StorageColumnType.Vector:
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
