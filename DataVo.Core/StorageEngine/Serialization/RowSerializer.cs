using System.Buffers.Binary;
using System.Text;
using System.Collections.Concurrent;
using System.Globalization;
using DataVo.Core.Compat;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Utils;
using DataVo.Core.ValueConversion;

namespace DataVo.Core.StorageEngine.Serialization;

/// <summary>
/// Serializes row dictionaries to binary payloads and deserializes them back using the active catalog schema.
/// </summary>
/// <remarks>
/// The serializer is schema-aware. Column order, null handling, and primitive encodings are driven by
/// catalog metadata rather than by the order of values in the supplied dictionary.
/// </remarks>
public static partial class RowSerializer
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
    public static byte[] SerializeCells(IReadOnlyList<Column> columns, ReadOnlySpan<CellValue> cells) =>
        SerializeCells(columns, cells, omittedOrdinal: -1);

    /// <summary>
    /// <see cref="SerializeCells(IReadOnlyList{Column}, ReadOnlySpan{CellValue})"/> with cell
    /// <paramref name="omittedOrdinal"/> written as NULL: the stored form of a clustered row, whose key
    /// the row key holds (see <see cref="WithoutCell"/>, which produces the same bytes from a full row).
    /// </summary>
    public static byte[] SerializeCells(IReadOnlyList<Column> columns, ReadOnlySpan<CellValue> cells, int omittedOrdinal)
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
            if (cell.IsNull || i == omittedOrdinal)
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
    /// Returns <see langword="true"/> when a column's storage type is fixed-width (<c>INT</c>, <c>BIGINT</c>, <c>FLOAT</c>, <c>DOUBLE</c>,
    /// or <c>BIT</c>) — the types the zero-allocation update path can overwrite in place.
    /// </summary>
    public static bool IsFixedWidthType(string columnType)
    {
        StorageColumnType type = ClassifyColumnType(columnType);
        return type is StorageColumnType.Int or StorageColumnType.BigInt or StorageColumnType.Float
            or StorageColumnType.Double or StorageColumnType.Bit;
    }

    /// <summary>Returns <see langword="true"/> when a column's storage type is an integer type (<c>INT</c> or <c>BIGINT</c>).</summary>
    public static bool IsIntegerType(string columnType) => ClassifyColumnType(columnType) is StorageColumnType.Int or StorageColumnType.BigInt;

    /// <summary>
    /// Overwrites a single fixed-width cell (<c>INT</c>/<c>BIGINT</c>/<c>FLOAT</c>/<c>DOUBLE</c>/<c>BIT</c>) in an already-serialized row
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
            case StorageColumnType.BigInt:
                if (offset + sizeof(long) > rowBytes.Length)
                {
                    return false;
                }

                BinaryPrimitives.WriteInt64LittleEndian(rowBytes.Slice(offset, sizeof(long)), Convert.ToInt64(newValue));
                return true;
            case StorageColumnType.Double:
                if (offset + sizeof(long) > rowBytes.Length)
                {
                    return false;
                }

                BinaryPrimitives.WriteInt64LittleEndian(
                    rowBytes.Slice(offset, sizeof(long)),
                    BitConverter.DoubleToInt64Bits(Convert.ToDouble(newValue)));
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
            case StorageColumnType.BigInt:
                if (newValue.Type is not (DataVoFixedWidthValueType.Int32 or DataVoFixedWidthValueType.Int64)
                    || offset + sizeof(long) > rowBytes.Length)
                {
                    return false;
                }

                BinaryPrimitives.WriteInt64LittleEndian(
                    rowBytes.Slice(offset, sizeof(long)),
                    newValue.Type == DataVoFixedWidthValueType.Int32 ? newValue.AsInt32() : newValue.AsInt64());
                return true;
            case StorageColumnType.Double:
                if (newValue.Type != DataVoFixedWidthValueType.Double || offset + sizeof(long) > rowBytes.Length)
                {
                    return false;
                }

                BinaryPrimitives.WriteInt64LittleEndian(
                    rowBytes.Slice(offset, sizeof(long)),
                    BitConverter.DoubleToInt64Bits(newValue.AsDouble()));
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
            case StorageColumnType.BigInt:
            case StorageColumnType.Double:
            case StorageColumnType.Date:
            case StorageColumnType.DateTime:
                offset += sizeof(long);
                break;
            case StorageColumnType.Guid:
                offset += 16;
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
    internal enum StorageColumnType : byte { Int, BigInt, Float, Double, Bit, Date, DateTime, Guid, Vector, String }

    internal static StorageColumnType ClassifyColumnType(string type) =>
        string.Equals(type, "INT", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Int :
        string.Equals(type, "BIGINT", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.BigInt :
        string.Equals(type, "FLOAT", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Float :
        string.Equals(type, "DOUBLE", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Double :
        string.Equals(type, "BIT", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Bit :
        string.Equals(type, "DATE", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Date :
        string.Equals(type, "DATETIME", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.DateTime :
        string.Equals(type, "GUID", StringComparison.OrdinalIgnoreCase) ? StorageColumnType.Guid :
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
            case StorageColumnType.BigInt:
                return CellValue.From(reader.ReadInt64());
            case StorageColumnType.Double:
                return CellValue.From(BitConverter.Int64BitsToDouble(reader.ReadInt64()));
            case StorageColumnType.Bit:
                return CellValue.From(reader.ReadBoolean());
            case StorageColumnType.Date:
                return CellValue.From(DataVoDate.FromDateTime(DateTime.FromBinary(reader.ReadInt64())));
            case StorageColumnType.Guid:
                return CellValue.From(reader.ReadGuid());
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

    /// <summary>
    /// Decodes the columns flagged in <paramref name="decode"/> into <paramref name="byOrdinal"/> at their own
    /// ordinals, skipping the rest without allocating. Cells not flagged are left untouched.
    /// </summary>
    public static void DecodeCellsByOrdinal(
        ReadOnlySpan<byte> data,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<bool> decode,
        Span<CellValue> byOrdinal)
    {
        var reader = new ByteSpanReader(data);
        int last = decode.LastIndexOf(true);
        for (int i = 0; i <= last; i++)
        {
            bool isNull = reader.ReadBoolean();
            if (decode[i])
            {
                byOrdinal[i] = isNull ? CellValue.Null : DecodeTypedCell(ref reader, columns[i]);
            }
            else if (!isNull)
            {
                SkipTypedCell(ref reader, columns[i]);
            }
        }
    }

    /// <summary>
    /// Decodes the single cell at <paramref name="ordinal"/> (storage order) of a serialized row, skipping
    /// the cells before it without allocating. Used to read a clustered table's key out of a row.
    /// </summary>
    public static CellValue ReadCell(ReadOnlySpan<byte> data, IReadOnlyList<Column> columns, int ordinal)
    {
        var reader = new ByteSpanReader(data);
        for (int i = 0; i < ordinal; i++)
        {
            if (!reader.ReadBoolean())
            {
                SkipTypedCell(ref reader, columns[i]);
            }
        }

        return reader.ReadBoolean() ? CellValue.Null : DecodeTypedCell(ref reader, columns[ordinal]);
    }

    /// <summary>
    /// Finds cell <paramref name="ordinal"/>: the offset of its null flag and the length of its value
    /// (0 for a null cell).
    /// </summary>
    private static (int FlagOffset, int ValueLength) LocateCell(ReadOnlySpan<byte> data, IReadOnlyList<Column> columns, int ordinal)
    {
        var reader = new ByteSpanReader(data);
        for (int i = 0; i < ordinal; i++)
        {
            if (!reader.ReadBoolean())
            {
                SkipTypedCell(ref reader, columns[i]);
            }
        }

        int flagOffset = reader.Position;
        if (reader.ReadBoolean())
        {
            return (flagOffset, 0);
        }

        SkipTypedCell(ref reader, columns[ordinal]);
        return (flagOffset, reader.Position - flagOffset - 1);
    }

    /// <summary>
    /// A copy of the row with cell <paramref name="ordinal"/> stored as NULL (its value bytes removed).
    /// Clustered tables store their key this way: the row's key already holds it.
    /// </summary>
    public static byte[] WithoutCell(ReadOnlySpan<byte> data, IReadOnlyList<Column> columns, int ordinal)
    {
        (int flagOffset, int valueLength) = LocateCell(data, columns, ordinal);
        if (valueLength == 0)
        {
            return data.ToArray();
        }

        var result = new byte[data.Length - valueLength];
        data[..flagOffset].CopyTo(result);
        result[flagOffset] = 1; // null
        data[(flagOffset + 1 + valueLength)..].CopyTo(result.AsSpan(flagOffset + 1));
        return result;
    }

    /// <summary>
    /// A copy of the row with cell <paramref name="ordinal"/> holding <paramref name="value"/> when it is
    /// stored as NULL (the inverse of <see cref="WithoutCell"/>); a row that still holds the cell is copied
    /// unchanged. <paramref name="value"/> is the cell's encoded value, as <see cref="SerializeCells"/> writes it.
    /// </summary>
    public static byte[] WithCell(ReadOnlySpan<byte> data, IReadOnlyList<Column> columns, int ordinal, ReadOnlySpan<byte> value)
    {
        (int flagOffset, int valueLength) = LocateCell(data, columns, ordinal);
        if (valueLength != 0 || data[flagOffset] == 0)
        {
            return data.ToArray();
        }

        var result = new byte[data.Length + value.Length];
        data[..flagOffset].CopyTo(result);
        result[flagOffset] = 0; // not null
        value.CopyTo(result.AsSpan(flagOffset + 1));
        data[(flagOffset + 1)..].CopyTo(result.AsSpan(flagOffset + 1 + value.Length));
        return result;
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
            case StorageColumnType.BigInt:
            case StorageColumnType.Double:
            case StorageColumnType.Date:
                reader.Skip(sizeof(long));
                return;
            case StorageColumnType.Guid:
                reader.Skip(16);
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
        if (value is DataVoBoundValue boundValue)
        {
            WriteTypedCell(writer, column, boundValue.Cell);
            return;
        }

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

        if (type == StorageColumnType.BigInt)
        {
            writer.Write(Convert.ToInt64(boxed));
            return;
        }

        if (type == StorageColumnType.Double)
        {
            writer.Write(BitConverter.DoubleToInt64Bits(Convert.ToDouble(boxed)));
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

        if (type == StorageColumnType.Guid)
        {
            Guid guid = boxed switch
            {
                Guid g => g,
                string s when Guid.TryParse(s, out Guid parsed) => parsed,
                _ => Guid.Parse(Convert.ToString(boxed, CultureInfo.InvariantCulture)!)
            };

            Span<byte> bytes = stackalloc byte[16];
            guid.TryWriteBytes(bytes);
            writer.Write(bytes);
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

        if (type == StorageColumnType.BigInt)
        {
            return reader.ReadInt64();
        }

        if (type == StorageColumnType.Double)
        {
            return BitConverter.Int64BitsToDouble(reader.ReadInt64());
        }

        if (type == StorageColumnType.Bit)
        {
            return reader.ReadBoolean();
        }

        if (type == StorageColumnType.Date)
        {
            return DataVoDate.FromDateTime(DateTime.FromBinary(reader.ReadInt64()));
        }

        if (type == StorageColumnType.DateTime)
        {
            return DateTime.FromBinary(reader.ReadInt64());
        }

        if (type == StorageColumnType.Guid)
        {
            return new Guid(reader.ReadBytes(16));
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
            case StorageColumnType.BigInt:
                writer.Write(cell.Type == CellType.Int32 ? cell.AsInt32() : cell.AsInt64());
                return;
            case StorageColumnType.Double:
                writer.Write(BitConverter.DoubleToInt64Bits(cell.AsDouble()));
                return;
            case StorageColumnType.Bit:
                writer.Write(cell.AsBoolean());
                return;
            case StorageColumnType.Date:
                writer.Write(cell.AsDate().ToDateTime().ToBinary());
                return;
            case StorageColumnType.Guid:
            {
                Span<byte> bytes = stackalloc byte[16];
                cell.AsGuid().TryWriteBytes(bytes);
                writer.Write(bytes);
                return;
            }
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
            case StorageColumnType.BigInt:
                return CellValue.From(reader.ReadInt64());
            case StorageColumnType.Double:
                return CellValue.From(BitConverter.Int64BitsToDouble(reader.ReadInt64()));
            case StorageColumnType.Bit:
                return CellValue.From(reader.ReadBoolean());
            case StorageColumnType.Date:
                return CellValue.From(DataVoDate.FromDateTime(DateTime.FromBinary(reader.ReadInt64())));
            case StorageColumnType.Guid:
                return CellValue.From(new Guid(reader.ReadBytes(16)));
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
        if (value is DataVoDate dataVoDate)
        {
            return dataVoDate.ToDateTime().ToBinary();
        }

        if (DateObjectAdapter.TryConvert(value, out DataVoDate adaptedDate))
        {
            return adaptedDate.ToDateTime().ToBinary();
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
