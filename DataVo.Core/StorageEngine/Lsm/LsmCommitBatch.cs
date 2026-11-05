using System.Buffers.Binary;
using System.Text;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>The kind of one mutation inside an Lsm commit batch.</summary>
internal enum LsmCommitOperationKind : byte
{
    /// <summary>Stores <c>Value</c> as the current version of the row.</summary>
    Put = 1,

    /// <summary>Removes the row.</summary>
    Delete = 2,

    /// <summary>Drops a whole table; replay discards it unless a newer copy is already on disk.</summary>
    DropTable = 3,

    /// <summary>Drops every table of a database (the operation's table name is empty).</summary>
    DropDatabase = 4,

    /// <summary>Adds an index entry (index name and key in <c>Value</c>, see <see cref="LsmCommitBatch.IndexPut"/>).</summary>
    IndexPut = 5,

    /// <summary>Removes an index entry.</summary>
    IndexDelete = 6,
}

/// <summary>One row mutation recorded in the commit log.</summary>
internal readonly record struct LsmCommitOperation(
    LsmCommitOperationKind Kind,
    string Database,
    string Table,
    long RowId,
    byte[] Value);

/// <summary>
/// Every storage mutation of one write unit (an autocommit statement or a committed transaction). The
/// commit log stores a batch as a single checksummed frame, so recovery applies all of it or none.
/// </summary>
internal sealed class LsmCommitBatch
{
    private readonly List<LsmCommitOperation> _operations = [];

    public IReadOnlyList<LsmCommitOperation> Operations => _operations;

    public int Count => _operations.Count;

    public bool IsEmpty => _operations.Count == 0;

    /// <summary>Records that <paramref name="value"/> becomes the row's current version. The batch keeps the array.</summary>
    public void Put(string database, string table, long rowId, byte[] value)
    {
        Validate(database, table, rowId);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(value);
        _operations.Add(new LsmCommitOperation(LsmCommitOperationKind.Put, database, table, rowId, value));
    }

    /// <summary>Records that the row is removed.</summary>
    public void Delete(string database, string table, long rowId)
    {
        Validate(database, table, rowId);
        _operations.Add(new LsmCommitOperation(LsmCommitOperationKind.Delete, database, table, rowId, []));
    }

    /// <summary>Records that a table is dropped.</summary>
    public void DropTable(string database, string table)
    {
        Validate(database, table, 0);
        _operations.Add(new LsmCommitOperation(LsmCommitOperationKind.DropTable, database, table, 0, []));
    }

    /// <summary>Records that a whole database is dropped.</summary>
    public void DropDatabase(string database)
    {
        if (string.IsNullOrEmpty(database))
        {
            throw new ArgumentException("A commit operation needs a database name.", nameof(database));
        }

        _operations.Add(new LsmCommitOperation(LsmCommitOperationKind.DropDatabase, database, string.Empty, 0, []));
    }

    /// <summary>
    /// Records that index <paramref name="index"/> of the table maps <paramref name="encodedKey"/> to
    /// <paramref name="rowId"/>. Written in the same frame as the row change, so both survive a crash or
    /// neither does. Value layout: u16 index name bytes | index name (UTF-8) | encoded key.
    /// </summary>
    public void IndexPut(string database, string table, string index, ReadOnlySpan<byte> encodedKey, long rowId)
    {
        Validate(database, table, rowId);
        _operations.Add(new LsmCommitOperation(LsmCommitOperationKind.IndexPut, database, table, rowId, EncodeIndexValue(index, encodedKey)));
    }

    /// <summary>Records that the index entry (key, row id) is removed.</summary>
    public void IndexDelete(string database, string table, string index, ReadOnlySpan<byte> encodedKey, long rowId)
    {
        Validate(database, table, rowId);
        _operations.Add(new LsmCommitOperation(LsmCommitOperationKind.IndexDelete, database, table, rowId, EncodeIndexValue(index, encodedKey)));
    }

    /// <summary>Index name and encoded key of an <see cref="LsmCommitOperationKind.IndexPut"/>/<see cref="LsmCommitOperationKind.IndexDelete"/>.</summary>
    public static (string Index, byte[] EncodedKey) DecodeIndexValue(byte[] value)
    {
        if (!TryDecodeIndexValue(value, out string index, out byte[] key))
        {
            throw new InvalidDataException("Index operation value is malformed.");
        }

        return (index, key);
    }

    internal static bool TryDecodeIndexValue(ReadOnlySpan<byte> value, out string index, out byte[] encodedKey)
    {
        index = string.Empty;
        encodedKey = [];
        if (value.Length < sizeof(ushort))
        {
            return false;
        }

        int nameBytes = BinaryPrimitives.ReadUInt16LittleEndian(value);
        if (nameBytes == 0 || value.Length - sizeof(ushort) - nameBytes < 1)
        {
            return false;
        }

        try
        {
            index = new UTF8Encoding(false, true).GetString(value.Slice(sizeof(ushort), nameBytes));
        }
        catch (DecoderFallbackException)
        {
            return false;
        }

        encodedKey = value[(sizeof(ushort) + nameBytes)..].ToArray();
        return true;
    }

    private static byte[] EncodeIndexValue(string index, ReadOnlySpan<byte> encodedKey)
    {
        if (string.IsNullOrEmpty(index))
        {
            throw new ArgumentException("An index operation needs an index name.", nameof(index));
        }

        if (encodedKey.IsEmpty)
        {
            throw new ArgumentException("An index operation needs a key.", nameof(encodedKey));
        }

        int nameBytes = Encoding.UTF8.GetByteCount(index);
        var value = new byte[sizeof(ushort) + nameBytes + encodedKey.Length];
        BinaryPrimitives.WriteUInt16LittleEndian(value, checked((ushort)nameBytes));
        Encoding.UTF8.GetBytes(index, 0, index.Length, value, sizeof(ushort));
        encodedKey.CopyTo(value.AsSpan(sizeof(ushort) + nameBytes));
        return value;
    }

    public void Clear() => _operations.Clear();

    /// <summary>
    /// Keeps only the last put or delete per row, in the order those last operations occurred. Every
    /// mutation of a batch shares one sequence number, so two versions of the same row would be ambiguous.
    /// </summary>
    public void CollapseToLatestPerRow()
    {
        if (_operations.Count < 2)
        {
            return;
        }

        var lastIndex = new Dictionary<CollapseKey, int>(CollapseKeyComparer.Instance);
        bool hasDuplicates = false;
        for (int i = 0; i < _operations.Count; i++)
        {
            if (TryGetCollapseKey(_operations[i], out CollapseKey key))
            {
                hasDuplicates |= lastIndex.ContainsKey(key);
                lastIndex[key] = i;
            }
        }

        if (!hasDuplicates)
        {
            return;
        }

        var collapsed = new List<LsmCommitOperation>(lastIndex.Count);
        for (int i = 0; i < _operations.Count; i++)
        {
            LsmCommitOperation operation = _operations[i];
            if (!TryGetCollapseKey(operation, out CollapseKey key) || lastIndex[key] == i)
            {
                collapsed.Add(operation);
            }
        }

        _operations.Clear();
        _operations.AddRange(collapsed);
    }

    // Rows collapse per (database, table, row id); index entries per (database, table, index and key, row id).
    private readonly record struct CollapseKey(string Database, string Table, long RowId, byte[]? IndexValue);

    private static bool TryGetCollapseKey(LsmCommitOperation operation, out CollapseKey key)
    {
        switch (operation.Kind)
        {
            case LsmCommitOperationKind.Put or LsmCommitOperationKind.Delete:
                key = new CollapseKey(operation.Database, operation.Table, operation.RowId, null);
                return true;
            case LsmCommitOperationKind.IndexPut or LsmCommitOperationKind.IndexDelete:
                key = new CollapseKey(operation.Database, operation.Table, operation.RowId, operation.Value);
                return true;
            default:
                key = default;
                return false;
        }
    }

    private sealed class CollapseKeyComparer : IEqualityComparer<CollapseKey>
    {
        public static readonly CollapseKeyComparer Instance = new();

        public bool Equals(CollapseKey x, CollapseKey y) =>
            x.RowId == y.RowId
            && string.Equals(x.Table, y.Table, StringComparison.Ordinal)
            && string.Equals(x.Database, y.Database, StringComparison.Ordinal)
            && (x.IndexValue is null ? y.IndexValue is null : y.IndexValue is not null && x.IndexValue.AsSpan().SequenceEqual(y.IndexValue));

        public int GetHashCode(CollapseKey key)
        {
            int hash = HashCode.Combine(key.Database, key.Table, key.RowId);
            if (key.IndexValue is { } value)
            {
                foreach (byte b in value)
                {
                    hash = (hash * 31) + b;
                }
            }

            return hash;
        }
    }

    private static void Validate(string database, string table, long rowId)
    {
        if (string.IsNullOrEmpty(database))
        {
            throw new ArgumentException("A commit operation needs a database name.", nameof(database));
        }

        if (string.IsNullOrEmpty(table))
        {
            throw new ArgumentException("A commit operation needs a table name.", nameof(table));
        }

        if (rowId < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(rowId), "Row ids are never negative.");
        }
    }
}

/// <summary>
/// Payload format of an Lsm commit-log frame (version 1, little-endian):
/// <code>
/// u16 version | u16 reserved | i32 operationCount
/// per operation: u8 kind | u8 reserved | u16 databaseBytes | u16 tableBytes | i64 rowId | i32 valueBytes
///                | database (UTF-8) | table (UTF-8) | value
/// </code>
/// Decoding is strict: any length or kind that does not add up exactly makes the payload invalid.
/// </summary>
internal static class LsmCommitBatchCodec
{
    private const ushort CurrentVersion = 1;
    private const int BatchHeaderSize = 8;
    private const int OperationHeaderSize = 18;

    /// <summary>Byte offset of the first operation's kind byte (for format tests).</summary>
    internal const int FirstOperationOffset = BatchHeaderSize;

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static int MeasureSize(LsmCommitBatch batch)
    {
        long size = BatchHeaderSize;
        foreach (LsmCommitOperation operation in batch.Operations)
        {
            size += OperationHeaderSize
                + MeasureName(operation.Database)
                + MeasureName(operation.Table)
                + operation.Value.Length;
        }

        return checked((int)size);
    }

    public static int Write(Span<byte> destination, LsmCommitBatch batch)
    {
        int size = MeasureSize(batch);
        if (destination.Length < size)
        {
            throw new ArgumentException("Destination is too small for the commit batch.", nameof(destination));
        }

        BinaryPrimitives.WriteUInt16LittleEndian(destination, CurrentVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[2..], 0);
        BinaryPrimitives.WriteInt32LittleEndian(destination[4..], batch.Count);
        int offset = BatchHeaderSize;

        foreach (LsmCommitOperation operation in batch.Operations)
        {
            Span<byte> header = destination.Slice(offset, OperationHeaderSize);
            int databaseBytes = StrictUtf8.GetByteCount(operation.Database);
            int tableBytes = StrictUtf8.GetByteCount(operation.Table);
            header[0] = (byte)operation.Kind;
            header[1] = 0;
            BinaryPrimitives.WriteUInt16LittleEndian(header[2..], (ushort)databaseBytes);
            BinaryPrimitives.WriteUInt16LittleEndian(header[4..], (ushort)tableBytes);
            BinaryPrimitives.WriteInt64LittleEndian(header[6..], operation.RowId);
            BinaryPrimitives.WriteInt32LittleEndian(header[14..], operation.Value.Length);
            offset += OperationHeaderSize;

            offset += StrictUtf8.GetBytes(operation.Database, destination[offset..]);
            offset += StrictUtf8.GetBytes(operation.Table, destination[offset..]);
            operation.Value.CopyTo(destination[offset..]);
            offset += operation.Value.Length;
        }

        return offset;
    }

    public static bool TryRead(ReadOnlySpan<byte> payload, out List<LsmCommitOperation> operations)
    {
        operations = [];
        if (payload.Length < BatchHeaderSize
            || BinaryPrimitives.ReadUInt16LittleEndian(payload) != CurrentVersion)
        {
            return false;
        }

        int count = BinaryPrimitives.ReadInt32LittleEndian(payload[4..]);
        if (count <= 0 || count > (payload.Length - BatchHeaderSize) / OperationHeaderSize)
        {
            return false;
        }

        operations = new List<LsmCommitOperation>(count);
        int offset = BatchHeaderSize;
        for (int i = 0; i < count; i++)
        {
            if (payload.Length - offset < OperationHeaderSize)
            {
                return false;
            }

            ReadOnlySpan<byte> header = payload.Slice(offset, OperationHeaderSize);
            var kind = (LsmCommitOperationKind)header[0];
            int databaseBytes = BinaryPrimitives.ReadUInt16LittleEndian(header[2..]);
            int tableBytes = BinaryPrimitives.ReadUInt16LittleEndian(header[4..]);
            long rowId = BinaryPrimitives.ReadInt64LittleEndian(header[6..]);
            int valueBytes = BinaryPrimitives.ReadInt32LittleEndian(header[14..]);
            offset += OperationHeaderSize;

            bool isRowOperation = kind is LsmCommitOperationKind.Put or LsmCommitOperationKind.Delete;
            bool isIndexOperation = kind is LsmCommitOperationKind.IndexPut or LsmCommitOperationKind.IndexDelete;
            if (!Enum.IsDefined(typeof(LsmCommitOperationKind), kind)
                || databaseBytes == 0
                || (tableBytes == 0) != (kind == LsmCommitOperationKind.DropDatabase)
                || rowId < 0
                || valueBytes < 0
                || (kind != LsmCommitOperationKind.Put && !isIndexOperation && valueBytes != 0)
                || (isIndexOperation && !LsmCommitBatch.TryDecodeIndexValue(payload.Slice(offset + databaseBytes + tableBytes, Math.Max(0, Math.Min(valueBytes, payload.Length - offset - databaseBytes - tableBytes))), out _, out _))
                || (!isRowOperation && !isIndexOperation && rowId != 0)
                || (long)databaseBytes + tableBytes + valueBytes > payload.Length - offset)
            {
                return false;
            }

            string table = string.Empty;
            if (!TryDecodeName(payload.Slice(offset, databaseBytes), out string database)
                || (tableBytes > 0 && !TryDecodeName(payload.Slice(offset + databaseBytes, tableBytes), out table)))
            {
                return false;
            }

            offset += databaseBytes + tableBytes;
            byte[] value = payload.Slice(offset, valueBytes).ToArray();
            offset += valueBytes;
            operations.Add(new LsmCommitOperation(kind, database, table, rowId, value));
        }

        return offset == payload.Length;
    }

    private static int MeasureName(string name)
    {
        int bytes = StrictUtf8.GetByteCount(name);
        if (bytes > ushort.MaxValue)
        {
            throw new ArgumentException($"Name '{name[..32]}...' is too long for the commit log.", nameof(name));
        }

        return bytes;
    }

    private static bool TryDecodeName(ReadOnlySpan<byte> bytes, out string name)
    {
        try
        {
            name = StrictUtf8.GetString(bytes);
            return true;
        }
        catch (DecoderFallbackException)
        {
            name = string.Empty;
            return false;
        }
    }
}
