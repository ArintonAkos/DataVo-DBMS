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

    public void Clear() => _operations.Clear();

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

            if (kind is not (LsmCommitOperationKind.Put or LsmCommitOperationKind.Delete)
                || databaseBytes == 0
                || tableBytes == 0
                || rowId < 0
                || valueBytes < 0
                || (kind == LsmCommitOperationKind.Delete && valueBytes != 0)
                || (long)databaseBytes + tableBytes + valueBytes > payload.Length - offset)
            {
                return false;
            }

            if (!TryDecodeName(payload.Slice(offset, databaseBytes), out string database)
                || !TryDecodeName(payload.Slice(offset + databaseBytes, tableBytes), out string table))
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
