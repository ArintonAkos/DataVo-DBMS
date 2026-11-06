using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// Where a clustered table's key lives in its serialized rows, and how the key becomes the row's
/// <see cref="RowKey"/>: an INT key is the integer, a GUID key its 16 bytes.
/// </summary>
internal sealed class ClusteredKeyLayout(IReadOnlyList<Column> columns, int ordinal, string primaryKeyIndexName)
{
    public IReadOnlyList<Column> Columns { get; } = columns;

    /// <summary>Storage-order position of the key column.</summary>
    public int Ordinal { get; } = ordinal;

    /// <summary>The table's primary-key index name: served from the rows themselves, never written.</summary>
    public string PrimaryKeyIndexName { get; } = primaryKeyIndexName;

    /// <summary>The row key of a serialized row: its key column. A row without a key cannot be stored.</summary>
    public RowKey KeyOf(ReadOnlySpan<byte> rowBytes, string tableName)
    {
        CellValue cell = RowSerializer.ReadCell(rowBytes, Columns, Ordinal);
        return cell.Type switch
        {
            CellType.Int32 => cell.AsInt32(),
            CellType.Int64 => cell.AsInt64(),
            CellType.Guid => RowKey.FromGuid(cell.AsGuid()),
            _ when cell.IsNull => throw new ConstraintViolationException($"Primary key cannot be null in table {tableName}."),
            _ => throw new InvalidOperationException($"Clustered table {tableName} has a key of unsupported type {cell.Type}."),
        };
    }

    /// <summary>
    /// The row key an encoded index key (<see cref="LsmIndexKeyCodec"/>) of this table's primary key names,
    /// or <see langword="null"/> when the encoded key is not an integer or GUID (no row can have it).
    /// </summary>
    public static RowKey? KeyOfIndexKey(ReadOnlySpan<byte> encodedKey)
    {
        if (encodedKey.Length == 1 + sizeof(long) && encodedKey[0] == LsmIndexKeyCodec.IntegerTag)
        {
            return new RowKey(0, System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(encodedKey[1..]));
        }

        if (encodedKey.Length == 1 + 16 && encodedKey[0] == LsmIndexKeyCodec.GuidTag)
        {
            return RowKey.FromGuid(new Guid(encodedKey[1..].ToArray()));
        }

        return null;
    }

    /// <summary>The encoded index key (<see cref="LsmIndexKeyCodec"/>) of a row key; the inverse of <see cref="KeyOfIndexKey"/>.</summary>
    public byte[] IndexKeyOf(RowKey key) => Columns[Ordinal].Type.Equals("GUID", StringComparison.OrdinalIgnoreCase)
        ? LsmIndexKeyCodec.EncodeGuid(key.AsGuid())
        : LsmIndexKeyCodec.EncodeInteger(key.AsInt64());
}
