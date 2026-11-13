using System.Buffers.Binary;
using DataVo.Core.Transactions;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.Indexing.Columnar;

/// <summary>
/// Which columns of a table are kept in columnar form, and how to find them in a stored row: the stored type
/// of every column of the table, the table positions of the indexed columns, and the position of a clustered
/// table's key column.
/// </summary>
/// <remarks>
/// The LSM engine writes a copy into every SSTable that carries a columnar segment and keeps one in the table's
/// directory, so it knows what to write before any query has asked. Two specs with the same bytes describe the
/// same columns; a segment written for another spec is not used.
/// </remarks>
internal sealed class ColumnarSpec
{
    private const int FixedLength = 3 * sizeof(int);

    /// <summary>Creates a spec. The arrays are kept, not copied, and must not change afterwards.</summary>
    /// <param name="tableKinds">The stored type of every column of the table, in table order.</param>
    /// <param name="ordinals">The table positions of the indexed columns; position <c>n</c> is slot <c>n</c>.</param>
    /// <param name="keyOrdinal">The table position of a clustered table's key column, or -1.</param>
    public ColumnarSpec(StorageKind[] tableKinds, int[] ordinals, int keyOrdinal)
    {
        foreach (int ordinal in ordinals)
        {
            if ((uint)ordinal >= (uint)tableKinds.Length || !ColumnarIndex.Supports(tableKinds[ordinal]))
            {
                throw new ArgumentException($"Column {ordinal} cannot be in a columnar index.", nameof(ordinals));
            }
        }

        TableKinds = tableKinds;
        Ordinals = ordinals;
        KeyOrdinal = keyOrdinal;
        Span<byte> encoded = EncodedLength <= 512 ? stackalloc byte[EncodedLength] : new byte[EncodedLength];
        WriteTo(encoded);
        Hash = WalCrc32C.HashToUInt32(encoded);
    }

    /// <summary>The stored type of every column of the table, in table order.</summary>
    public StorageKind[] TableKinds { get; }

    /// <summary>The table positions of the indexed columns.</summary>
    public int[] Ordinals { get; }

    /// <summary>The table position of a clustered table's key column, or -1.</summary>
    public int KeyOrdinal { get; }

    /// <summary>A checksum of the encoded spec; equal specs have equal hashes.</summary>
    public uint Hash { get; }

    /// <summary>The number of indexed columns.</summary>
    public int ColumnCount => Ordinals.Length;

    /// <summary>The number of bytes <see cref="WriteTo"/> writes.</summary>
    public int EncodedLength => FixedLength + TableKinds.Length + (Ordinals.Length * sizeof(int));

    /// <summary>The stored type of the indexed column in a slot.</summary>
    public StorageKind KindOf(int slot) => TableKinds[Ordinals[slot]];

    /// <summary>Whether <paramref name="other"/> describes the same columns of the same table shape.</summary>
    public bool Matches(ColumnarSpec other) =>
        ReferenceEquals(this, other)
        || (Hash == other.Hash
            && KeyOrdinal == other.KeyOrdinal
            && TableKinds.AsSpan().SequenceEqual(other.TableKinds)
            && Ordinals.AsSpan().SequenceEqual(other.Ordinals));

    /// <summary>
    /// Writes the spec: <c>i32 key ordinal | i32 table columns | i32 indexed columns | u8 kind[table columns] |
    /// i32 ordinal[indexed columns]</c>, little-endian.
    /// </summary>
    public void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination, KeyOrdinal);
        BinaryPrimitives.WriteInt32LittleEndian(destination[4..], TableKinds.Length);
        BinaryPrimitives.WriteInt32LittleEndian(destination[8..], Ordinals.Length);
        int at = FixedLength;
        foreach (StorageKind kind in TableKinds)
        {
            destination[at++] = (byte)kind;
        }

        foreach (int ordinal in Ordinals)
        {
            BinaryPrimitives.WriteInt32LittleEndian(destination[at..], ordinal);
            at += sizeof(int);
        }
    }

    /// <summary>Reads a spec written by <see cref="WriteTo"/>.</summary>
    /// <exception cref="InvalidDataException">The bytes are not a whole, valid spec.</exception>
    public static ColumnarSpec ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < FixedLength)
        {
            throw new InvalidDataException("Columnar spec is truncated.");
        }

        int keyOrdinal = BinaryPrimitives.ReadInt32LittleEndian(source);
        int tableColumns = BinaryPrimitives.ReadInt32LittleEndian(source[4..]);
        int columns = BinaryPrimitives.ReadInt32LittleEndian(source[8..]);
        if (tableColumns < 0 || columns < 0 || keyOrdinal < -1 || keyOrdinal >= tableColumns
            || source.Length != FixedLength + (long)tableColumns + ((long)columns * sizeof(int)))
        {
            throw new InvalidDataException("Columnar spec has an invalid shape.");
        }

        var kinds = new StorageKind[tableColumns];
        for (int i = 0; i < tableColumns; i++)
        {
            kinds[i] = (StorageKind)source[FixedLength + i];
        }

        var ordinals = new int[columns];
        for (int i = 0; i < columns; i++)
        {
            ordinals[i] = BinaryPrimitives.ReadInt32LittleEndian(source[(FixedLength + tableColumns + (i * sizeof(int)))..]);
        }

        try
        {
            return new ColumnarSpec(kinds, ordinals, keyOrdinal);
        }
        catch (ArgumentException ex)
        {
            throw new InvalidDataException("Columnar spec names a column that cannot be indexed.", ex);
        }
    }
}
