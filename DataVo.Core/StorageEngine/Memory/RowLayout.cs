using System.Runtime.CompilerServices;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// Where one column of a table lives in a packed in-memory row: inside the row's fixed-width record, or in the
/// row's run of object references.
/// </summary>
internal readonly struct RowField
{
    /// <summary>Describes one column.</summary>
    /// <param name="type">The type of the column's non-null cells.</param>
    /// <param name="position">The byte offset in the record, or the reference slot, of the value.</param>
    /// <param name="width">The value's width in bytes; 0 for a reference column.</param>
    public RowField(CellType type, int position, int width)
    {
        Type = type;
        Position = position;
        Width = width;
    }

    /// <summary>The type of the column's non-null cells.</summary>
    public CellType Type { get; }

    /// <summary>
    /// For a fixed-width column, the byte offset of the value in the record. For a reference column, the index
    /// of the value among the row's references.
    /// </summary>
    public int Position { get; }

    /// <summary>The value's width in bytes; 0 for a reference column.</summary>
    public int Width { get; }

    /// <summary>Whether the value is an object reference (a string or a vector) kept beside the record.</summary>
    public bool IsReference => Width == 0;
}

/// <summary>
/// The shape of a table's packed in-memory rows. A row is one fixed-width record holding its numbers, dates,
/// flags and identifiers, plus one object reference for each string or vector column. Records of one table are
/// all the same length, so they can sit end to end in a <see cref="DataVo.Core.Memory.Flat.FlatChunk"/>.
/// </summary>
/// <remarks>
/// A record is laid out as: the fixed-width values, widest first; one status byte; one null bit per column;
/// padding up to a multiple of eight bytes. Widest-first order together with that padding puts every value on
/// a multiple of its own width, so no value is read across an alignment boundary.
/// <para>
/// The status byte of a chunk's untouched slots is zero, which reads as <em>no row</em>: a slot becomes a row
/// only when <see cref="MarkLive"/> is called on it.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// // Id INT, Active BIT, Gold BIGINT, Name VARCHAR
/// RowLayout layout = RowLayout.Create([CellType.Int32, CellType.Boolean, CellType.Int64, CellType.String]);
/// // Gold at 0, Id at 8, Active at 12, status at 13, null bits at 14; RecordBytes is 16.
/// // Name is reference 0.
/// </code>
/// </example>
internal sealed class RowLayout
{
    private const int RecordAlignment = 8;
    private const byte StatusEmpty = 0;
    private const byte StatusPacked = 1;
    private const byte StatusKeptApart = 2;

    private readonly RowField[] _fields;
    private readonly int _nullsOffset;

    private RowLayout(RowField[] fields, int statusOffset, int referenceCount)
    {
        _fields = fields;
        StatusOffset = statusOffset;
        _nullsOffset = statusOffset + 1;
        ReferenceCount = referenceCount;
        int used = _nullsOffset + ((fields.Length + 7) >> 3);
        RecordBytes = (used + RecordAlignment - 1) & ~(RecordAlignment - 1);
    }

    /// <summary>The number of columns.</summary>
    public int ColumnCount => _fields.Length;

    /// <summary>The length of one record in bytes: a multiple of eight.</summary>
    public int RecordBytes { get; }

    /// <summary>The number of object references a row has: one for each string or vector column.</summary>
    public int ReferenceCount { get; }

    /// <summary>The byte offset of the record's status byte.</summary>
    public int StatusOffset { get; }

    /// <summary>Where the column at <paramref name="ordinal"/> lives.</summary>
    public ref readonly RowField this[int ordinal] => ref _fields[ordinal];

    /// <summary>Lays out a table's rows.</summary>
    /// <param name="columnTypes">The type of each column's non-null cells, in column order.</param>
    /// <exception cref="ArgumentException">
    /// There are no columns, or a column's type is <see cref="CellType.Null"/>, which is not a column type.
    /// </exception>
    public static RowLayout Create(ReadOnlySpan<CellType> columnTypes)
    {
        if (columnTypes.IsEmpty)
        {
            throw new ArgumentException("A row layout needs at least one column.", nameof(columnTypes));
        }

        var fields = new RowField[columnTypes.Length];
        int fixedBytes = PlaceFixedColumns(columnTypes, fields);
        int references = PlaceReferenceColumns(columnTypes, fields);
        return new RowLayout(fields, fixedBytes, references);
    }

    /// <summary>The width in bytes of a column type's values in a record; 0 when they are kept as references.</summary>
    /// <exception cref="ArgumentException">The type is not a column type.</exception>
    public static int WidthOf(CellType type) => type switch
    {
        CellType.Boolean => 1,
        CellType.Int32 or CellType.Date => 4,
        CellType.Int64 or CellType.Double => 8,
        CellType.Decimal or CellType.Guid => 16,
        CellType.String or CellType.Vector => 0,
        _ => throw new ArgumentException($"'{type}' is not a column type.", nameof(type)),
    };

    /// <summary>Whether a record holds a row, packed or kept apart.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsLive(ReadOnlySpan<byte> record) => record[StatusOffset] != StatusEmpty;

    /// <summary>
    /// Whether a record stands for a row whose cells are kept apart from it, because one of them is not of its
    /// column's type. Such a record holds no values.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsKeptApart(ReadOnlySpan<byte> record) => record[StatusOffset] == StatusKeptApart;

    /// <summary>Marks a record as holding a packed row. Called last, once the record's values are in place.</summary>
    public void MarkLive(Span<byte> record) => record[StatusOffset] = StatusPacked;

    /// <summary>Marks a record as standing for a row whose cells are kept apart from it.</summary>
    public void MarkKeptApart(Span<byte> record) => record[StatusOffset] = StatusKeptApart;

    /// <summary>
    /// Marks a record as holding no row. Its values are left as they are, and readers that were already
    /// looking at the record see the change.
    /// </summary>
    public void MarkEmpty(Span<byte> record) => Volatile.Write(ref record[StatusOffset], StatusEmpty);

    /// <summary>Whether the column at <paramref name="ordinal"/> is NULL in a record.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsNull(ReadOnlySpan<byte> record, int ordinal) =>
        (record[_nullsOffset + (ordinal >> 3)] & (1 << (ordinal & 7))) != 0;

    /// <summary>Records that the column at <paramref name="ordinal"/> is NULL.</summary>
    public void SetNull(Span<byte> record, int ordinal) =>
        record[_nullsOffset + (ordinal >> 3)] |= (byte)(1 << (ordinal & 7));

    /// <summary>
    /// Gives each fixed-width column its offset, widest columns first and columns of one width in column order.
    /// </summary>
    /// <returns>The number of bytes the fixed-width values take.</returns>
    private static int PlaceFixedColumns(ReadOnlySpan<CellType> columnTypes, RowField[] fields)
    {
        int offset = 0;
        for (int width = 16; width > 0; width >>= 1)
        {
            for (int ordinal = 0; ordinal < columnTypes.Length; ordinal++)
            {
                if (WidthOf(columnTypes[ordinal]) == width)
                {
                    fields[ordinal] = new RowField(columnTypes[ordinal], offset, width);
                    offset += width;
                }
            }
        }

        return offset;
    }

    /// <summary>Gives each reference column its slot, in column order.</summary>
    /// <returns>The number of reference columns.</returns>
    private static int PlaceReferenceColumns(ReadOnlySpan<CellType> columnTypes, RowField[] fields)
    {
        int slot = 0;
        for (int ordinal = 0; ordinal < columnTypes.Length; ordinal++)
        {
            if (WidthOf(columnTypes[ordinal]) == 0)
            {
                fields[ordinal] = new RowField(columnTypes[ordinal], slot++, 0);
            }
        }

        return slot;
    }
}
