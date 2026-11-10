using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using DataVo.Core.StorageEngine.Serialization;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core;

/// <summary>
/// One row of a <see cref="RowRefStream"/>, read straight from the stored bytes. Nothing is copied or
/// allocated: each accessor decodes one cell where it lies.
/// </summary>
/// <remarks>
/// <para>
/// Column indexes follow the table's column order (the order of <c>CREATE TABLE</c>, and of the properties of a
/// <c>[DataVoTable]</c> model). <see cref="RowRefStream.GetOrdinal"/> finds one by name.
/// </para>
/// <para>
/// A row is valid only inside the loop iteration that produced it: the stream reuses its memory for the next
/// page. Copy any value you need to keep. Spans from <see cref="GetStringSpan"/> and <see cref="GetStringUtf8"/>
/// follow the same rule.
/// </para>
/// <para>
/// Each accessor requires the column's own type, with lossless widening allowed (<c>INT</c> through
/// <see cref="GetInt64"/>, <c>FLOAT</c> through <see cref="GetDouble"/>). Any other type throws
/// <see cref="InvalidCastException"/>; a NULL cell throws <see cref="InvalidOperationException"/>, so check
/// <see cref="IsNull"/> first for nullable columns.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// foreach (RowRef row in db.Table&lt;Player&gt;().StreamRef())
/// {
///     int id = row.GetInt32(0);
///     float health = row.GetFloat(1);
/// }
/// </code>
/// </example>
public readonly ref struct RowRef
{
    private readonly ReadOnlySpan<byte> _row;
    private readonly ReadOnlySpan<int> _cells;
    private readonly ReadOnlySpan<StorageKind> _kinds;
    private readonly Span<char> _chars;
    private readonly RowKey _key;
    private readonly int _keyOrdinal;
    private readonly bool _keyIsGuid;

    internal RowRef(
        ReadOnlySpan<byte> row,
        ReadOnlySpan<int> cells,
        ReadOnlySpan<StorageKind> kinds,
        Span<char> chars,
        RowKey key,
        int keyOrdinal,
        bool keyIsGuid)
    {
        _row = row;
        _cells = cells;
        _kinds = kinds;
        _chars = chars;
        _key = key;
        _keyOrdinal = keyOrdinal;
        _keyIsGuid = keyIsGuid;
    }

    /// <summary>Gets the number of columns in the row.</summary>
    public int ColumnCount => _kinds.Length;

    /// <summary>Returns whether a column holds NULL.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns><see langword="true"/> when the cell is NULL.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="columnIndex"/> is not a column of the row.</exception>
    public bool IsNull(int columnIndex)
    {
        CheckIndex(columnIndex);
        return IsStoredNull(columnIndex) && !IsKeyColumn(columnIndex);
    }

    /// <summary>Reads an <c>INT</c> column.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The value.</returns>
    public int GetInt32(int columnIndex)
    {
        if (TryKey(columnIndex, StorageKind.Int))
        {
            return checked((int)_key.AsInt64());
        }

        return BinaryPrimitives.ReadInt32LittleEndian(Value(columnIndex, StorageKind.Int));
    }

    /// <summary>Reads a <c>BIGINT</c> or <c>INT</c> column.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The value.</returns>
    public long GetInt64(int columnIndex)
    {
        StorageKind kind = KindOf(columnIndex);
        if (kind != StorageKind.Int && kind != StorageKind.BigInt)
        {
            throw Mismatch(columnIndex, "an integer");
        }

        if (TryKey(columnIndex, kind))
        {
            return _key.AsInt64();
        }

        ReadOnlySpan<byte> value = Value(columnIndex, kind);
        return kind == StorageKind.Int ? BinaryPrimitives.ReadInt32LittleEndian(value) : BinaryPrimitives.ReadInt64LittleEndian(value);
    }

    /// <summary>Reads a <c>FLOAT</c> column.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The value.</returns>
    public float GetFloat(int columnIndex) =>
        BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(Value(columnIndex, StorageKind.Float)));

    /// <summary>Reads a <c>DOUBLE</c> or <c>FLOAT</c> column.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The value.</returns>
    public double GetDouble(int columnIndex)
    {
        StorageKind kind = KindOf(columnIndex);
        if (kind == StorageKind.Float)
        {
            return GetFloat(columnIndex);
        }

        return BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(Value(columnIndex, StorageKind.Double)));
    }

    /// <summary>Reads a <c>BIT</c> column.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The value.</returns>
    public bool GetBoolean(int columnIndex) => Value(columnIndex, StorageKind.Bit)[0] != 0;

    /// <summary>Reads a <c>GUID</c> column.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The value.</returns>
    public Guid GetGuid(int columnIndex)
    {
        if (TryKey(columnIndex, StorageKind.Guid))
        {
            return _key.AsGuid();
        }

        return new Guid(Value(columnIndex, StorageKind.Guid).Slice(0, 16));
    }

    /// <summary>
    /// Reads a text column as characters. The span stays valid for the rest of the loop iteration, alongside the
    /// spans of the row's other text columns.
    /// </summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The text.</returns>
    public ReadOnlySpan<char> GetStringSpan(int columnIndex)
    {
        ReadOnlySpan<byte> utf8 = GetStringUtf8(columnIndex);
        if (utf8.IsEmpty)
        {
            return default;
        }

        // A string decodes into the characters that line up with its own bytes: UTF-8 never takes fewer bytes than
        // UTF-16 takes characters, so every text cell of the page has a slot of its own.
        int start = _cells[columnIndex];
        Span<char> slot = _chars.Slice(start, _row.Length - start);
        int written = Encoding.UTF8.GetChars(utf8, slot);
        return slot.Slice(0, written);
    }

    /// <summary>Reads a text column as its stored UTF-8 bytes, without decoding them.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The UTF-8 bytes.</returns>
    public ReadOnlySpan<byte> GetStringUtf8(int columnIndex)
    {
        ReadOnlySpan<byte> value = Value(columnIndex, StorageKind.String);
        int length = 0;
        int shift = 0;
        int prefix = 0;
        byte current;
        do
        {
            current = value[prefix++];
            length |= (current & 0x7F) << shift;
            shift += 7;
        }
        while ((current & 0x80) != 0);

        return value.Slice(prefix, length);
    }

    /// <summary>Reads a three-component <c>VECTOR</c> column, such as a <see cref="Vector3"/> property's.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The value.</returns>
    /// <exception cref="InvalidCastException">The vector does not have exactly three components.</exception>
    public Vector3 GetVector3(int columnIndex)
    {
        ReadOnlySpan<byte> value = Value(columnIndex, StorageKind.Vector);
        if (BinaryPrimitives.ReadInt32LittleEndian(value) != 3)
        {
            throw new InvalidCastException($"Column {columnIndex} holds a vector of {BinaryPrimitives.ReadInt32LittleEndian(value)} components, not 3.");
        }

        return new Vector3(Single(value, 4), Single(value, 8), Single(value, 12));
    }

    /// <summary>Reads a <c>VECTOR</c> column's components as stored: little-endian 32-bit floats.</summary>
    /// <param name="columnIndex">The column's position in the table.</param>
    /// <returns>The component bytes; four per component.</returns>
    public ReadOnlySpan<byte> GetVectorBytes(int columnIndex)
    {
        ReadOnlySpan<byte> value = Value(columnIndex, StorageKind.Vector);
        return value.Slice(sizeof(int), BinaryPrimitives.ReadInt32LittleEndian(value) * sizeof(float));
    }

    private static float Single(ReadOnlySpan<byte> value, int offset) =>
        BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(value.Slice(offset, sizeof(float))));

    private bool IsStoredNull(int columnIndex)
    {
        int flag = _cells[columnIndex];
        return flag == RowSerializer.AbsentCell || _row[flag] != 0;
    }

    // Clustered tables keep the primary key in the row key and store its cell as NULL.
    private bool IsKeyColumn(int columnIndex) => columnIndex == _keyOrdinal;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryKey(int columnIndex, StorageKind kind)
    {
        if (_keyOrdinal < 0 || columnIndex != _keyOrdinal || !IsStoredNull(columnIndex))
        {
            return false;
        }

        if (KindOf(columnIndex) != kind || (kind == StorageKind.Guid) != _keyIsGuid)
        {
            throw Mismatch(columnIndex, NameOf(kind));
        }

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> Value(int columnIndex, StorageKind kind)
    {
        if (KindOf(columnIndex) != kind)
        {
            throw Mismatch(columnIndex, NameOf(kind));
        }

        if (IsStoredNull(columnIndex))
        {
            throw NullCell(columnIndex);
        }

        return _row.Slice(_cells[columnIndex] + 1);
    }

    private StorageKind KindOf(int columnIndex)
    {
        CheckIndex(columnIndex);
        return _kinds[columnIndex];
    }

    private void CheckIndex(int columnIndex)
    {
        if ((uint)columnIndex >= (uint)_kinds.Length)
        {
            throw OutOfRange(columnIndex, _kinds.Length);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ArgumentOutOfRangeException OutOfRange(int columnIndex, int count) =>
        new(nameof(columnIndex), columnIndex, $"The row has {count} columns.");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private InvalidCastException Mismatch(int columnIndex, string wanted) =>
        new($"Column {columnIndex} is {NameOf(_kinds[columnIndex])}, not {wanted}.");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidOperationException NullCell(int columnIndex) =>
        new($"Column {columnIndex} is NULL. Check IsNull before reading a nullable column.");

    private static string NameOf(StorageKind kind) => kind switch
    {
        StorageKind.Int => "INT",
        StorageKind.BigInt => "BIGINT",
        StorageKind.Float => "FLOAT",
        StorageKind.Double => "DOUBLE",
        StorageKind.Bit => "BIT",
        StorageKind.Date => "DATE",
        StorageKind.DateTime => "DATETIME",
        StorageKind.Guid => "GUID",
        StorageKind.Vector => "VECTOR",
        _ => "text",
    };
}
