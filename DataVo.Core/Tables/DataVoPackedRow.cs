using System.ComponentModel;
using System.Runtime.CompilerServices;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Core;

/// <summary>
/// Builds a model from a <see cref="DataVoPackedRow"/>. <c>DataVo.Generators</c> writes one per model;
/// application code does not use this type directly.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <param name="row">The stored row.</param>
/// <returns>The model.</returns>
[EditorBrowsable(EditorBrowsableState.Never)]
public delegate T DataVoPackedReader<out T>(DataVoPackedRow row);

/// <summary>
/// One row of an in-memory table, read in place: each method takes a column's value straight from where the
/// table keeps it, with no intermediate row, dictionary or boxed value. Generated model readers use it;
/// application code does not use this type directly.
/// </summary>
/// <remarks>
/// Columns are numbered in the order of the model's column list. The value is valid only for the call it was
/// passed to. The methods mirror the readers of <see cref="DataVoGeneratedTableSupport"/>: a NULL in a column
/// whose property cannot hold one throws the same error.
/// </remarks>
[EditorBrowsable(EditorBrowsableState.Never)]
public readonly ref struct DataVoPackedRow
{
    private readonly ReadOnlySpan<byte> _record;
    private readonly object?[] _references;
    private readonly int _referenceBase;
    private readonly PackedField[] _fields;

    internal DataVoPackedRow(FlatRow row, PackedField[] fields)
    {
        _record = row.Record;
        _references = row.References;
        _referenceBase = row.ReferenceBase;
        _fields = fields;
    }

    /// <summary>Reads a required INT column.</summary>
    public int ReadInt32(int column, string name) =>
        IsNull(column) ? throw DataVoGeneratedTableSupport.NullColumn(name) : FlatRowCodec.ReadInt32(_record, _fields[column].Position);

    /// <summary>Reads a nullable INT column.</summary>
    public int? ReadNullableInt32(int column, string name) =>
        IsNull(column) ? null : FlatRowCodec.ReadInt32(_record, _fields[column].Position);

    /// <summary>Reads a required BIGINT column.</summary>
    public long ReadInt64(int column, string name) =>
        IsNull(column) ? throw DataVoGeneratedTableSupport.NullColumn(name) : FlatRowCodec.ReadInt64(_record, _fields[column].Position);

    /// <summary>Reads a nullable BIGINT column.</summary>
    public long? ReadNullableInt64(int column, string name) =>
        IsNull(column) ? null : FlatRowCodec.ReadInt64(_record, _fields[column].Position);

    /// <summary>Reads a required DOUBLE column.</summary>
    public double ReadDouble(int column, string name) =>
        IsNull(column) ? throw DataVoGeneratedTableSupport.NullColumn(name) : FlatRowCodec.ReadDouble(_record, _fields[column].Position);

    /// <summary>Reads a nullable DOUBLE column.</summary>
    public double? ReadNullableDouble(int column, string name) =>
        IsNull(column) ? null : FlatRowCodec.ReadDouble(_record, _fields[column].Position);

    /// <summary>Reads a required FLOAT column.</summary>
    public float ReadSingle(int column, string name) => (float)ReadDouble(column, name);

    /// <summary>Reads a nullable FLOAT column.</summary>
    public float? ReadNullableSingle(int column, string name) => (float?)ReadNullableDouble(column, name);

    /// <summary>Reads a required BIT column.</summary>
    public bool ReadBoolean(int column, string name) =>
        IsNull(column) ? throw DataVoGeneratedTableSupport.NullColumn(name) : FlatRowCodec.ReadBoolean(_record, _fields[column].Position);

    /// <summary>Reads a nullable BIT column.</summary>
    public bool? ReadNullableBoolean(int column, string name) =>
        IsNull(column) ? null : FlatRowCodec.ReadBoolean(_record, _fields[column].Position);

    /// <summary>Reads a required VARCHAR column.</summary>
    public string ReadString(int column, string name) =>
        ReadNullableString(column, name) ?? throw DataVoGeneratedTableSupport.NullColumn(name);

    /// <summary>Reads a nullable VARCHAR column.</summary>
    public string? ReadNullableString(int column, string name) =>
        FlatRowCodec.ReadString(_references, _referenceBase, _fields[column].Position);

    /// <summary>Reads a required GUID column.</summary>
    public Guid ReadGuid(int column, string name) =>
        IsNull(column) ? throw DataVoGeneratedTableSupport.NullColumn(name) : FlatRowCodec.ReadGuid(_record, _fields[column].Position);

    /// <summary>Reads a nullable GUID column.</summary>
    public Guid? ReadNullableGuid(int column, string name) =>
        IsNull(column) ? null : FlatRowCodec.ReadGuid(_record, _fields[column].Position);

    /// <summary>Reads a required DATE column.</summary>
    public DataVoDate ReadDate(int column, string name) =>
        IsNull(column) ? throw DataVoGeneratedTableSupport.NullColumn(name) : FlatRowCodec.ReadDate(_record, _fields[column].Position);

    /// <summary>Reads a nullable DATE column.</summary>
    public DataVoDate? ReadNullableDate(int column, string name) =>
        IsNull(column) ? null : FlatRowCodec.ReadDate(_record, _fields[column].Position);

    /// <summary>Reads a required VECTOR(3) column as a 3D point.</summary>
    public System.Numerics.Vector3 ReadVector3(int column, string name) =>
        ReadNullableVector3(column, name) ?? throw DataVoGeneratedTableSupport.NullColumn(name);

    /// <summary>Reads a nullable VECTOR(3) column as a 3D point.</summary>
    public System.Numerics.Vector3? ReadNullableVector3(int column, string name) =>
        FlatRowCodec.ReadVector(_references, _referenceBase, _fields[column].Position) switch
        {
            null => null,
            { Length: 3 } xyz => new System.Numerics.Vector3(xyz[0], xyz[1], xyz[2]),
            float[] other => throw new InvalidOperationException($"Column {name} holds {other.Length} values, but a 3D position needs 3."),
        };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsNull(int column)
    {
        ref readonly PackedField field = ref _fields[column];
        return (_record[field.NullOffset] & field.NullMask) != 0;
    }
}

/// <summary>
/// Where a model reads one of its columns in a packed record: the value's position, and the bit that says the
/// column is NULL.
/// </summary>
internal readonly struct PackedField
{
    /// <summary>Describes one column of a model.</summary>
    /// <param name="position">The value's byte offset in the record, or its reference slot.</param>
    /// <param name="nullOffset">The offset of the record byte that holds the column's null bit.</param>
    /// <param name="nullMask">The column's null bit within that byte.</param>
    public PackedField(int position, int nullOffset, byte nullMask)
    {
        Position = position;
        NullOffset = nullOffset;
        NullMask = nullMask;
    }

    /// <summary>The value's byte offset in the record, or its reference slot.</summary>
    public int Position { get; }

    /// <summary>The offset of the record byte that holds the column's null bit.</summary>
    public int NullOffset { get; }

    /// <summary>The column's null bit within that byte.</summary>
    public byte NullMask { get; }
}
