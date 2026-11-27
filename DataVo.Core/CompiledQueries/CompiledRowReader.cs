using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.CompiledQueries;

/// <summary>
/// A public, zero-boxing reader over one stored row, addressed by column name. Wraps the internal
/// <see cref="StoredRowView"/> so source-generated projectors (which live in the consumer assembly and cannot
/// see internals) can read typed cells without materializing a dictionary or boxing. Getters fail fast: an
/// unknown column throws <see cref="KeyNotFoundException"/>; a type mismatch or NULL into a non-nullable getter
/// throws (mirroring the dictionary path's casts).
/// </summary>
public readonly ref partial struct CompiledRowReader
{
    private readonly StoredRowView _view;

    internal CompiledRowReader(StoredRowView view) => _view = view;

    /// <summary>
    /// Reads a column as a boxed value (<c>null</c> for SQL NULL; a VECTOR is copied). Generated mappers use it only
    /// for constructor parameter types that have no typed getter.
    /// </summary>
    public object? GetValue(string column) => _view[column].ToObject();

    /// <summary>Whether the named column holds SQL NULL.</summary>
    public bool IsNull(string column) => _view[column].IsNull;

    /// <summary>Whether the ordinal column holds SQL NULL.</summary>
    public bool IsNull(int ordinal) => _view[ordinal].IsNull;

    /// <summary>Reads an <see cref="int"/> column; throws if the cell is not Int32 or is NULL.</summary>
    public int GetInt32(string column) => _view[column].AsInt32();

    /// <summary>Reads an <see cref="int"/> column by ordinal; throws if the cell is not Int32 or is NULL.</summary>
    public int GetInt32(int ordinal) => _view[ordinal].AsInt32();

    /// <summary>Reads a <see cref="long"/> column; throws if the cell is not Int64 or is NULL.</summary>
    public long GetInt64(string column) => _view[column].AsInt64();

    /// <summary>Reads a <see cref="long"/> column by ordinal; throws if the cell is not Int64 or is NULL.</summary>
    public long GetInt64(int ordinal) => _view[ordinal].AsInt64();

    /// <summary>Reads a <see cref="double"/> column; throws if the cell is not Double or is NULL.</summary>
    public double GetDouble(string column) => _view[column].AsDouble();

    /// <summary>Reads a <see cref="double"/> column by ordinal; throws if the cell is not Double or is NULL.</summary>
    public double GetDouble(int ordinal) => _view[ordinal].AsDouble();

    /// <summary>Reads a <see cref="decimal"/> column; throws if the cell is not Decimal or is NULL.</summary>
    public decimal GetDecimal(string column) => _view[column].AsDecimal();

    /// <summary>Reads a <see cref="decimal"/> column by ordinal; throws if the cell is not Decimal or is NULL.</summary>
    public decimal GetDecimal(int ordinal) => _view[ordinal].AsDecimal();

    /// <summary>Reads a <see cref="bool"/> column; throws if the cell is not Boolean or is NULL.</summary>
    public bool GetBoolean(string column) => _view[column].AsBoolean();

    /// <summary>Reads a <see cref="bool"/> column by ordinal; throws if the cell is not Boolean or is NULL.</summary>
    public bool GetBoolean(int ordinal) => _view[ordinal].AsBoolean();

    /// <summary>Reads a DATETIME column as a UTC <see cref="DateTime"/>; throws if the cell is not DateTime or is NULL.</summary>
    public DateTime GetDateTime(string column) => _view[column].AsDateTime();

    /// <summary>Reads a DATETIME column by ordinal as a UTC <see cref="DateTime"/>.</summary>
    public DateTime GetDateTime(int ordinal) => _view[ordinal].AsDateTime();

    /// <summary>Reads a DATETIME column as a <see cref="DateTimeOffset"/> (offset zero).</summary>
    public DateTimeOffset GetDateTimeOffset(string column) => _view[column].AsDateTimeOffset();

    /// <summary>Reads a DATETIME column by ordinal as a <see cref="DateTimeOffset"/> (offset zero).</summary>
    public DateTimeOffset GetDateTimeOffset(int ordinal) => _view[ordinal].AsDateTimeOffset();

    /// <summary>Reads a nullable DATETIME column; SQL NULL returns <c>null</c>.</summary>
    public DateTime? GetDateTimeOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDateTime(); }

    /// <summary>Reads a nullable DATETIME column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public DateTime? GetDateTimeOrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsDateTime(); }

    /// <summary>Reads a nullable DATETIME column as a <see cref="DateTimeOffset"/>; SQL NULL returns <c>null</c>.</summary>
    public DateTimeOffset? GetDateTimeOffsetOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDateTimeOffset(); }

    /// <summary>Reads a nullable DATETIME column by ordinal as a <see cref="DateTimeOffset"/>; SQL NULL returns <c>null</c>.</summary>
    public DateTimeOffset? GetDateTimeOffsetOrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsDateTimeOffset(); }

    /// <summary>Reads a <see cref="DataVoDate"/> column; throws if the cell is not Date or is NULL.</summary>
    public DataVoDate GetDate(string column) => _view[column].AsDate();

    /// <summary>Reads a <see cref="DataVoDate"/> column by ordinal; throws if the cell is not Date or is NULL.</summary>
    public DataVoDate GetDate(int ordinal) => _view[ordinal].AsDate();

    /// <summary>Reads a <see cref="Guid"/> column; throws if the cell is not Guid or is NULL.</summary>
    public Guid GetGuid(string column) => _view[column].AsGuid();

    /// <summary>Reads a <see cref="Guid"/> column by ordinal; throws if the cell is not Guid or is NULL.</summary>
    public Guid GetGuid(int ordinal) => _view[ordinal].AsGuid();

    /// <summary>Reads a VECTOR (<see cref="float"/>[]) column, returning a defensive clone.</summary>
    public float[] GetVector(string column) => _view[column].AsVector();

    /// <summary>Reads a VECTOR column by ordinal, returning a defensive clone.</summary>
    public float[] GetVector(int ordinal) => _view[ordinal].AsVector();

    /// <summary>Reads a string column; SQL NULL returns <c>null</c>.</summary>
    public string? GetString(string column) => _view[column].AsString();

    /// <summary>Reads a string column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public string? GetString(int ordinal) => _view[ordinal].AsString();

    /// <summary>Reads a nullable <see cref="int"/> column; SQL NULL returns <c>null</c>.</summary>
    public int? GetInt32OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsInt32(); }

    /// <summary>Reads a nullable <see cref="int"/> column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public int? GetInt32OrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsInt32(); }

    /// <summary>Reads a nullable <see cref="long"/> column; SQL NULL returns <c>null</c>.</summary>
    public long? GetInt64OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsInt64(); }

    /// <summary>Reads a nullable <see cref="long"/> column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public long? GetInt64OrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsInt64(); }

    /// <summary>Reads a nullable <see cref="double"/> column; SQL NULL returns <c>null</c>.</summary>
    public double? GetDoubleOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDouble(); }

    /// <summary>Reads a nullable <see cref="double"/> column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public double? GetDoubleOrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsDouble(); }

    /// <summary>Reads a nullable <see cref="decimal"/> column; SQL NULL returns <c>null</c>.</summary>
    public decimal? GetDecimalOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDecimal(); }

    /// <summary>Reads a nullable <see cref="decimal"/> column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public decimal? GetDecimalOrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsDecimal(); }

    /// <summary>Reads a nullable <see cref="bool"/> column; SQL NULL returns <c>null</c>.</summary>
    public bool? GetBooleanOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsBoolean(); }

    /// <summary>Reads a nullable <see cref="bool"/> column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public bool? GetBooleanOrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsBoolean(); }

    /// <summary>Reads a nullable <see cref="DataVoDate"/> column; SQL NULL returns <c>null</c>.</summary>
    public DataVoDate? GetDateOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDate(); }

    /// <summary>Reads a nullable <see cref="DataVoDate"/> column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public DataVoDate? GetDateOrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsDate(); }

    /// <summary>Reads a nullable <see cref="Guid"/> column; SQL NULL returns <c>null</c>.</summary>
    public Guid? GetGuidOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsGuid(); }

    /// <summary>Reads a nullable <see cref="Guid"/> column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public Guid? GetGuidOrNull(int ordinal) { CellValue c = _view[ordinal]; return c.IsNull ? null : c.AsGuid(); }

    /// <summary>Reads a FLOAT column as a <see cref="float"/> (stored as a double cell).</summary>
    public float GetSingle(string column) => (float)_view[column].AsDouble();

    /// <summary>Reads a nullable FLOAT column as a <see cref="float"/>; SQL NULL returns <c>null</c>.</summary>
    public float? GetSingleOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : (float)c.AsDouble(); }

    /// <summary>Reads an INT column as a <see cref="short"/>; throws <see cref="OverflowException"/> when it does not fit.</summary>
    public short GetInt16(string column) => checked((short)_view[column].AsInt32());

    /// <summary>Reads a nullable INT column as a <see cref="short"/>; SQL NULL returns <c>null</c>.</summary>
    public short? GetInt16OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : checked((short)c.AsInt32()); }

    /// <summary>Reads an INT column as a <see cref="byte"/>; throws <see cref="OverflowException"/> when it does not fit.</summary>
    public byte GetByte(string column) => checked((byte)_view[column].AsInt32());

    /// <summary>Reads a nullable INT column as a <see cref="byte"/>; SQL NULL returns <c>null</c>.</summary>
    public byte? GetByteOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : checked((byte)c.AsInt32()); }

    /// <summary>Reads a VECTOR column as <see cref="ReadOnlyMemory{T}"/> of <see cref="float"/> over a defensive clone.</summary>
    public ReadOnlyMemory<float> GetVectorMemory(string column) => _view[column].AsVector();

    /// <summary>Reads a nullable VECTOR column as <see cref="ReadOnlyMemory{T}"/>; SQL NULL returns <c>null</c>.</summary>
    public ReadOnlyMemory<float>? GetVectorMemoryOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsVector(); }

    /// <summary>Reads a VECTOR(3) column as a <see cref="System.Numerics.Vector3"/>.</summary>
    /// <exception cref="InvalidCastException">The vector does not have three components.</exception>
    public System.Numerics.Vector3 GetVector3(string column) => ToVector3(_view[column], column);

    /// <summary>Reads a nullable VECTOR(3) column as a <see cref="System.Numerics.Vector3"/>; SQL NULL returns <c>null</c>.</summary>
    public System.Numerics.Vector3? GetVector3OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : ToVector3(c, column); }

    private static System.Numerics.Vector3 ToVector3(CellValue cell, string column) =>
        cell.VectorLength == 3 && cell.AsVector() is { } xyz
            ? new System.Numerics.Vector3(xyz[0], xyz[1], xyz[2])
            : throw new InvalidCastException($"Column '{column}' holds a vector of {cell.VectorLength} components, not 3.");
}
