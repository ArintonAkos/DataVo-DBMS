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
public readonly ref struct CompiledRowReader
{
    private readonly StoredRowView _view;

    internal CompiledRowReader(StoredRowView view) => _view = view;

    /// <summary>Whether the named column holds SQL NULL.</summary>
    public bool IsNull(string column) => _view[column].IsNull;

    /// <summary>Reads an <see cref="int"/> column; throws if the cell is not Int32 or is NULL.</summary>
    public int GetInt32(string column) => _view[column].AsInt32();

    /// <summary>Reads a <see cref="long"/> column; throws if the cell is not Int64 or is NULL.</summary>
    public long GetInt64(string column) => _view[column].AsInt64();

    /// <summary>Reads a <see cref="double"/> column; throws if the cell is not Double or is NULL.</summary>
    public double GetDouble(string column) => _view[column].AsDouble();

    /// <summary>Reads a <see cref="decimal"/> column; throws if the cell is not Decimal or is NULL.</summary>
    public decimal GetDecimal(string column) => _view[column].AsDecimal();

    /// <summary>Reads a <see cref="bool"/> column; throws if the cell is not Boolean or is NULL.</summary>
    public bool GetBoolean(string column) => _view[column].AsBoolean();

    /// <summary>Reads a <see cref="DateOnly"/> column; throws if the cell is not Date or is NULL.</summary>
    public DateOnly GetDate(string column) => _view[column].AsDate();

    /// <summary>Reads a VECTOR (<see cref="float"/>[]) column, returning a defensive clone.</summary>
    public float[] GetVector(string column) => _view[column].AsVector();

    /// <summary>Reads a string column; SQL NULL returns <c>null</c>.</summary>
    public string? GetString(string column) => _view[column].AsString();

    /// <summary>Reads a nullable <see cref="int"/> column; SQL NULL returns <c>null</c>.</summary>
    public int? GetInt32OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsInt32(); }

    /// <summary>Reads a nullable <see cref="long"/> column; SQL NULL returns <c>null</c>.</summary>
    public long? GetInt64OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsInt64(); }

    /// <summary>Reads a nullable <see cref="double"/> column; SQL NULL returns <c>null</c>.</summary>
    public double? GetDoubleOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDouble(); }

    /// <summary>Reads a nullable <see cref="decimal"/> column; SQL NULL returns <c>null</c>.</summary>
    public decimal? GetDecimalOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDecimal(); }

    /// <summary>Reads a nullable <see cref="bool"/> column; SQL NULL returns <c>null</c>.</summary>
    public bool? GetBooleanOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsBoolean(); }

    /// <summary>Reads a nullable <see cref="DateOnly"/> column; SQL NULL returns <c>null</c>.</summary>
    public DateOnly? GetDateOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDate(); }
}
