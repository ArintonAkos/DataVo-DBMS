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

    public int GetInt32(string column) => _view[column].AsInt32();
    public long GetInt64(string column) => _view[column].AsInt64();
    public double GetDouble(string column) => _view[column].AsDouble();
    public decimal GetDecimal(string column) => _view[column].AsDecimal();
    public bool GetBoolean(string column) => _view[column].AsBoolean();
    public DateOnly GetDate(string column) => _view[column].AsDate();
    public float[] GetVector(string column) => _view[column].AsVector();

    /// <summary>Reads a string column; SQL NULL returns <c>null</c>.</summary>
    public string? GetString(string column) => _view[column].AsString();

    public int? GetInt32OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsInt32(); }
    public long? GetInt64OrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsInt64(); }
    public double? GetDoubleOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDouble(); }
    public decimal? GetDecimalOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDecimal(); }
    public bool? GetBooleanOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsBoolean(); }
    public DateOnly? GetDateOrNull(string column) { CellValue c = _view[column]; return c.IsNull ? null : c.AsDate(); }
}
