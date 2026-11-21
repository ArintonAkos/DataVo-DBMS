using System.ComponentModel;
using System.Numerics;
using DataVo.Core.Annotations;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A borrowed view over a single reactive row: a <see cref="ReactiveRowSchema"/> plus the row's
/// <see cref="CellValue"/> cells. Valid only for the duration of the callback/operator frame that
/// owns the backing buffer — being a <c>ref struct</c>, it cannot be stored, boxed, or captured.
/// Retain data via <see cref="ToOwnedDictionary"/>.
/// </summary>
public readonly ref struct RowRef
{
    private readonly ReactiveRowSchema _schema;
    private readonly ReadOnlySpan<CellValue> _values;

    /// <summary>Creates a borrowed row view. The cell count must equal the schema's column count.</summary>
    public RowRef(ReactiveRowSchema schema, ReadOnlySpan<CellValue> values)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(schema);
        if (values.Length != schema.ColumnCount)
        {
            throw new ArgumentException(
                $"Row has {values.Length} cells but schema has {schema.ColumnCount} columns.",
                nameof(values));
        }

        _schema = schema;
        _values = values;
    }

    /// <summary>The number of cells in this row.</summary>
    public int Count => _values.Length;

    /// <summary>The shared column names.</summary>
    public ReadOnlySpan<string> Columns => _schema.Columns;

    /// <summary>The row's cells, in column order.</summary>
    public ReadOnlySpan<CellValue> Values => _values;

    /// <summary>The cell at <paramref name="ordinal"/>.</summary>
    public ref readonly CellValue this[int ordinal] => ref _values[ordinal];

    /// <summary>Resolves a cell by column name (case-insensitive).</summary>
    public bool TryGet(string column, out CellValue value)
    {
        if (_schema.TryGetOrdinal(column, out int ordinal))
        {
            value = _values[ordinal];
            return true;
        }

        value = CellValue.Null;
        return false;
    }

    /// <summary>The cell with the given column name (case-insensitive).</summary>
    /// <exception cref="KeyNotFoundException">No such column.</exception>
    public CellValue this[string column] =>
        _schema.TryGetOrdinal(column, out int ordinal)
            ? _values[ordinal]
            : throw new KeyNotFoundException($"Column '{column}' is not in the row schema.");

    /// <summary>
    /// Compatibility-only: copies this row into a boxed, case-insensitive dictionary. NOT for the
    /// hot path — used to materialize to the owned <see cref="QueryChange"/> API.
    /// </summary>
    public IReadOnlyDictionary<string, object?> ToOwnedDictionary()
    {
        var dict = new Dictionary<string, object?>(_values.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _values.Length; i++)
        {
            dict[_schema.ColumnAt(i)] = _values[i].ToObject();
        }

        return dict;
    }

    /// <summary>Reads a required <c>INT</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public int ReadInt32(int ordinal, string name)
    {
        CellValue cell = _values[ordinal];
        return cell.Type switch
        {
            CellType.Int32 => cell.AsInt32(),
            CellType.Int64 => checked((int)cell.AsInt64()),
            CellType.Null => throw DataVoGeneratedTableSupport.NullColumn(name),
            _ => DataVoGeneratedTableSupport.ReadInt32(cell.ToObject(), name),
        };
    }

    /// <summary>Reads a nullable <c>INT</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public int? ReadNullableInt32(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : ReadInt32(ordinal, name);

    /// <summary>Reads a required <c>BIGINT</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public long ReadInt64(int ordinal, string name)
    {
        CellValue cell = _values[ordinal];
        return cell.Type switch
        {
            CellType.Int64 => cell.AsInt64(),
            CellType.Int32 => cell.AsInt32(),
            CellType.Null => throw DataVoGeneratedTableSupport.NullColumn(name),
            _ => DataVoGeneratedTableSupport.ReadInt64(cell.ToObject(), name),
        };
    }

    /// <summary>Reads a nullable <c>BIGINT</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public long? ReadNullableInt64(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : ReadInt64(ordinal, name);

    /// <summary>Reads a required <c>DOUBLE</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public double ReadDouble(int ordinal, string name)
    {
        CellValue cell = _values[ordinal];
        return cell.Type switch
        {
            CellType.Double => cell.AsDouble(),
            CellType.Int32 => cell.AsInt32(),
            CellType.Int64 => cell.AsInt64(),
            CellType.Decimal => (double)cell.AsDecimal(),
            CellType.Null => throw DataVoGeneratedTableSupport.NullColumn(name),
            _ => DataVoGeneratedTableSupport.ReadDouble(cell.ToObject(), name),
        };
    }

    /// <summary>Reads a nullable <c>DOUBLE</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public double? ReadNullableDouble(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : ReadDouble(ordinal, name);

    /// <summary>Reads a required <c>FLOAT</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public float ReadSingle(int ordinal, string name) => (float)ReadDouble(ordinal, name);

    /// <summary>Reads a nullable <c>FLOAT</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public float? ReadNullableSingle(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : (float)ReadDouble(ordinal, name);

    /// <summary>Reads a required <c>BIT</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool ReadBoolean(int ordinal, string name)
    {
        CellValue cell = _values[ordinal];
        return cell.Type switch
        {
            CellType.Boolean => cell.AsBoolean(),
            CellType.Null => throw DataVoGeneratedTableSupport.NullColumn(name),
            _ => DataVoGeneratedTableSupport.ReadBoolean(cell.ToObject(), name),
        };
    }

    /// <summary>Reads a nullable <c>BIT</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool? ReadNullableBoolean(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : ReadBoolean(ordinal, name);

    /// <summary>Reads a required <c>VARCHAR</c> cell at <paramref name="ordinal"/> without allocation. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string ReadString(int ordinal, string name)
    {
        CellValue cell = _values[ordinal];
        return cell.Type switch
        {
            CellType.String => cell.AsString()!,
            CellType.Null => throw DataVoGeneratedTableSupport.NullColumn(name),
            _ => DataVoGeneratedTableSupport.ReadString(cell.ToObject(), name),
        };
    }

    /// <summary>Reads a nullable <c>VARCHAR</c> cell at <paramref name="ordinal"/> without allocation. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public string? ReadNullableString(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : ReadString(ordinal, name);

    /// <summary>Reads a required <c>GUID</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Guid ReadGuid(int ordinal, string name)
    {
        CellValue cell = _values[ordinal];
        return cell.Type switch
        {
            CellType.Guid => cell.AsGuid(),
            CellType.Null => throw DataVoGeneratedTableSupport.NullColumn(name),
            _ => DataVoGeneratedTableSupport.ReadGuid(cell.ToObject(), name),
        };
    }

    /// <summary>Reads a nullable <c>GUID</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Guid? ReadNullableGuid(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : ReadGuid(ordinal, name);

    /// <summary>Reads a required <c>DATE</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoDate ReadDate(int ordinal, string name)
    {
        CellValue cell = _values[ordinal];
        return cell.Type switch
        {
            CellType.Date => cell.AsDate(),
            CellType.Null => throw DataVoGeneratedTableSupport.NullColumn(name),
            _ => DataVoGeneratedTableSupport.ReadDate(cell.ToObject(), name),
        };
    }

    /// <summary>Reads a nullable <c>DATE</c> cell at <paramref name="ordinal"/> without boxing. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public DataVoDate? ReadNullableDate(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : ReadDate(ordinal, name);

    /// <summary>Reads a required <c>VECTOR(3)</c> cell at <paramref name="ordinal"/> as a <see cref="Vector3"/> without cloning. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Vector3 ReadVector3(int ordinal, string name)
    {
        CellValue cell = _values[ordinal];
        if (cell.Type == CellType.Vector)
        {
            ReadOnlySpan<float> xyz = cell.AsVectorReadOnlySpan();
            if (xyz.Length != 3)
            {
                throw new InvalidOperationException($"Column {name} holds {xyz.Length} values, but a 3D position needs 3.");
            }

            return new Vector3(xyz[0], xyz[1], xyz[2]);
        }

        return cell.IsNull
            ? throw DataVoGeneratedTableSupport.NullColumn(name)
            : DataVoGeneratedTableSupport.ReadVector3(cell.ToObject(), name);
    }

    /// <summary>Reads a nullable <c>VECTOR(3)</c> cell at <paramref name="ordinal"/> as a <see cref="Vector3"/> without cloning. Used by generated code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Vector3? ReadNullableVector3(int ordinal, string name) =>
        _values[ordinal].IsNull ? null : ReadVector3(ordinal, name);
}
