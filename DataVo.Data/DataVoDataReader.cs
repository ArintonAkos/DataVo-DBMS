using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Diagnostics.CodeAnalysis;
using DataVo.Core;
using DataVo.Core.Contracts.Results;

namespace DataVo.Data;

/// <summary>
/// A forward-only reader that wraps the <see cref="QueryResult"/> returned by the DataVo engine.
/// Provides typed column access by ordinal or column name.
/// </summary>
/// <example>
/// <code>
/// using var reader = cmd.ExecuteReader();
/// while (reader.Read())
/// {
///     int id = reader.GetInt32("Id");
///     string name = reader.GetString("Name");
/// }
/// </code>
/// </example>
public class DataVoDataReader : DbDataReader
{
    // The state past the last result set: no columns and no rows, so FieldCount/HasRows stay answerable.
    private static readonly QueryResult s_exhausted = QueryResult.Default();

    private readonly List<QueryResult> _results;
    private readonly bool _singleRow;
    private readonly int _recordsAffected;
    private DataVoConnection? _closeOnDispose;
    private int _resultIndex;
    private int _rowIndex = -1;
    private bool _closed;

    private QueryResult CurrentResult => _resultIndex < _results.Count ? _results[_resultIndex] : s_exhausted;
    private Dictionary<string, object?>? CurrentRow =>
        _rowIndex >= 0 && _rowIndex < CurrentResult.Data.Count ? CurrentResult.Data[_rowIndex] : null;

    /// <summary>
    /// Initializes the reader from one or more <see cref="QueryResult"/> objects.
    /// </summary>
    /// <param name="results">The batch's results; only those with columns or rows become result sets.</param>
    /// <param name="behavior">The command behavior: <see cref="CommandBehavior.SingleResult"/> keeps only the first
    /// result set and <see cref="CommandBehavior.SingleRow"/> only its first row.</param>
    /// <param name="closeOnDispose">The connection to close with the reader (<see cref="CommandBehavior.CloseConnection"/>).</param>
    internal DataVoDataReader(List<QueryResult> results, CommandBehavior behavior = CommandBehavior.Default, DataVoConnection? closeOnDispose = null)
    {
        _results = results.Where(r => r.Fields.Count > 0 || r.Data.Count > 0).ToList();
        if (_results.Count == 0)
            _results.Add(QueryResult.Default());

        if ((behavior & (CommandBehavior.SingleResult | CommandBehavior.SingleRow)) != 0 && _results.Count > 1)
            _results.RemoveRange(1, _results.Count - 1);

        _singleRow = (behavior & CommandBehavior.SingleRow) != 0;
        _recordsAffected = DataVoCommand.SumRowsAffected(results, noRowsValue: -1);
        _closeOnDispose = closeOnDispose;
    }

    /// <inheritdoc />
    public override int FieldCount => CurrentResult.Fields.Count;

    /// <inheritdoc />
    public override bool HasRows => CurrentResult.Data.Count > 0;

    /// <inheritdoc />
    public override bool IsClosed => _closed;

    /// <summary>
    /// Gets the number of rows the batch's INSERT, UPDATE and DELETE statements changed; <c>-1</c> when it had none.
    /// </summary>
    public override int RecordsAffected => _recordsAffected;

    /// <inheritdoc />
    public override int Depth => 0;

    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <summary>
    /// Advances to the next row. Returns <c>false</c> when there are no more rows.
    /// </summary>
    public override bool Read()
    {
        EnsureNotClosed();
        int rowCount = CurrentResult.Data.Count;
        if (_singleRow && rowCount > 1)
        {
            rowCount = 1;
        }

        if (_rowIndex < rowCount)
        {
            _rowIndex++;
        }

        return _rowIndex < rowCount;
    }

    /// <summary>
    /// Advances to the next result set (for batched queries). Returns <c>false</c> when exhausted; after that the
    /// reader reports no columns and no rows.
    /// </summary>
    public override bool NextResult()
    {
        EnsureNotClosed();
        if (_resultIndex < _results.Count)
        {
            _resultIndex++;
        }

        _rowIndex = -1;
        return _resultIndex < _results.Count;
    }

    /// <summary>
    /// Closes the reader, and its connection when the command ran with <see cref="CommandBehavior.CloseConnection"/>.
    /// </summary>
    public override void Close()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;
        DataVoConnection? connection = _closeOnDispose;
        _closeOnDispose = null;
        connection?.Close();
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }

        base.Dispose(disposing);
    }

    /// <summary>
    /// Gets a column's ordinal: an exact (case-sensitive) match first, then a case-insensitive one, as ADO.NET
    /// specifies.
    /// </summary>
    /// <exception cref="IndexOutOfRangeException">No column has that name.</exception>
    public override int GetOrdinal(string name)
    {
        List<string> fields = CurrentResult.Fields;
        int idx = fields.IndexOf(name);
        if (idx < 0)
            idx = fields.FindIndex(field => string.Equals(field, name, StringComparison.OrdinalIgnoreCase));
        if (idx < 0)
            throw new IndexOutOfRangeException($"Column '{name}' not found. Available columns: {string.Join(", ", fields)}");
        return idx;
    }

    /// <inheritdoc />
    public override string GetName(int ordinal) => CurrentResult.Fields[ordinal];

    /// <inheritdoc />
    public override object GetValue(int ordinal)
    {
        EnsureNotClosed();
        if (CurrentRow == null)
            throw new InvalidOperationException("No current row. Call Read() first.");

        string columnName = GetName(ordinal);
        return CurrentRow.GetValueOrDefault(columnName) ?? DBNull.Value;
    }

    /// <inheritdoc />
    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++) values[i] = GetValue(i);
        return count;
    }

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal));

    /// <inheritdoc />
    public override byte GetByte(int ordinal) => Convert.ToByte(GetValue(ordinal));

    /// <summary>
    /// Copies bytes of a binary (<see cref="byte"/>[] or <see cref="ReadOnlyMemory{T}"/> of bytes) column into
    /// <paramref name="buffer"/>; with a <see langword="null"/> buffer, returns the value's total length.
    /// </summary>
    /// <exception cref="InvalidCastException">The value is not binary.</exception>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        ReadOnlySpan<byte> bytes = GetValue(ordinal) switch
        {
            byte[] array => array,
            ReadOnlyMemory<byte> memory => memory.Span,
            Memory<byte> memory => memory.Span,
            object other => throw new InvalidCastException($"Column {ordinal} holds {DescribeType(other)}, not binary data."),
        };

        return CopySlice(bytes, dataOffset, buffer, bufferOffset, length);
    }

    /// <inheritdoc />
    public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal));

    /// <summary>
    /// Copies characters of a text column into <paramref name="buffer"/>; with a <see langword="null"/> buffer,
    /// returns the value's total length.
    /// </summary>
    /// <exception cref="InvalidCastException">The value is not text.</exception>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        ReadOnlySpan<char> chars = GetValue(ordinal) switch
        {
            string text => text.AsSpan(),
            char[] array => array,
            object other => throw new InvalidCastException($"Column {ordinal} holds {DescribeType(other)}, not text."),
        };

        return CopySlice(chars, dataOffset, buffer, bufferOffset, length);
    }

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal)
    {
        object value = GetValue(ordinal);
        return value switch
        {
            DataVoDate date => date.ToDateTime(),
            DateTime dateTime => dateTime,
            DateTimeOffset offset => offset.UtcDateTime,
            _ => Convert.ToDateTime(value, System.Globalization.CultureInfo.InvariantCulture),
        };
    }

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal));

    /// <inheritdoc />
    public override double GetDouble(int ordinal) => Convert.ToDouble(GetValue(ordinal));

    /// <inheritdoc />
    public override float GetFloat(int ordinal) => Convert.ToSingle(GetValue(ordinal));

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => Guid.Parse(GetValue(ordinal).ToString()!);

    /// <inheritdoc />
    public override short GetInt16(int ordinal) => Convert.ToInt16(GetValue(ordinal));

    /// <inheritdoc />
    public override int GetInt32(int ordinal) => Convert.ToInt32(GetValue(ordinal));

    /// <inheritdoc />
    public override long GetInt64(int ordinal) => Convert.ToInt64(GetValue(ordinal));

    /// <inheritdoc />
    /// <exception cref="InvalidCastException">The value is <see cref="DBNull"/>.</exception>
    public override string GetString(int ordinal) => GetValue(ordinal) switch
    {
        string text => text,
        DBNull => throw new InvalidCastException($"Column {ordinal} is NULL; check IsDBNull before calling GetString."),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        object other => other.ToString()!,
    };

    /// <summary>
    /// Gets a string value by column name. Convenience overload.
    /// </summary>
    public string GetString(string name) => GetString(GetOrdinal(name));

    /// <summary>
    /// Gets an integer value by column name. Convenience overload.
    /// </summary>
    public int GetInt32(string name) => GetInt32(GetOrdinal(name));

    /// <inheritdoc />
    [return: DynamicallyAccessedMembers(
        DynamicallyAccessedMemberTypes.PublicFields
        | DynamicallyAccessedMemberTypes.PublicProperties)]
    [UnconditionalSuppressMessage("Trimming", "IL2073",
        Justification = "GetFieldType returns the runtime CLR type of the stored cell value; the engine " +
                        "never reflects over its members, and consumers must not rely on reflecting them " +
                        "under trimming. The base DbDataReader.GetFieldType annotation is preserved on the signature.")]
    public override Type GetFieldType(int ordinal)
    {
        // ADO.NET callers (Dapper, DataTable.Load) ask for column metadata before the first Read(),
        // so the type is taken from the first non-null cell of the materialized result rather than
        // from the current row.
        object? val = CurrentRow != null ? GetValue(ordinal) : FirstNonNullValue(ordinal);
        return val == null || val == DBNull.Value ? typeof(object) : val.GetType();
    }

    /// <summary>
    /// Finds the first non-null value of a column in the current result set, or <c>null</c> when
    /// the result has no rows or the column is null in every row.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    private object? FirstNonNullValue(int ordinal)
    {
        string columnName = GetName(ordinal);
        List<Dictionary<string, object?>> rows = CurrentResult.Data;

        for (int i = 0; i < rows.Count; i++)
        {
            if (rows[i].GetValueOrDefault(columnName) is { } value)
            {
                return value;
            }
        }

        return null;
    }

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => GetValue(ordinal) == DBNull.Value;

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    /// <summary>
    /// Describes the current result set's columns (name, ordinal, CLR type, nullability) in the standard
    /// <see cref="SchemaTableColumn"/> layout used by <c>DataTable.Load</c> and ORMs.
    /// </summary>
    public override DataTable GetSchemaTable()
    {
        EnsureNotClosed();
        var table = new DataTable("SchemaTable") { Locale = CultureInfo.InvariantCulture };
        table.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
        table.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
        table.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
        table.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short));
        table.Columns.Add(SchemaTableColumn.NumericScale, typeof(short));
        // Typed as object, not Type: a DataColumn of System.Type makes the trimmer keep Type's annotated members
        // (IL2111). Each cell still holds the column's System.Type, which is what schema-table readers cast to.
        table.Columns.Add(SchemaTableColumn.DataType, typeof(object));
        table.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
        table.Columns.Add(SchemaTableColumn.IsKey, typeof(bool));
        table.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool));
        table.Columns.Add(SchemaTableColumn.IsLong, typeof(bool));
        table.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string));
        table.Columns.Add("DataTypeName", typeof(string));

        for (int ordinal = 0; ordinal < FieldCount; ordinal++)
        {
            Type type = GetFieldType(ordinal);
            DataRow row = table.NewRow();
            row[SchemaTableColumn.ColumnName] = GetName(ordinal);
            row[SchemaTableColumn.ColumnOrdinal] = ordinal;
            row[SchemaTableColumn.ColumnSize] = -1;
            row[SchemaTableColumn.NumericPrecision] = DBNull.Value;
            row[SchemaTableColumn.NumericScale] = DBNull.Value;
            row[SchemaTableColumn.DataType] = type;
            row[SchemaTableColumn.AllowDBNull] = true;
            row[SchemaTableColumn.IsKey] = false;
            row[SchemaTableColumn.IsUnique] = false;
            row[SchemaTableColumn.IsLong] = false;
            row[SchemaTableColumn.BaseColumnName] = GetName(ordinal);
            row["DataTypeName"] = type.Name;
            table.Rows.Add(row);
        }

        return table;
    }

    private void EnsureNotClosed()
    {
        if (_closed)
            throw new InvalidOperationException("The data reader is closed.");
    }

    private static string DescribeType(object value) => value is DBNull ? "NULL" : value.GetType().Name;

    // GetBytes/GetChars contract: a null buffer asks for the total length; otherwise copy up to `length` items
    // starting at `dataOffset` and return how many were copied.
    private static long CopySlice<T>(ReadOnlySpan<T> source, long dataOffset, T[]? buffer, int bufferOffset, int length)
    {
        if (buffer is null)
        {
            return source.Length;
        }

        if (dataOffset < 0 || dataOffset > source.Length)
            throw new ArgumentOutOfRangeException(nameof(dataOffset));
        if (bufferOffset < 0 || bufferOffset > buffer.Length)
            throw new ArgumentOutOfRangeException(nameof(bufferOffset));
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length));

        int count = (int)Math.Min(Math.Min(length, source.Length - dataOffset), buffer.Length - bufferOffset);
        source.Slice((int)dataOffset, count).CopyTo(buffer.AsSpan(bufferOffset, count));
        return count;
    }
}
