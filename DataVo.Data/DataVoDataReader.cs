using System.Collections;
using System.Data.Common;
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
    private readonly List<QueryResult> _results;
    private int _resultIndex;
    private int _rowIndex = -1;

    private QueryResult CurrentResult => _results[_resultIndex];
    private Dictionary<string, dynamic>? CurrentRow =>
        _rowIndex >= 0 && _rowIndex < CurrentResult.Data.Count ? CurrentResult.Data[_rowIndex] : null;

    /// <summary>
    /// Initializes the reader from one or more <see cref="QueryResult"/> objects.
    /// </summary>
    internal DataVoDataReader(List<QueryResult> results)
    {
        _results = results.Where(r => r.Fields.Count > 0 || r.Data.Count > 0).ToList();
        if (_results.Count == 0)
            _results.Add(QueryResult.Default());
    }

    /// <inheritdoc />
    public override int FieldCount => CurrentResult.Fields.Count;

    /// <inheritdoc />
    public override bool HasRows => CurrentResult.Data.Count > 0;

    /// <inheritdoc />
    public override bool IsClosed { get; }

    /// <inheritdoc />
    public override int RecordsAffected => -1;

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
        _rowIndex++;
        return _rowIndex < CurrentResult.Data.Count;
    }

    /// <summary>
    /// Advances to the next result set (for batched queries). Returns <c>false</c> when exhausted.
    /// </summary>
    public override bool NextResult()
    {
        _resultIndex++;
        _rowIndex = -1;
        return _resultIndex < _results.Count;
    }

    /// <inheritdoc />
    public override int GetOrdinal(string name)
    {
        int idx = CurrentResult.Fields.IndexOf(name);
        if (idx < 0)
            throw new IndexOutOfRangeException($"Column '{name}' not found. Available columns: {string.Join(", ", CurrentResult.Fields)}");
        return idx;
    }

    /// <inheritdoc />
    public override string GetName(int ordinal) => CurrentResult.Fields[ordinal];

    /// <inheritdoc />
    public override object GetValue(int ordinal)
    {
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

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal));

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
        throw new NotSupportedException();

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal) => Convert.ToDateTime(GetValue(ordinal));

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
    public override string GetString(int ordinal) => GetValue(ordinal).ToString()!;

    /// <summary>
    /// Gets a string value by column name. Convenience overload.
    /// </summary>
    public string GetString(string name) => GetString(GetOrdinal(name));

    /// <summary>
    /// Gets an integer value by column name. Convenience overload.
    /// </summary>
    public int GetInt32(string name) => GetInt32(GetOrdinal(name));

    /// <inheritdoc />
    public override Type GetFieldType(int ordinal)
    {
        object val = GetValue(ordinal);
        return val == DBNull.Value ? typeof(object) : val.GetType();
    }

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => GetValue(ordinal) == DBNull.Value;

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this);
}
