using System.Data;
using System.Data.Common;

namespace DataVo.Data;

/// <summary>
/// Represents a named parameter for a <see cref="DataVoCommand"/>.
/// </summary>
/// <example>
/// <code>
/// var param = new DataVoParameter("@age", 21);
/// cmd.Parameters.Add(param);
/// </code>
/// </example>
public class DataVoParameter : DbParameter
{
    /// <inheritdoc />
    public override DbType DbType { get; set; } = DbType.String;

    /// <inheritdoc />
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;

    /// <inheritdoc />
    public override bool IsNullable { get; set; } = true;

    /// <inheritdoc />
    public override string ParameterName { get; set; } = string.Empty;

    /// <inheritdoc />
    public override int Size { get; set; }

    /// <inheritdoc />
    public override string SourceColumn { get; set; } = string.Empty;

    /// <inheritdoc />
    public override bool SourceColumnNullMapping { get; set; }

    /// <inheritdoc />
    public override object? Value { get; set; }

    /// <summary>
    /// Creates a new empty parameter.
    /// </summary>
    public DataVoParameter() { }

    /// <summary>
    /// Creates a parameter with the specified name and value.
    /// </summary>
    /// <param name="name">The parameter name (e.g. <c>@age</c>).</param>
    /// <param name="value">The parameter value.</param>
    public DataVoParameter(string name, object? value)
    {
        ParameterName = name;
        Value = value;
    }

    /// <inheritdoc />
    public override void ResetDbType() => DbType = DbType.String;
}

/// <summary>
/// A collection of <see cref="DataVoParameter"/> objects attached to a <see cref="DataVoCommand"/>.
/// </summary>
public class DataVoParameterCollection : DbParameterCollection
{
    private readonly List<DataVoParameter> _parameters = [];

    /// <inheritdoc />
    public override int Count => _parameters.Count;

    /// <inheritdoc />
    public override object SyncRoot => ((System.Collections.ICollection)_parameters).SyncRoot;

    /// <summary>
    /// Adds a parameter with the given name and value.
    /// </summary>
    /// <param name="parameterName">The parameter name (e.g. <c>@age</c>).</param>
    /// <param name="value">The parameter value.</param>
    /// <returns>The created <see cref="DataVoParameter"/>.</returns>
    public DataVoParameter AddWithValue(string parameterName, object? value)
    {
        var p = new DataVoParameter(parameterName, value);
        _parameters.Add(p);
        return p;
    }

    /// <inheritdoc />
    public override int Add(object value)
    {
        _parameters.Add((DataVoParameter)value);
        return _parameters.Count - 1;
    }

    /// <inheritdoc />
    public override void AddRange(Array values)
    {
        foreach (DataVoParameter p in values) _parameters.Add(p);
    }

    /// <inheritdoc />
    public override void Clear() => _parameters.Clear();

    /// <inheritdoc />
    public override bool Contains(object value) => _parameters.Contains((DataVoParameter)value);

    /// <inheritdoc />
    public override bool Contains(string value) => _parameters.Exists(p => p.ParameterName == value);

    /// <inheritdoc />
    public override void CopyTo(Array array, int index) => ((System.Collections.ICollection)_parameters).CopyTo(array, index);

    /// <inheritdoc />
    public override System.Collections.IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    /// <inheritdoc />
    public override int IndexOf(object value) => _parameters.IndexOf((DataVoParameter)value);

    /// <inheritdoc />
    public override int IndexOf(string parameterName) => _parameters.FindIndex(p => p.ParameterName == parameterName);

    /// <inheritdoc />
    public override void Insert(int index, object value) => _parameters.Insert(index, (DataVoParameter)value);

    /// <inheritdoc />
    public override void Remove(object value) => _parameters.Remove((DataVoParameter)value);

    /// <inheritdoc />
    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    /// <inheritdoc />
    public override void RemoveAt(string parameterName) => _parameters.RemoveAll(p => p.ParameterName == parameterName);

    /// <inheritdoc />
    protected override DbParameter GetParameter(int index) => _parameters[index];

    /// <inheritdoc />
    protected override DbParameter GetParameter(string parameterName) => _parameters.First(p => p.ParameterName == parameterName);

    /// <inheritdoc />
    protected override void SetParameter(int index, DbParameter value) => _parameters[index] = (DataVoParameter)value;

    /// <inheritdoc />
    protected override void SetParameter(string parameterName, DbParameter value)
    {
        int idx = IndexOf(parameterName);
        if (idx >= 0) _parameters[idx] = (DataVoParameter)value;
    }

    /// <summary>
    /// Returns all parameters as a flat list for SQL substitution.
    /// </summary>
    internal IReadOnlyList<DataVoParameter> AllParameters => _parameters;
}
