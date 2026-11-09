using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;

namespace DataVo.Data;

/// <summary>
/// Represents a SQL command to be executed against a DataVo database.
/// <para>
/// Supports parameterized queries via <see cref="Parameters"/> and pre-parsed execution via
/// <see cref="Prepare"/> and <see cref="DataVoPreparedStatement"/>.
/// </para>
/// </summary>
/// <example>
/// <code>
/// using var cmd = connection.CreateCommand();
/// cmd.CommandText = "SELECT * FROM Users WHERE Id = @id;";
/// cmd.Prepare();
/// cmd.Parameters.AddWithValue("@id", 42);
/// using var reader = cmd.ExecuteReader();
/// </code>
/// </example>
public class DataVoCommand : DbCommand
{
    private DataVoConnection? _connection;
    private DataVoTransaction? _transaction;
    private readonly DataVoParameterCollection _parameters = new();
    private string _commandText = string.Empty;
    private DataVoPreparedStatement? _preparedStatement;
    private DataVoEngine? _preparedEngine;
    private DataVoQueryParameter[] _parameterBuffer = [];

    /// <inheritdoc />
    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set
        {
            string normalized = value ?? string.Empty;
            if (!string.Equals(_commandText, normalized, StringComparison.Ordinal))
            {
                _commandText = normalized;
                _preparedStatement = null;
            }
        }
    }

    /// <inheritdoc />
    public override int CommandTimeout { get; set; } = 30;

    /// <inheritdoc />
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <inheritdoc />
    public override bool DesignTimeVisible { get; set; }

    /// <inheritdoc />
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <inheritdoc />
    protected override DbConnection? DbConnection
    {
        get => _connection;
        set
        {
            if (!ReferenceEquals(_connection, value))
            {
                _connection = (DataVoConnection?)value;
                _preparedStatement = null;
            }
        }
    }

    /// <inheritdoc />
    protected override DbParameterCollection DbParameterCollection => _parameters;

    /// <summary>
    /// Gets the strongly-typed parameter collection for this command.
    /// </summary>
    public new DataVoParameterCollection Parameters => _parameters;

    /// <inheritdoc />
    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set => _transaction = (DataVoTransaction?)value;
    }

    /// <summary>
    /// Executes the command and returns a <see cref="DataVoDataReader"/> for reading the result set.
    /// </summary>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var results = ExecuteEngine();
        return new DataVoDataReader(results);
    }

    /// <summary>
    /// Executes a non-query command (INSERT, UPDATE, DELETE, DDL) and returns
    /// the number of rows affected, parsed from the engine's response messages.
    /// </summary>
    public override int ExecuteNonQuery()
    {
        var results = ExecuteEngine();
        int totalAffected = 0;

        for (int i = 0; i < results.Count; i++)
        {
            List<string> messages = results[i].Messages;
            for (int j = 0; j < messages.Count; j++)
            {
                if (TryParseRowsAffected(messages[j], out int affected))
                {
                    totalAffected += affected;
                }
            }
        }

        return totalAffected;
    }

    /// <summary>
    /// Executes the query and returns the first column of the first row in the result set.
    /// Returns <c>null</c> if the result set is empty.
    /// </summary>
    public override object? ExecuteScalar()
    {
        var results = ExecuteEngine();
        for (int i = 0; i < results.Count; i++)
        {
            QueryResult candidate = results[i];
            if (candidate.Data.Count > 0)
            {
                if (candidate.Fields.Count == 0)
                {
                    return null;
                }

                string firstField = candidate.Fields[0];
                return candidate.Data[0].GetValueOrDefault(firstField);
            }
        }

        return null;
    }

    /// <inheritdoc />
    public override void Cancel() { }

    /// <summary>
    /// Pre-parses and compiles the current <see cref="CommandText"/> into a reusable <see cref="DataVoPreparedStatement"/>
    /// bound to the active <see cref="DataVoConnection"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the connection is not set or not open.</exception>
    public override void Prepare()
    {
        if (_connection?.Engine == null)
        {
            throw new InvalidOperationException("Connection is not set or not open.");
        }

        BindPreparedStatement(_connection.Engine, _connection.Session);
    }

    private DataVoPreparedStatement BindPreparedStatement(DataVoEngine engine, Guid session)
    {
        _preparedEngine = engine;
        return _preparedStatement = new DataVoPreparedStatement(engine, _commandText, session);
    }

    /// <inheritdoc />
    protected override DbParameter CreateDbParameter() => new DataVoParameter();

    /// <summary>
    /// Executes the command via either the compiled <see cref="DataVoPreparedStatement"/> pipeline
    /// (when prepared or parameterized) or the standard <see cref="QueryEngine"/> pipeline.
    /// </summary>
    private List<QueryResult> ExecuteEngine()
    {
        if (_connection?.Engine == null)
        {
            throw new InvalidOperationException("Connection is not set.");
        }

        List<QueryResult> results;
        if (_preparedStatement != null || _parameters.Count > 0)
        {
            DataVoPreparedStatement statement = _preparedStatement is not null && ReferenceEquals(_preparedEngine, _connection.Engine)
                ? _preparedStatement
                : BindPreparedStatement(_connection.Engine, _connection.Session);
            ReadOnlySpan<DataVoQueryParameter> boundParameters = BuildBoundParameterSpan();
            results = statement.ExecuteAll(boundParameters, _connection.Session);
        }
        else
        {
            var engine = new QueryEngine(_commandText, _connection.Session, _connection.Engine);
            results = engine.Parse();
        }

        for (int i = 0; i < results.Count; i++)
        {
            QueryResult result = results[i];
            if (result.IsError)
            {
                // Carry the error contract to ADO.NET callers: they branch on ErrorCode, not message text.
                throw new DataVoException(string.Join("; ", result.Messages))
                {
                    ErrorCode = result.ErrorCode,
                    Line = result.ErrorLine,
                    Column = result.ErrorColumn,
                };
            }
        }

        return results;
    }

    private ReadOnlySpan<DataVoQueryParameter> BuildBoundParameterSpan()
    {
        IReadOnlyList<DataVoParameter> source = _parameters.AllParameters;
        int count = source.Count;
        if (count == 0)
        {
            return ReadOnlySpan<DataVoQueryParameter>.Empty;
        }

        if (_parameterBuffer.Length < count)
        {
            _parameterBuffer = new DataVoQueryParameter[count];
        }

        int written = 0;
        for (int i = 0; i < count; i++)
        {
            DataVoParameter parameter = source[i];
            if (string.IsNullOrEmpty(parameter.ParameterName))
            {
                continue;
            }

            _parameterBuffer[written++] = DataVoQueryParameter.FromObject(parameter.ParameterName, parameter.Value);
        }

        return _parameterBuffer.AsSpan(0, written);
    }

    private static bool TryParseRowsAffected(string message, out int count)
    {
        const string prefix = "Rows affected:";
        count = 0;
        int idx = message.IndexOf(prefix, StringComparison.OrdinalIgnoreCase);
        if (idx < 0)
        {
            return false;
        }

        ReadOnlySpan<char> slice = message.AsSpan(idx + prefix.Length).Trim();
        int end = 0;
        while (end < slice.Length && char.IsDigit(slice[end]))
        {
            end++;
        }

        return end > 0 && int.TryParse(slice[..end], out count);
    }
}
