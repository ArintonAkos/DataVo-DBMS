using System.Data;
using System.Data.Common;
using DataVo.Core.Parser;
using DataVo.Core.StorageEngine;
using DataVo.Core.Transactions;

namespace DataVo.Data;

/// <summary>
/// Represents a connection to a DataVo embedded database.
/// <para>
/// Uses a connection string to configure the storage mode, data source path,
/// and WAL settings. Opening the connection initializes the storage engine
/// and automatically runs <c>USE {database}</c>.
/// </para>
/// </summary>
/// <example>
/// <code>
/// using var connection = new DataVoConnection("StorageMode=Disk;DataSource=./mydb");
/// connection.Open();
/// using var cmd = connection.CreateCommand();
/// cmd.CommandText = "SELECT 1;";
/// </code>
/// </example>
public class DataVoConnection : DbConnection
{
    private readonly DataVoConnectionStringBuilder _builder;
    private ConnectionState _state = ConnectionState.Closed;

    /// <summary>
    /// The internal session identifier used by the DataVo engine for this connection.
    /// </summary>
    internal Guid Session { get; } = Guid.NewGuid();

    /// <inheritdoc />
    public override string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = value ?? string.Empty;
    }
    private string _connectionString;

    /// <inheritdoc />
    public override string Database => _builder.DataSource;

    /// <inheritdoc />
    public override string DataSource => _builder.DataSource;

    /// <inheritdoc />
    public override string ServerVersion => "DataVo 1.0";

    /// <inheritdoc />
    public override ConnectionState State => _state;

    /// <summary>
    /// Creates a new connection with the specified connection string.
    /// </summary>
    /// <param name="connectionString">
    /// Semicolon-delimited key=value pairs. Supported keys:
    /// <c>StorageMode</c>, <c>DataSource</c>, <c>WalEnabled</c>.
    /// </param>
    public DataVoConnection(string connectionString)
    {
        _connectionString = connectionString;
        _builder = new DataVoConnectionStringBuilder(connectionString);
    }

    /// <summary>
    /// Initializes the storage engine and selects the target database.
    /// </summary>
    public override void Open()
    {
        if (_state == ConnectionState.Open)
            throw new InvalidOperationException("Connection is already open.");

        var config = _builder.ToConfig();
        StorageContext.Initialize(config);

        // Create and use the database
        ExecuteInternal($"CREATE DATABASE IF NOT EXISTS {_builder.DataSource};");
        ExecuteInternal($"USE {_builder.DataSource};");

        _state = ConnectionState.Open;
    }

    /// <summary>
    /// Rolls back any uncommitted transaction and closes the connection.
    /// </summary>
    public override void Close()
    {
        if (_state == ConnectionState.Closed) return;

        if (TransactionManager.Instance.HasActiveTransaction(Session))
        {
            ExecuteInternal("ROLLBACK;");
        }

        _state = ConnectionState.Closed;
    }

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName)
    {
        EnsureOpen();
        ExecuteInternal($"USE {databaseName};");
    }

    /// <inheritdoc />
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        EnsureOpen();
        ExecuteInternal("BEGIN;");
        return new DataVoTransaction(this);
    }

    /// <inheritdoc />
    protected override DbCommand CreateDbCommand()
    {
        return new DataVoCommand { Connection = this };
    }

    /// <summary>
    /// Convenience method to create a <see cref="DataVoCommand"/> with the strongly-typed connection.
    /// </summary>
    public new DataVoCommand CreateCommand()
    {
        return new DataVoCommand { Connection = this };
    }

    /// <summary>
    /// Executes raw SQL against the engine for internal housekeeping
    /// (e.g. <c>USE</c>, <c>BEGIN</c>, <c>COMMIT</c>, <c>ROLLBACK</c>).
    /// </summary>
    internal void ExecuteInternal(string sql)
    {
        var engine = new QueryEngine(sql, Session);
        var results = engine.Parse();

        foreach (var result in results)
        {
            if (result.IsError)
                throw new DataVoException(string.Join("; ", result.Messages));
        }
    }

    private void EnsureOpen()
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
    }
}
