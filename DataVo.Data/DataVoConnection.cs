using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.Transactions;

namespace DataVo.Data;

/// <summary>
/// Represents a connection to a DataVo embedded database.
/// <para>
/// Uses a connection string to configure the storage mode, the data source path and the SQL database (see
/// <see cref="DataVoConnectionStringBuilder"/>). Opening the connection initializes the storage engine, creates the
/// database when it does not exist, and selects it with <c>USE {database}</c>.
/// </para>
/// </summary>
/// <example>
/// <code>
/// using var connection = new DataVoConnection("StorageMode=Lsm;DataSource=./mydb");
/// connection.Open();
/// using var cmd = connection.CreateCommand();
/// cmd.CommandText = "SELECT 1;";
/// </code>
/// </example>
public class DataVoConnection : DbConnection
{
    private const string FallbackServerVersion = "0.5.1";
    private static readonly string s_serverVersion = ResolveServerVersion();

    private DataVoConnectionStringBuilder _builder;
    private ConnectionState _state = ConnectionState.Closed;
    private DataVoEngine? _engine;
    private string? _currentDatabase;

    /// <summary>
    /// The internal session identifier used by the DataVo engine for this connection.
    /// </summary>
    internal Guid Session { get; } = Guid.NewGuid();

    /// <summary>
    /// Gets the engine context associated with this open connection.
    /// </summary>
    internal DataVoEngine? Engine => _engine;

    /// <inheritdoc />
    /// <exception cref="InvalidOperationException">The connection is open.</exception>
    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (_state != ConnectionState.Closed)
            {
                throw new InvalidOperationException("The connection string cannot be changed while the connection is open.");
            }

            string normalized = value ?? string.Empty;
            _builder = new DataVoConnectionStringBuilder(normalized);
            _connectionString = normalized;
            _currentDatabase = null;
        }
    }
    private string _connectionString;

    /// <summary>
    /// Gets the SQL database in use: the one selected by <see cref="ChangeDatabase"/>, otherwise the connection
    /// string's database (see <see cref="DataVoConnectionStringBuilder.Database"/>).
    /// </summary>
    public override string Database => _currentDatabase ?? _builder.Database;

    /// <inheritdoc />
    public override string DataSource => _builder.DataSource;

    /// <summary>
    /// Gets the DataVo engine version, from the assembly's informational version (for example <c>0.5.1</c>).
    /// </summary>
    public override string ServerVersion => s_serverVersion;

    /// <summary>
    /// Gets the <see cref="DataVoFactory"/> that creates this provider's objects.
    /// </summary>
    protected override DbProviderFactory DbProviderFactory => DataVoFactory.Instance;

    /// <inheritdoc />
    public override ConnectionState State => _state;

    /// <summary>
    /// Creates a new connection with the specified connection string.
    /// </summary>
    /// <param name="connectionString">
    /// Semicolon-delimited key=value pairs. Supported keys:
    /// <c>StorageMode</c> and <c>DataSource</c>.
    /// </param>
    public DataVoConnection(string? connectionString)
    {
        _connectionString = connectionString ?? string.Empty;
        _builder = new DataVoConnectionStringBuilder(_connectionString);
    }

    /// <summary>
    /// Creates a connection with an empty connection string; set <see cref="ConnectionString"/> before
    /// <see cref="Open"/>. Required by <see cref="DbProviderFactory"/>, dependency injection and ORMs.
    /// </summary>
    public DataVoConnection()
        : this(string.Empty)
    {
    }

    /// <summary>
    /// Initializes the storage engine and selects the target database.
    /// </summary>
    public override void Open()
    {
        if (_state == ConnectionState.Open)
            throw new InvalidOperationException("Connection is already open.");

        var config = _builder.ToConfig();
        string database = _builder.Database;
        _engine = DataVoEngine.Initialize(config);

        try
        {
            // The SQL database is a logical name decoupled from the (possibly path-shaped) DataSource.
            ExecuteInternal($"CREATE DATABASE IF NOT EXISTS {database};");
            ExecuteInternal($"USE {database};");
        }
        catch
        {
            _engine.Dispose();
            _engine = null;
            throw;
        }

        _currentDatabase = database;
        _state = ConnectionState.Open;
    }

    /// <summary>
    /// Rolls back any uncommitted transaction and closes the connection.
    /// </summary>
    public override void Close()
    {
        if (_state == ConnectionState.Closed)
        {
            _engine?.Dispose();
            _engine = null;
            return;
        }

        if (_engine?.TransactionManager.HasActiveTransaction(Session) == true)
        {
            ExecuteInternal("ROLLBACK;");
        }

        _engine?.Dispose();
        _engine = null;
        _currentDatabase = null;
        _state = ConnectionState.Closed;
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

    /// <inheritdoc />
    public override void ChangeDatabase(string databaseName)
    {
        EnsureOpen();
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new ArgumentException("Database name is required.", nameof(databaseName));
        }

        ExecuteInternal($"USE {databaseName};");
        _currentDatabase = databaseName;
    }

    /// <summary>
    /// Starts a transaction. DataVo transactions run under snapshot isolation with first-committer-wins write
    /// conflict detection, so <see cref="IsolationLevel.Unspecified"/>, <see cref="IsolationLevel.ReadCommitted"/>,
    /// <see cref="IsolationLevel.RepeatableRead"/> and <see cref="IsolationLevel.Snapshot"/> all run as
    /// <see cref="IsolationLevel.Snapshot"/> (each is satisfied or exceeded by it).
    /// </summary>
    /// <param name="isolationLevel">The requested isolation level.</param>
    /// <exception cref="NotSupportedException"><see cref="IsolationLevel.ReadUncommitted"/>,
    /// <see cref="IsolationLevel.Serializable"/> or <see cref="IsolationLevel.Chaos"/>: DataVo cannot honour them.</exception>
    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        EnsureOpen();
        IsolationLevel effective = DataVoTransaction.ResolveIsolationLevel(isolationLevel);
        ExecuteInternal("BEGIN;");
        return new DataVoTransaction(this, effective);
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
        var engine = new QueryEngine(sql, Session, _engine);
        var results = engine.Parse();

        foreach (var result in results)
        {
            if (result.IsError)
                throw DataVoException.FromResult(result);
        }
    }

    /// <summary>Gets whether this connection's session has an open engine transaction.</summary>
    internal bool HasActiveTransaction => _engine?.TransactionManager.HasActiveTransaction(Session) == true;

    private static string ResolveServerVersion()
    {
        string? informational = typeof(DataVoConnection).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (string.IsNullOrWhiteSpace(informational))
        {
            return FallbackServerVersion;
        }

        // Drop SourceLink's "+<commit>" build metadata: callers compare versions, not commits.
        int metadata = informational!.IndexOf('+');
        return metadata > 0 ? informational.Substring(0, metadata) : informational;
    }

    private void EnsureOpen()
    {
        if (_state != ConnectionState.Open)
            throw new InvalidOperationException("Connection is not open.");
    }
}
