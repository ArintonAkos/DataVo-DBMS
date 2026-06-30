using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DiskCrud;

/// <summary>
/// SQLite disk-CRUD engine: a file-based database in <c>journal_mode=WAL</c>, with a configurable
/// <c>synchronous</c> level so we can compare like-for-like against DataVo's two durability modes:
/// <list type="bullet">
/// <item><c>synchronous=NORMAL</c> — the recommended WAL pairing; commits do not fsync (only checkpoints do). Comparable to DataVo (Disk).</item>
/// <item><c>synchronous=FULL</c> — fsync the WAL on every commit (power-crash durable). Comparable to DataVo (Disk+fsync).</item>
/// </list>
/// Inserts run inside a single transaction; updates run as individual autocommit statements (one commit each).
/// </summary>
public sealed class SqliteDiskCrudEngine : IDiskCrudEngine
{
    private readonly string _synchronous;
    private readonly bool _fullFsync;
    private readonly string _name;
    private string? _workingDirectory;
    private string? _dbPath;
    private SqliteConnection? _connection;
    private SqliteCommand? _insertCommand;
    private SqliteCommand? _updateCommand;
    private SqliteTransaction? _transaction;

    // Concurrent updates: SQLite forbids sharing a connection/command across threads, and WAL serializes
    // writers through its own write lock. Each writer thread therefore gets its own connection + prepared
    // UPDATE command (the canonical multi-writer pattern); busy_timeout lets writers wait for the lock
    // instead of erroring with SQLITE_BUSY.
    private ThreadLocal<SqliteCommand>? _threadUpdateCommands;

    public SqliteDiskCrudEngine(string synchronous)
    {
        _synchronous = synchronous.ToUpperInvariant();
        _fullFsync = string.Equals(_synchronous, "FULL", StringComparison.Ordinal);
        _name = $"SQLite (WAL,{_synchronous.ToLowerInvariant()})";
    }

    public string Name => _name;

    public void Initialize(string workingDirectory)
    {
        DisposeCore();
        _workingDirectory = workingDirectory;
        Directory.CreateDirectory(workingDirectory);
        _dbPath = Path.Combine(workingDirectory, "diskcrud.sqlite");

        // Start from a clean database file (plus any stale WAL/SHM sidecars).
        foreach (string suffix in new[] { string.Empty, "-wal", "-shm", "-journal" })
        {
            string path = _dbPath + suffix;
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }

        _connection = new SqliteConnection($"Data Source={_dbPath}");
        _connection.Open();

        ExecutePragma("PRAGMA journal_mode=WAL;");
        ExecutePragma(CreateDurabilityPragmas(includeBusyTimeout: false));

        using (SqliteCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = "CREATE TABLE Records (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL, Score REAL NOT NULL);";
            schema.ExecuteNonQuery();
        }

        _insertCommand = _connection.CreateCommand();
        _insertCommand.CommandText = "INSERT INTO Records (Id, Name, Value, Score) VALUES ($id, $name, $value, $score);";
        _insertCommand.Parameters.Add("$id", SqliteType.Integer);
        _insertCommand.Parameters.Add("$name", SqliteType.Text);
        _insertCommand.Parameters.Add("$value", SqliteType.Integer);
        _insertCommand.Parameters.Add("$score", SqliteType.Real);
        _insertCommand.Prepare();

        _updateCommand = _connection.CreateCommand();
        _updateCommand.CommandText = "UPDATE Records SET Value = $value, Score = $score WHERE Id = $id;";
        _updateCommand.Parameters.Add("$value", SqliteType.Integer);
        _updateCommand.Parameters.Add("$score", SqliteType.Real);
        _updateCommand.Parameters.Add("$id", SqliteType.Integer);
        _updateCommand.Prepare();

        _threadUpdateCommands = new ThreadLocal<SqliteCommand>(CreateThreadUpdateCommand, trackAllValues: true);
    }

    /// <summary>
    /// Opens a per-thread WAL connection and prepares its own UPDATE command, so concurrent writer threads
    /// never share SQLite state. The main connection's <see cref="_updateCommand"/> serves single-writer runs.
    /// </summary>
    private SqliteCommand CreateThreadUpdateCommand()
    {
        var connection = new SqliteConnection($"Data Source={_dbPath}");
        connection.Open();

        using (SqliteCommand pragma = connection.CreateCommand())
        {
            pragma.CommandText = CreateDurabilityPragmas(includeBusyTimeout: true);
            pragma.ExecuteNonQuery();
        }

        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE Records SET Value = $value, Score = $score WHERE Id = $id;";
        command.Parameters.Add("$value", SqliteType.Integer);
        command.Parameters.Add("$score", SqliteType.Real);
        command.Parameters.Add("$id", SqliteType.Integer);
        command.Prepare();
        return command;
    }

    public void BeginInsertBatch()
    {
        _transaction = Connection().BeginTransaction();
        InsertCommand().Transaction = _transaction;
    }

    public void CompleteInsertBatch()
    {
        _transaction?.Commit();
        _transaction?.Dispose();
        _transaction = null;
        InsertCommand().Transaction = null;
    }

    public void BeginUpdateBatch()
    {
        // Keep the update phase in SQLite autocommit mode. For WAL,FULL plus fullfsync this intentionally
        // forces one physical synchronization boundary per point update, matching the hardware-durability
        // hypothesis tested by the disk-crud-wal scenario.
    }

    public void Insert(FlatRecord record)
    {
        SqliteCommand command = InsertCommand();
        command.Parameters["$id"].Value = record.Id;
        command.Parameters["$name"].Value = record.Name;
        command.Parameters["$value"].Value = record.Value;
        command.Parameters["$score"].Value = record.Score;
        command.ExecuteNonQuery();
    }

    public void Update(long id, int newValue, double newScore)
    {
        // Use the calling thread's own connection/command so concurrent writers stay isolated.
        SqliteCommand command = _threadUpdateCommands?.Value ?? UpdateCommand();
        command.Parameters["$value"].Value = newValue;
        command.Parameters["$score"].Value = newScore;
        command.Parameters["$id"].Value = id;
        int affected = command.ExecuteNonQuery();

        if (affected != 1)
        {
            throw new InvalidOperationException(
                $"SQLite disk-CRUD update for Id={id} affected {affected} rows (expected 1).");
        }
    }

    public void CompleteUpdateBatch()
    {
        // See BeginUpdateBatch: SQLite updates are intentionally unbatched for the fullfsync durability test.
    }

    public void Dispose() => DisposeCore();

    private SqliteConnection Connection() =>
        _connection ?? throw new InvalidOperationException("SQLite disk-CRUD engine has not been initialized.");

    private SqliteCommand InsertCommand() =>
        _insertCommand ?? throw new InvalidOperationException("SQLite disk-CRUD engine has not been initialized.");

    private SqliteCommand UpdateCommand() =>
        _updateCommand ?? throw new InvalidOperationException("SQLite disk-CRUD engine has not been initialized.");

    private void ExecutePragma(string sql)
    {
        using SqliteCommand command = Connection().CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private string CreateDurabilityPragmas(bool includeBusyTimeout)
    {
        string sql = $"PRAGMA synchronous={_synchronous};";

        if (_fullFsync)
        {
            sql += " PRAGMA fullfsync=1;";
        }

        if (includeBusyTimeout)
        {
            sql += " PRAGMA busy_timeout=60000;";
        }

        return sql;
    }

    private void DisposeCore()
    {
        if (_threadUpdateCommands is not null)
        {
            foreach (SqliteCommand command in _threadUpdateCommands.Values)
            {
                SqliteConnection? threadConnection = command.Connection;
                command.Dispose();
                threadConnection?.Dispose();
            }

            _threadUpdateCommands.Dispose();
            _threadUpdateCommands = null;
        }

        _transaction?.Dispose();
        _transaction = null;
        _insertCommand?.Dispose();
        _insertCommand = null;
        _updateCommand?.Dispose();
        _updateCommand = null;
        _connection?.Dispose();
        _connection = null;
        SqliteConnection.ClearAllPools();

        if (_workingDirectory is not null && Directory.Exists(_workingDirectory))
        {
            try { Directory.Delete(_workingDirectory, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
