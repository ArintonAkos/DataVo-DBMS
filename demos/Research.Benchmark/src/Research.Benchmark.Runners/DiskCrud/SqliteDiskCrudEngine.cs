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
    private readonly string _name;
    private string? _workingDirectory;
    private string? _dbPath;
    private SqliteConnection? _connection;
    private SqliteCommand? _insertCommand;
    private SqliteCommand? _updateCommand;
    private SqliteTransaction? _transaction;

    public SqliteDiskCrudEngine(string synchronous)
    {
        _synchronous = synchronous.ToUpperInvariant();
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
        ExecutePragma($"PRAGMA synchronous={_synchronous};");

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
        SqliteCommand command = UpdateCommand();
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

    private void DisposeCore()
    {
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
