using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.Whitepaper;

public sealed class SqliteWhitepaperBenchmarkEngine : IWhitepaperBenchmarkEngine
{
    private readonly string _synchronous;
    private string? _workingDirectory;
    private string? _dbPath;
    private SqliteConnection? _connection;
    private SqliteCommand? _insertCommand;
    private SqliteTransaction? _insertTransaction;
    private ThreadLocal<SqliteCommand>? _threadReadCommands;
    private ThreadLocal<SqliteCommand>? _threadUpdateCommands;

    public SqliteWhitepaperBenchmarkEngine(string synchronous)
    {
        _synchronous = synchronous.ToUpperInvariant();
    }

    public string Name => $"SQLite (WAL,{_synchronous.ToLowerInvariant()})";

    public string WorkingDirectory => _workingDirectory
        ?? throw new InvalidOperationException("SQLite whitepaper engine has not been initialized.");

    public void Initialize(string workingDirectory, bool fresh)
    {
        DisposeConnections();
        _workingDirectory = workingDirectory;
        Directory.CreateDirectory(workingDirectory);
        _dbPath = Path.Combine(workingDirectory, "whitepaper.sqlite");

        if (fresh)
        {
            foreach (string suffix in new[] { string.Empty, "-wal", "-shm", "-journal" })
            {
                string path = _dbPath + suffix;
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
        }

        _connection = new SqliteConnection($"Data Source={_dbPath}");
        _connection.Open();
        ExecutePragma("PRAGMA journal_mode=WAL;");
        ExecutePragma($"PRAGMA synchronous={_synchronous};");
        ExecutePragma("PRAGMA busy_timeout=60000;");

        using SqliteCommand schema = _connection.CreateCommand();
        schema.CommandText = "CREATE TABLE IF NOT EXISTS Records (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL, Score REAL NOT NULL);";
        schema.ExecuteNonQuery();

        _insertCommand = _connection.CreateCommand();
        _insertCommand.CommandText = "INSERT INTO Records (Id, Name, Value, Score) VALUES ($id, $name, $value, $score);";
        _insertCommand.Parameters.Add("$id", SqliteType.Integer);
        _insertCommand.Parameters.Add("$name", SqliteType.Text);
        _insertCommand.Parameters.Add("$value", SqliteType.Integer);
        _insertCommand.Parameters.Add("$score", SqliteType.Real);
        _insertCommand.Prepare();

        _threadReadCommands = new ThreadLocal<SqliteCommand>(CreateThreadReadCommand, trackAllValues: true);
        _threadUpdateCommands = new ThreadLocal<SqliteCommand>(CreateThreadUpdateCommand, trackAllValues: true);
    }

    public void Preload(int records)
    {
        _insertTransaction = Connection().BeginTransaction();
        InsertCommand().Transaction = _insertTransaction;
        for (int i = 1; i <= records; i++)
        {
            InsertCommand().Parameters["$id"].Value = i;
            InsertCommand().Parameters["$name"].Value = $"name-{i}";
            InsertCommand().Parameters["$value"].Value = i;
            InsertCommand().Parameters["$score"].Value = i * 1.5d;
            InsertCommand().ExecuteNonQuery();
        }

        _insertTransaction.Commit();
        _insertTransaction.Dispose();
        _insertTransaction = null;
        InsertCommand().Transaction = null;
    }

    public FlatRecord? Read(long id)
    {
        SqliteCommand command = _threadReadCommands?.Value ?? CreateThreadReadCommand();
        command.Parameters["$id"].Value = id;
        using SqliteDataReader reader = command.ExecuteReader();
        return reader.Read()
            ? new FlatRecord(reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2), reader.GetDouble(3))
            : null;
    }

    public void Update(long id, int newValue, double newScore)
    {
        SqliteCommand command = _threadUpdateCommands?.Value ?? CreateThreadUpdateCommand();
        command.Parameters["$value"].Value = newValue;
        command.Parameters["$score"].Value = newScore;
        command.Parameters["$id"].Value = id;
        int affected = command.ExecuteNonQuery();
        if (affected != 1)
        {
            throw new InvalidOperationException($"SQLite whitepaper update affected {affected} rows for Id={id}.");
        }
    }

    public void CloseForRecovery() => DisposeConnections();

    public void OpenExisting()
    {
        string directory = WorkingDirectory;
        Initialize(directory, fresh: false);
    }

    public void Dispose() => DisposeConnections();

    private SqliteCommand CreateThreadReadCommand()
    {
        SqliteConnection connection = OpenWorkerConnection();
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name, Value, Score FROM Records WHERE Id = $id;";
        command.Parameters.Add("$id", SqliteType.Integer);
        command.Prepare();
        return command;
    }

    private SqliteCommand CreateThreadUpdateCommand()
    {
        SqliteConnection connection = OpenWorkerConnection();
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE Records SET Value = $value, Score = $score WHERE Id = $id;";
        command.Parameters.Add("$value", SqliteType.Integer);
        command.Parameters.Add("$score", SqliteType.Real);
        command.Parameters.Add("$id", SqliteType.Integer);
        command.Prepare();
        return command;
    }

    private SqliteConnection OpenWorkerConnection()
    {
        var connection = new SqliteConnection($"Data Source={_dbPath}");
        connection.Open();
        using SqliteCommand pragma = connection.CreateCommand();
        pragma.CommandText = $"PRAGMA synchronous={_synchronous}; PRAGMA busy_timeout=60000;";
        pragma.ExecuteNonQuery();
        return connection;
    }

    private SqliteConnection Connection() =>
        _connection ?? throw new InvalidOperationException("SQLite whitepaper engine has not been initialized.");

    private SqliteCommand InsertCommand() =>
        _insertCommand ?? throw new InvalidOperationException("SQLite whitepaper insert command has not been prepared.");

    private void ExecutePragma(string sql)
    {
        using SqliteCommand command = Connection().CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private void DisposeConnections()
    {
        DisposeThreadCommands(_threadReadCommands);
        DisposeThreadCommands(_threadUpdateCommands);
        _threadReadCommands = null;
        _threadUpdateCommands = null;
        _insertTransaction?.Dispose();
        _insertTransaction = null;
        _insertCommand?.Dispose();
        _insertCommand = null;
        _connection?.Dispose();
        _connection = null;
        SqliteConnection.ClearAllPools();
    }

    private static void DisposeThreadCommands(ThreadLocal<SqliteCommand>? commands)
    {
        if (commands is null)
        {
            return;
        }

        foreach (SqliteCommand command in commands.Values)
        {
            SqliteConnection? connection = command.Connection;
            command.Dispose();
            connection?.Dispose();
        }

        commands.Dispose();
    }
}
