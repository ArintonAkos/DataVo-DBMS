using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.FlatCrud;

/// <summary>
/// SQLite flat-CRUD engine: in-memory database, an integer primary key, and prepared parameterized commands
/// for insert and point-lookup (the idiomatic relational keyed read — prepared once, executed per id).
/// Inserts run inside a single transaction.
/// </summary>
public sealed class SqliteFlatCrudEngine : IFlatCrudEngine
{
    private SqliteConnection? _connection;
    private SqliteCommand? _insertCommand;
    private SqliteParameter? _insertId, _insertName, _insertValue, _insertScore;
    private SqliteCommand? _lookupCommand;
    private SqliteParameter? _lookupId;
    private SqliteTransaction? _transaction;

    public string Name => "SQLite";

    public void Initialize()
    {
        DisposeCore();
        _connection = new SqliteConnection($"Data Source=FairPlayFlatCrud-{Guid.NewGuid():N};Mode=Memory;Cache=Shared");
        _connection.Open();

        using (SqliteCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = "CREATE TABLE Records (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL, Score REAL NOT NULL);";
            schema.ExecuteNonQuery();
        }

        _insertCommand = _connection.CreateCommand();
        _insertCommand.CommandText = "INSERT INTO Records (Id, Name, Value, Score) VALUES ($id, $name, $value, $score);";
        _insertId = _insertCommand.Parameters.Add("$id", SqliteType.Integer);
        _insertName = _insertCommand.Parameters.Add("$name", SqliteType.Text);
        _insertValue = _insertCommand.Parameters.Add("$value", SqliteType.Integer);
        _insertScore = _insertCommand.Parameters.Add("$score", SqliteType.Real);
        _insertCommand.Prepare();

        _lookupCommand = _connection.CreateCommand();
        _lookupCommand.CommandText = "SELECT Id, Name, Value, Score FROM Records WHERE Id = $id;";
        _lookupId = _lookupCommand.Parameters.Add("$id", SqliteType.Integer);
        _lookupCommand.Prepare();
    }

    public void BeginBatch()
    {
        _transaction = Connection().BeginTransaction();
        InsertCommand().Transaction = _transaction;
    }

    public void CompleteBatch()
    {
        _transaction?.Commit();
        _transaction?.Dispose();
        _transaction = null;
        InsertCommand().Transaction = null;
    }

    public void Insert(FlatRecord record)
    {
        _insertId!.Value = record.Id;
        _insertName!.Value = record.Name;
        _insertValue!.Value = record.Value;
        _insertScore!.Value = record.Score;
        InsertCommand().ExecuteNonQuery();
    }

    public FlatRecord? GetById(long id)
    {
        _lookupId!.Value = id;
        using SqliteDataReader reader = LookupCommand().ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new FlatRecord(reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2), reader.GetDouble(3));
    }

    public void Dispose() => DisposeCore();

    private SqliteConnection Connection() =>
        _connection ?? throw new InvalidOperationException("SQLite flat-CRUD engine has not been initialized.");

    private SqliteCommand InsertCommand() =>
        _insertCommand ?? throw new InvalidOperationException("SQLite flat-CRUD engine has not been initialized.");

    private SqliteCommand LookupCommand() =>
        _lookupCommand ?? throw new InvalidOperationException("SQLite flat-CRUD engine has not been initialized.");

    private void DisposeCore()
    {
        _transaction?.Dispose();
        _transaction = null;
        _insertCommand?.Dispose();
        _insertCommand = null;
        _lookupCommand?.Dispose();
        _lookupCommand = null;
        _connection?.Dispose();
        _connection = null;
    }
}
