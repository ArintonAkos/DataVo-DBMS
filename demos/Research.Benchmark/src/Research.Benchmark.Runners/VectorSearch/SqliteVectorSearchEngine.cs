using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.VectorSearch;

/// <summary>
/// SQLite vector-search engine backed by the native <c>sqlite-vec</c> (<c>vec0</c>) extension: vectors live
/// in a <c>vec0</c> virtual table and Top-K is a KNN query (<c>embedding MATCH ? ORDER BY distance LIMIT
/// k</c>). The extension is a loadable native library; its path is taken from the <c>SQLITE_VEC_PATH</c>
/// environment variable. If the extension is absent or cannot be loaded in this environment,
/// <see cref="Initialize"/> throws and the host reports this engine as <c>n/a</c> (never a fabricated number).
/// </summary>
public sealed class SqliteVectorSearchEngine : IVectorSearchEngine
{
    private SqliteConnection? _connection;
    private SqliteCommand? _insertCommand;
    private SqliteParameter? _insertRowId, _insertEmbedding;
    private SqliteCommand? _searchCommand;
    private SqliteParameter? _searchEmbedding, _searchK;
    private SqliteTransaction? _transaction;

    public string Name => "SQLite";

    public void Initialize(int dimensions)
    {
        DisposeCore();

        string? extensionPath = Environment.GetEnvironmentVariable("SQLITE_VEC_PATH");
        if (string.IsNullOrWhiteSpace(extensionPath) || !File.Exists(extensionPath))
        {
            throw new InvalidOperationException(
                "sqlite-vec extension not available: set SQLITE_VEC_PATH to a vec0 loadable library.");
        }

        _connection = new SqliteConnection($"Data Source=FairPlayVector-{Guid.NewGuid():N};Mode=Memory;Cache=Shared");
        _connection.Open();
        _connection.EnableExtensions(true);
        _connection.LoadExtension(extensionPath); // throws if the bundle/library can't load it -> n/a

        using (SqliteCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = $"CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[{dimensions}]);";
            schema.ExecuteNonQuery();
        }

        _insertCommand = _connection.CreateCommand();
        _insertCommand.CommandText = "INSERT INTO vec_items(rowid, embedding) VALUES ($id, $emb);";
        _insertRowId = _insertCommand.Parameters.Add("$id", SqliteType.Integer);
        _insertEmbedding = _insertCommand.Parameters.Add("$emb", SqliteType.Blob);
        _insertCommand.Prepare();

        _searchCommand = _connection.CreateCommand();
        _searchCommand.CommandText = "SELECT rowid FROM vec_items WHERE embedding MATCH $q ORDER BY distance LIMIT $k;";
        _searchEmbedding = _searchCommand.Parameters.Add("$q", SqliteType.Blob);
        _searchK = _searchCommand.Parameters.Add("$k", SqliteType.Integer);
        _searchCommand.Prepare();
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

    public void Insert(long id, float[] vector)
    {
        _insertRowId!.Value = id;
        _insertEmbedding!.Value = ToBytes(vector);
        InsertCommand().ExecuteNonQuery();
    }

    public IReadOnlyList<long> Search(float[] query, int k)
    {
        _searchEmbedding!.Value = ToBytes(query);
        _searchK!.Value = k;

        var ids = new List<long>(k);
        using SqliteDataReader reader = SearchCommand().ExecuteReader();
        while (reader.Read())
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }

    public void Dispose() => DisposeCore();

    private static byte[] ToBytes(float[] vector)
    {
        var bytes = new byte[vector.Length * sizeof(float)];
        Buffer.BlockCopy(vector, 0, bytes, 0, bytes.Length);
        return bytes;
    }

    private SqliteConnection Connection() =>
        _connection ?? throw new InvalidOperationException("SQLite vector engine has not been initialized.");

    private SqliteCommand InsertCommand() =>
        _insertCommand ?? throw new InvalidOperationException("SQLite vector engine has not been initialized.");

    private SqliteCommand SearchCommand() =>
        _searchCommand ?? throw new InvalidOperationException("SQLite vector engine has not been initialized.");

    private void DisposeCore()
    {
        _transaction?.Dispose();
        _transaction = null;
        _insertCommand?.Dispose();
        _insertCommand = null;
        _searchCommand?.Dispose();
        _searchCommand = null;
        _connection?.Dispose();
        _connection = null;
    }
}
