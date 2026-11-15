using Microsoft.Data.Sqlite;

namespace Research.Benchmark.Runners.HybridVector;

/// <summary>
/// SQLite with <c>sqlite-vec</c>, used the way its documentation describes filtered KNN: a <c>vec0</c> table with
/// <c>distance_metric=cosine</c> whose filter columns are <em>metadata</em> columns, so the filter is applied
/// inside the KNN scan (<c>emb MATCH @q AND k = 10 AND category = @c ...</c>). vec0 is an exact brute-force scan.
/// The join shapes run the KNN in a materialised CTE and join <c>Authors</c> to its ten rows. File database,
/// WAL, <c>synchronous=NORMAL</c>. The extension path comes from <c>SQLITE_VEC_PATH</c>.
/// </summary>
public sealed class SqliteVecHybridVectorEngine : IHybridVectorEngine
{
    private readonly Dictionary<HybridShape, SqliteCommand> _prepared = [];
    private SqliteConnection? _connection;
    private string _authorFilterForm = "";

    public string Name => "SQLite + sqlite-vec";

    public string Notes =>
        "File, WAL, synchronous=NORMAL; vec0(id, emb float[d] distance_metric=cosine, category, price, author_id as metadata columns); exact KNN with metadata filters; "
        + $"joins via MATERIALIZED CTE; author-country filter: {_authorFilterForm}.";

    public void Initialize(string directory, int dimensions)
    {
        string? extension = Environment.GetEnvironmentVariable("SQLITE_VEC_PATH");
        if (string.IsNullOrWhiteSpace(extension) || !File.Exists(extension))
        {
            throw new InvalidOperationException("sqlite-vec not available: set SQLITE_VEC_PATH to the vec0 loadable library.");
        }

        Directory.CreateDirectory(directory);
        _connection = new SqliteConnection($"Data Source={Path.Combine(directory, "hybrid.sqlite")};Pooling=False");
        _connection.Open();
        _connection.EnableExtensions(true);
        _connection.LoadExtension(Path.GetFullPath(extension));
        Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
        Exec($"CREATE VIRTUAL TABLE vd USING vec0(id INTEGER PRIMARY KEY, emb float[{dimensions}] distance_metric=cosine, category INTEGER, price FLOAT, author_id INTEGER)");
        Exec("CREATE TABLE Authors (Id INTEGER PRIMARY KEY, Name TEXT, Country TEXT)");
    }

    public void Load(HybridVectorData data)
    {
        using (SqliteTransaction tx = Conn.BeginTransaction())
        {
            using SqliteCommand authors = Conn.CreateCommand();
            authors.Transaction = tx;
            authors.CommandText = "INSERT INTO Authors VALUES ($i, $n, $c)";
            SqliteParameter ai = authors.Parameters.Add("$i", SqliteType.Integer);
            SqliteParameter an = authors.Parameters.Add("$n", SqliteType.Text);
            SqliteParameter ac = authors.Parameters.Add("$c", SqliteType.Text);
            authors.Prepare();
            for (int id = 1; id <= data.Authors; id++)
            {
                ai.Value = id;
                an.Value = HybridVectorData.AuthorName(id);
                ac.Value = HybridVectorData.AuthorCountry(id);
                authors.ExecuteNonQuery();
            }

            using SqliteCommand docs = Conn.CreateCommand();
            docs.Transaction = tx;
            docs.CommandText = "INSERT INTO vd (id, emb, category, price, author_id) VALUES ($i, $e, $c, $p, $a)";
            SqliteParameter di = docs.Parameters.Add("$i", SqliteType.Integer);
            SqliteParameter de = docs.Parameters.Add("$e", SqliteType.Blob);
            SqliteParameter dc = docs.Parameters.Add("$c", SqliteType.Integer);
            SqliteParameter dp = docs.Parameters.Add("$p", SqliteType.Real);
            SqliteParameter da = docs.Parameters.Add("$a", SqliteType.Integer);
            docs.Prepare();
            for (int id = 1; id <= data.Documents; id++)
            {
                di.Value = id;
                de.Value = Bytes(data.Embeddings[id]);
                dc.Value = data.Category[id];
                dp.Value = data.Price[id];
                da.Value = data.AuthorId[id];
                docs.ExecuteNonQuery();
            }

            tx.Commit();
        }

        // The idiomatic author filter is a KNN constrained to the matching author ids. Use it when this
        // sqlite-vec build accepts IN on a metadata column; otherwise fall back to an exact distance scan.
        try
        {
            using SqliteCommand probe = Conn.CreateCommand();
            probe.CommandText = InAuthorSql;
            probe.Parameters.AddWithValue("@q", Bytes(data.QueryVectors[0]));
            probe.Parameters.AddWithValue("@k", HybridVectorData.AuthorCountry(0));
            using SqliteDataReader reader = probe.ExecuteReader();
            while (reader.Read())
            {
            }

            _authorFilterForm = "KNN with author_id IN (SELECT Id FROM Authors WHERE Country = @k)";
        }
        catch (SqliteException)
        {
            _authorFilterForm = "exact scan: JOIN + WHERE + ORDER BY vec_distance_cosine LIMIT 10 (vec0 rejected IN on metadata)";
        }
    }

    private const string InAuthorSql =
        "WITH knn AS MATERIALIZED (SELECT id, distance, author_id FROM vd WHERE emb MATCH @q AND k = 10 AND author_id IN (SELECT Id FROM Authors WHERE Country = @k)) "
        + "SELECT knn.id, a.Name FROM knn JOIN Authors a ON a.Id = knn.author_id ORDER BY knn.distance, knn.id";

    public string SqlFor(HybridShape shape) => shape switch
    {
        HybridShape.V0Unfiltered => "SELECT id FROM vd WHERE emb MATCH @q AND k = 10 ORDER BY distance",
        HybridShape.V1Selective => "SELECT id FROM vd WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p ORDER BY distance",
        HybridShape.V2Broad => "SELECT id FROM vd WHERE emb MATCH @q AND k = 10 AND price < @p ORDER BY distance",
        HybridShape.V3JoinDocFilter =>
            "WITH knn AS MATERIALIZED (SELECT id, distance, author_id FROM vd WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p) "
            + "SELECT knn.id, a.Name FROM knn JOIN Authors a ON a.Id = knn.author_id ORDER BY knn.distance, knn.id",
        HybridShape.V4JoinAuthorFilter => _authorFilterForm.StartsWith("KNN", StringComparison.Ordinal)
            ? InAuthorSql
            : "SELECT d.id, a.Name FROM vd d JOIN Authors a ON a.Id = d.author_id WHERE a.Country = @k ORDER BY vec_distance_cosine(d.emb, @q), d.id LIMIT 10",
        _ => throw new ArgumentOutOfRangeException(nameof(shape)),
    };

    public IReadOnlyList<long> Search(HybridShape shape, float[] query, int queryIndex)
    {
        if (!_prepared.TryGetValue(shape, out SqliteCommand? command))
        {
            command = Conn.CreateCommand();
            command.CommandText = SqlFor(shape);
            command.Parameters.Add("@q", SqliteType.Blob);
            if (command.CommandText.Contains("@c", StringComparison.Ordinal)) command.Parameters.Add("@c", SqliteType.Integer);
            if (command.CommandText.Contains("@p", StringComparison.Ordinal)) command.Parameters.Add("@p", SqliteType.Real);
            if (command.CommandText.Contains("@k", StringComparison.Ordinal)) command.Parameters.Add("@k", SqliteType.Text);
            command.Prepare();
            _prepared[shape] = command;
        }

        (int category, double price, string country) = HybridVectorData.ParametersFor(queryIndex);
        command.Parameters["@q"].Value = Bytes(query);
        if (command.Parameters.Contains("@c")) command.Parameters["@c"].Value = category;
        if (command.Parameters.Contains("@p")) command.Parameters["@p"].Value = price;
        if (command.Parameters.Contains("@k")) command.Parameters["@k"].Value = country;

        var ids = new List<long>(HybridVectorData.TopK);
        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }

    public HybridRouting? Explain(HybridShape shape, float[] query, int queryIndex) => null;

    public void Dispose()
    {
        foreach (SqliteCommand command in _prepared.Values)
        {
            command.Dispose();
        }

        _prepared.Clear();
        _connection?.Dispose();
        _connection = null;
    }

    private static byte[] Bytes(float[] vector)
    {
        var bytes = new byte[vector.Length * sizeof(float)];
        Buffer.BlockCopy(vector, 0, bytes, 0, bytes.Length);
        return bytes;
    }

    private SqliteConnection Conn => _connection ?? throw new InvalidOperationException("SQLite is not open.");

    private void Exec(string sql)
    {
        using SqliteCommand command = Conn.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
