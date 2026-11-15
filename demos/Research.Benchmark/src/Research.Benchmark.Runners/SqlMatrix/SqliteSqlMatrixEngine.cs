using Microsoft.Data.Sqlite;

namespace Research.Benchmark.Runners.SqlMatrix;

/// <summary>
/// SQLite on a file in WAL mode with <c>synchronous=NORMAL</c> (no flush per commit, the setting comparable to
/// DataVo's Relaxed LSM). The load runs in one transaction and is followed by <c>ANALYZE</c>, as an
/// application that cares about plans would do. Every query is a prepared command reused with parameters.
/// </summary>
public sealed class SqliteSqlMatrixEngine : ISqlMatrixEngine
{
    private readonly Dictionary<string, SqliteCommand> _prepared = new(StringComparer.Ordinal);
    private SqliteConnection? _connection;
    private string? _path;

    public string Name => "SQLite";

    public string Notes => "File, journal_mode=WAL, synchronous=NORMAL; load in one transaction then ANALYZE; prepared commands with parameters (Microsoft.Data.Sqlite).";

    public bool SupportsColumnarIndex => false;

    public long? ColumnarScans => null;

    public void Create(string directory)
    {
        Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, "matrix.sqlite");
        OpenConnection();
        Exec("""
            CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Name TEXT, Segment INTEGER, Region TEXT);
            CREATE TABLE Products (Id INTEGER PRIMARY KEY, Name TEXT, CategoryId INTEGER, Price REAL);
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY, CustomerId INTEGER, ProductId INTEGER, ProductTag INTEGER, Status TEXT, Amount REAL, Qty INTEGER, Discount REAL, Bucket INTEGER, Sku TEXT);
            CREATE INDEX ix_orders_customer ON Orders (CustomerId);
            CREATE INDEX ix_orders_product ON Orders (ProductId);
            """);
    }

    public void Load(SqlMatrixData data)
    {
        using (SqliteTransaction tx = Conn.BeginTransaction())
        {
            Insert(tx, "INSERT INTO Customers VALUES ($a, $b, $c, $d)", data.Customers, static (p, i) =>
            {
                p[0].Value = i;
                p[1].Value = SqlMatrixData.CustomerName(i);
                p[2].Value = SqlMatrixData.CustomerSegment(i);
                p[3].Value = SqlMatrixData.CustomerRegion(i);
            });
            Insert(tx, "INSERT INTO Products VALUES ($a, $b, $c, $d)", data.Products, static (p, i) =>
            {
                p[0].Value = i;
                p[1].Value = SqlMatrixData.ProductName(i);
                p[2].Value = SqlMatrixData.ProductCategory(i);
                p[3].Value = SqlMatrixData.ProductPrice(i);
            });
            Insert(tx, "INSERT INTO Orders VALUES ($a, $b, $c, $d, $e, $f, $g, $h, $i, $j)", data.Orders, (p, i) =>
            {
                p[0].Value = i;
                p[1].Value = data.CustomerId[i];
                p[2].Value = data.ProductId[i];
                p[3].Value = data.ProductId[i];
                p[4].Value = data.Status(i);
                p[5].Value = data.Amount[i];
                p[6].Value = data.Qty[i];
                p[7].Value = data.Discount[i] is double discount ? discount : DBNull.Value;
                p[8].Value = data.Bucket[i];
                p[9].Value = data.Sku[i];
            });
            tx.Commit();
        }

        Exec("ANALYZE;");
    }

    public void Reopen()
    {
        CloseConnection();
        OpenConnection();
    }

    public List<object?[]> Query(SqlMatrixQuery query, SqlMatrixParameter[] parameters)
    {
        using SqliteDataReader reader = Bind(query, parameters).ExecuteReader();
        var rows = new List<object?[]>();
        while (reader.Read())
        {
            var row = new object?[reader.FieldCount];
            for (int i = 0; i < row.Length; i++)
            {
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    public long Execute(SqlMatrixQuery query, SqlMatrixParameter[] parameters)
    {
        using SqliteDataReader reader = Bind(query, parameters).ExecuteReader();
        long rows = 0;
        int fields = reader.FieldCount;
        while (reader.Read())
        {
            for (int i = 0; i < fields; i++)
            {
                _ = reader.GetValue(i);
            }

            rows++;
        }

        return rows;
    }

    public void CreateColumnarIndex() => throw new NotSupportedException("SQLite has no columnar index.");

    public long SizeOnDiskBytes() => SqlMatrixRunner.DirectorySize(Path.GetDirectoryName(_path!)!);

    public void Dispose() => CloseConnection();

    private SqliteCommand Bind(SqlMatrixQuery query, SqlMatrixParameter[] parameters)
    {
        if (!_prepared.TryGetValue(query.Sql, out SqliteCommand? command))
        {
            command = Conn.CreateCommand();
            command.CommandText = query.Sql;
            foreach (SqlMatrixParameter parameter in parameters)
            {
                command.Parameters.Add(new SqliteParameter("@" + parameter.Name, parameter.Value));
            }

            command.Prepare();
            _prepared[query.Sql] = command;
        }

        foreach (SqlMatrixParameter parameter in parameters)
        {
            command.Parameters["@" + parameter.Name].Value = parameter.Value;
        }

        return command;
    }

    private void Insert(SqliteTransaction tx, string sql, int count, Action<SqliteParameter[], int> fill)
    {
        using SqliteCommand command = Conn.CreateCommand();
        command.Transaction = tx;
        command.CommandText = sql;
        int parameterCount = sql.Count(static c => c == '$');
        var parameters = new SqliteParameter[parameterCount];
        for (int i = 0; i < parameterCount; i++)
        {
            // No declared type: the binding follows each value's CLR type, as with the query parameters.
            parameters[i] = new SqliteParameter("$" + (char)('a' + i), DBNull.Value);
            command.Parameters.Add(parameters[i]);
        }

        command.Prepare();
        for (int i = 1; i <= count; i++)
        {
            fill(parameters, i);
            command.ExecuteNonQuery();
        }
    }

    private SqliteConnection Conn => _connection ?? throw new InvalidOperationException("SQLite is not open.");

    private void OpenConnection()
    {
        _connection = new SqliteConnection($"Data Source={_path};Pooling=False");
        _connection.Open();
        Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
    }

    private void CloseConnection()
    {
        foreach (SqliteCommand command in _prepared.Values)
        {
            command.Dispose();
        }

        _prepared.Clear();
        _connection?.Dispose();
        _connection = null;
        SqliteConnection.ClearAllPools();
    }

    private void Exec(string sql)
    {
        using SqliteCommand command = Conn.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
