using DuckDB.NET.Data;

namespace Research.Benchmark.Runners.SqlMatrix;

/// <summary>
/// DuckDB on a file with its default thread count. Bulk load uses the appender; queries are prepared commands
/// reused with named parameters (<c>$name</c>). DuckDB is a column store, included as the analytical reference.
/// </summary>
public sealed class DuckDbSqlMatrixEngine : ISqlMatrixEngine
{
    private readonly Dictionary<string, DuckDBCommand> _prepared = new(StringComparer.Ordinal);
    private DuckDBConnection? _connection;
    private string? _path;

    public string Name => "DuckDB";

    public string Notes => $"File database, default threads ({Environment.ProcessorCount} cores); appender for bulk load, then close and reopen; prepared commands with named parameters (DuckDB.NET).";

    public bool SupportsColumnarIndex => false;

    public long? ColumnarScans => null;

    public void Create(string directory)
    {
        Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, "matrix.duckdb");
        OpenConnection();
        Exec("""
            CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Name VARCHAR, Segment INTEGER, Region VARCHAR);
            CREATE TABLE Products (Id INTEGER PRIMARY KEY, Name VARCHAR, CategoryId INTEGER, Price DOUBLE);
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY, CustomerId INTEGER, ProductId INTEGER, ProductTag INTEGER, Status VARCHAR, Amount DOUBLE, Qty INTEGER, Discount DOUBLE, Bucket INTEGER, Sku VARCHAR);
            CREATE INDEX ix_orders_customer ON Orders (CustomerId);
            CREATE INDEX ix_orders_product ON Orders (ProductId);
            """);
    }

    public void Load(SqlMatrixData data)
    {
        using (DuckDBAppender customers = Conn.CreateAppender("Customers"))
        {
            for (int i = 1; i <= data.Customers; i++)
            {
                customers.CreateRow().AppendValue(i).AppendValue(SqlMatrixData.CustomerName(i))
                    .AppendValue(SqlMatrixData.CustomerSegment(i)).AppendValue(SqlMatrixData.CustomerRegion(i)).EndRow();
            }
        }

        using (DuckDBAppender products = Conn.CreateAppender("Products"))
        {
            for (int i = 1; i <= data.Products; i++)
            {
                products.CreateRow().AppendValue(i).AppendValue(SqlMatrixData.ProductName(i))
                    .AppendValue(SqlMatrixData.ProductCategory(i)).AppendValue(SqlMatrixData.ProductPrice(i)).EndRow();
            }
        }

        using DuckDBAppender orders = Conn.CreateAppender("Orders");
        for (int i = 1; i <= data.Orders; i++)
        {
            var row = orders.CreateRow()
                .AppendValue(i)
                .AppendValue(data.CustomerId[i])
                .AppendValue(data.ProductId[i])
                .AppendValue(data.ProductId[i])
                .AppendValue(data.Status(i))
                .AppendValue(data.Amount[i])
                .AppendValue(data.Qty[i]);
            row = data.Discount[i] is double discount ? row.AppendValue(discount) : row.AppendNullValue();
            row.AppendValue(data.Bucket[i]).AppendValue(data.Sku[i]).EndRow();
        }
    }

    public void Reopen()
    {
        CloseConnection();
        OpenConnection();
    }

    public List<object?[]> Query(SqlMatrixQuery query, SqlMatrixParameter[] parameters)
    {
        using DuckDBDataReader reader = Bind(query, parameters).ExecuteReader();
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
        using DuckDBDataReader reader = Bind(query, parameters).ExecuteReader();
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

    public void CreateColumnarIndex() => throw new NotSupportedException("DuckDB is columnar already.");

    public long SizeOnDiskBytes() => SqlMatrixRunner.DirectorySize(Path.GetDirectoryName(_path!)!);

    public void Dispose() => CloseConnection();

    private DuckDBCommand Bind(SqlMatrixQuery query, SqlMatrixParameter[] parameters)
    {
        if (!_prepared.TryGetValue(query.Sql, out DuckDBCommand? command))
        {
            command = Conn.CreateCommand();
            command.CommandText = query.Sql.Replace('@', '$');
            foreach (SqlMatrixParameter parameter in parameters)
            {
                command.Parameters.Add(new DuckDBParameter(parameter.Name, parameter.Value));
            }

            _prepared[query.Sql] = command;
        }

        foreach (SqlMatrixParameter parameter in parameters)
        {
            command.Parameters[parameter.Name].Value = parameter.Value;
        }

        return command;
    }

    private DuckDBConnection Conn => _connection ?? throw new InvalidOperationException("DuckDB is not open.");

    private void OpenConnection()
    {
        _connection = new DuckDBConnection($"Data Source={_path}");
        _connection.Open();
    }

    private void CloseConnection()
    {
        foreach (DuckDBCommand command in _prepared.Values)
        {
            command.Dispose();
        }

        _prepared.Clear();
        _connection?.Dispose();
        _connection = null;
    }

    private void Exec(string sql)
    {
        using DuckDBCommand command = Conn.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
