using DuckDB.NET.Data;

namespace Research.Benchmark.Runners.SqlWorkload;

/// <summary>
/// DuckDB on a file. Bulk load uses DuckDB's appender (its documented fastest ingest path); queries
/// are prepared once and reused with parameters.
/// </summary>
public sealed class DuckDbSqlWorkloadEngine : ISqlWorkloadEngine
{
    private readonly Dictionary<string, DuckDBCommand> _prepared = new(StringComparer.Ordinal);
    private DuckDBConnection? _connection;
    private string? _path;

    public string Name => "DuckDB";

    public string Notes => "File database with its default WAL; appender for bulk load; prepared statements with parameters; GUIDs as UUID.";

    public void Create(string directory)
    {
        Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, "bench.duckdb");
        OpenConnection();
        Exec("""
            CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Name VARCHAR, Region VARCHAR);
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY, CustomerId INTEGER, Status VARCHAR, Amount DOUBLE, Qty INTEGER);
            CREATE INDEX ix_orders_customer ON Orders (CustomerId);
            CREATE TABLE GuidItems (Id UUID PRIMARY KEY, Name VARCHAR, Value INTEGER);
            """);
    }

    public void LoadOrders(SqlWorkloadData data)
    {
        using (DuckDBAppender customers = Conn.CreateAppender("Customers"))
        {
            for (int i = 1; i <= data.Customers; i++)
            {
                customers.CreateRow().AppendValue(i).AppendValue("customer-" + i).AppendValue(SqlWorkloadData.RegionOf(i)).EndRow();
            }
        }

        using DuckDBAppender orders = Conn.CreateAppender("Orders");
        for (int i = 1; i <= data.Orders; i++)
        {
            orders.CreateRow()
                .AppendValue(i)
                .AppendValue(SqlWorkloadData.CustomerOf(i, data.Customers))
                .AppendValue(SqlWorkloadData.StatusOf(i))
                .AppendValue(SqlWorkloadData.AmountOf(i))
                .AppendValue(SqlWorkloadData.QtyOf(i))
                .EndRow();
        }
    }

    public void LoadGuids(SqlWorkloadData data)
    {
        using DuckDBAppender items = Conn.CreateAppender("GuidItems");
        for (int i = 1; i <= data.Guids.Length; i++)
        {
            items.CreateRow().AppendValue(data.Guids[i - 1]).AppendValue("item-" + (i % 1000)).AppendValue(i % 97).EndRow();
        }
    }

    public long PointSelect(int id)
    {
        using DuckDBDataReader reader = Prepared("SELECT Id, CustomerId, Status, Amount, Qty FROM Orders WHERE Id = ?", id).ExecuteReader();
        return reader.Read() ? reader.GetInt32(4) : -1;
    }

    public long GuidPointSelect(Guid key) => CountRows(Prepared("SELECT Name, Value FROM GuidItems WHERE Id = ?", key));

    public long SecondaryLookup(int customerId) => CountRows(Prepared("SELECT Id, Amount FROM Orders WHERE CustomerId = ?", customerId));

    public long RangeScan(int fromId, int width) =>
        CountRows(Prepared("SELECT Id, Amount FROM Orders WHERE Id >= ? AND Id < ?", fromId, fromId + width));

    public long FilteredCount() => Convert.ToInt64(Prepared("SELECT COUNT(*) FROM Orders WHERE Status = 'paid'").ExecuteScalar());

    public long GroupByStatus()
    {
        using DuckDBDataReader reader = Prepared("SELECT Status, COUNT(*), AVG(Amount) FROM Orders GROUP BY Status").ExecuteReader();
        long total = 0;
        while (reader.Read())
        {
            total += reader.GetInt64(1);
        }

        return total;
    }

    public long JoinByRegion() =>
        CountRows(Prepared("SELECT c.Region, SUM(o.Amount) FROM Orders o JOIN Customers c ON o.CustomerId = c.Id GROUP BY c.Region"));

    public long TopTen() => CountRows(Prepared("SELECT Id, Amount FROM Orders ORDER BY Amount DESC LIMIT 10"));

    public void UpdateAmount(int id, double amount) => Prepared("UPDATE Orders SET Amount = ? WHERE Id = ?", amount, id).ExecuteNonQuery();

    public void Delete(int id) => Prepared("DELETE FROM Orders WHERE Id = ?", id).ExecuteNonQuery();

    public void Close()
    {
        foreach (DuckDBCommand command in _prepared.Values)
        {
            command.Dispose();
        }

        _prepared.Clear();
        _connection?.Dispose();
        _connection = null;
    }

    public void Reopen()
    {
        Close();
        OpenConnection();
    }

    public long SizeOnDiskBytes() => SqlWorkloadRunner.DirectorySize(Path.GetDirectoryName(_path!)!);

    public void Dispose() => Close();

    private DuckDBConnection Conn => _connection ?? throw new InvalidOperationException("DuckDB is not open.");

    private void OpenConnection()
    {
        _connection = new DuckDBConnection($"Data Source={_path}");
        _connection.Open();
    }

    private void Exec(string sql)
    {
        using DuckDBCommand command = Conn.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private DuckDBCommand Prepared(string sql, params object[] values)
    {
        if (!_prepared.TryGetValue(sql, out DuckDBCommand? command))
        {
            command = Conn.CreateCommand();
            command.CommandText = sql;
            foreach (object value in values)
            {
                command.Parameters.Add(new DuckDBParameter { Value = value });
            }

            _prepared[sql] = command;
        }

        for (int i = 0; i < values.Length; i++)
        {
            command.Parameters[i].Value = values[i];
        }

        return command;
    }

    private static long CountRows(DuckDBCommand command)
    {
        using DuckDBDataReader reader = command.ExecuteReader();
        long rows = 0;
        while (reader.Read())
        {
            rows++;
        }

        return rows;
    }
}
