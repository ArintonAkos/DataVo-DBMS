using Microsoft.Data.Sqlite;

namespace Research.Benchmark.Runners.SqlWorkload;

/// <summary>
/// SQLite in WAL mode with synchronous=NORMAL (no fsync per commit, the setting comparable to
/// DataVo's relaxed LSM mode). Every statement is prepared once and reused with parameters, which is
/// the fastest path Microsoft.Data.Sqlite offers.
/// </summary>
public sealed class SqliteSqlWorkloadEngine : ISqlWorkloadEngine
{
    private readonly Dictionary<string, SqliteCommand> _prepared = new(StringComparer.Ordinal);
    private SqliteConnection? _connection;
    private string? _path;

    public string Name => "SQLite";

    public string Notes => "WAL, synchronous=NORMAL; prepared statements with parameters; bulk load in one transaction; GUIDs as 16-byte BLOB keys in a WITHOUT ROWID table.";

    public void Create(string directory)
    {
        Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, "bench.sqlite");
        OpenConnection();
        Exec("""
            CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Name TEXT, Region TEXT);
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY, CustomerId INTEGER, Status TEXT, Amount REAL, Qty INTEGER);
            CREATE INDEX ix_orders_customer ON Orders (CustomerId);
            CREATE TABLE GuidItems (Id BLOB PRIMARY KEY, Name TEXT, Value INTEGER) WITHOUT ROWID;
            """);
    }

    public void LoadOrders(SqlWorkloadData data)
    {
        using SqliteTransaction tx = Conn.BeginTransaction();
        using (SqliteCommand customers = Conn.CreateCommand())
        {
            customers.Transaction = tx;
            customers.CommandText = "INSERT INTO Customers (Id, Name, Region) VALUES ($a, $b, $c)";
            SqliteParameter a = customers.Parameters.Add("$a", SqliteType.Integer);
            SqliteParameter b = customers.Parameters.Add("$b", SqliteType.Text);
            SqliteParameter c = customers.Parameters.Add("$c", SqliteType.Text);
            customers.Prepare();
            for (int i = 1; i <= data.Customers; i++)
            {
                a.Value = i;
                b.Value = "customer-" + i;
                c.Value = SqlWorkloadData.RegionOf(i);
                customers.ExecuteNonQuery();
            }
        }

        using (SqliteCommand orders = Conn.CreateCommand())
        {
            orders.Transaction = tx;
            orders.CommandText = "INSERT INTO Orders (Id, CustomerId, Status, Amount, Qty) VALUES ($a, $b, $c, $d, $e)";
            SqliteParameter a = orders.Parameters.Add("$a", SqliteType.Integer);
            SqliteParameter b = orders.Parameters.Add("$b", SqliteType.Integer);
            SqliteParameter c = orders.Parameters.Add("$c", SqliteType.Text);
            SqliteParameter d = orders.Parameters.Add("$d", SqliteType.Real);
            SqliteParameter e = orders.Parameters.Add("$e", SqliteType.Integer);
            orders.Prepare();
            for (int i = 1; i <= data.Orders; i++)
            {
                a.Value = i;
                b.Value = SqlWorkloadData.CustomerOf(i, data.Customers);
                c.Value = SqlWorkloadData.StatusOf(i);
                d.Value = SqlWorkloadData.AmountOf(i);
                e.Value = SqlWorkloadData.QtyOf(i);
                orders.ExecuteNonQuery();
            }
        }

        tx.Commit();
    }

    public void LoadGuids(SqlWorkloadData data)
    {
        using SqliteTransaction tx = Conn.BeginTransaction();
        using SqliteCommand insert = Conn.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = "INSERT INTO GuidItems (Id, Name, Value) VALUES ($a, $b, $c)";
        SqliteParameter a = insert.Parameters.Add("$a", SqliteType.Blob);
        SqliteParameter b = insert.Parameters.Add("$b", SqliteType.Text);
        SqliteParameter c = insert.Parameters.Add("$c", SqliteType.Integer);
        insert.Prepare();
        for (int i = 1; i <= data.Guids.Length; i++)
        {
            a.Value = data.Guids[i - 1].ToByteArray(bigEndian: true);
            b.Value = "item-" + (i % 1000);
            c.Value = i % 97;
            insert.ExecuteNonQuery();
        }

        tx.Commit();
    }

    public long PointSelect(int id)
    {
        SqliteCommand cmd = Prepared("SELECT Id, CustomerId, Status, Amount, Qty FROM Orders WHERE Id = $p0", id);
        using SqliteDataReader reader = cmd.ExecuteReader();
        return reader.Read() ? reader.GetInt64(4) : -1;
    }

    public long GuidPointSelect(Guid key) =>
        CountRows(Prepared("SELECT Name, Value FROM GuidItems WHERE Id = $p0", key.ToByteArray(bigEndian: true)));

    public long SecondaryLookup(int customerId) =>
        CountRows(Prepared("SELECT Id, Amount FROM Orders WHERE CustomerId = $p0", customerId));

    public long RangeScan(int fromId, int width) =>
        CountRows(Prepared("SELECT Id, Amount FROM Orders WHERE Id >= $p0 AND Id < $p1", fromId, fromId + width));

    public long FilteredCount() =>
        Convert.ToInt64(Prepared("SELECT COUNT(*) FROM Orders WHERE Status = 'paid'").ExecuteScalar());

    public long GroupByStatus()
    {
        using SqliteDataReader reader = Prepared("SELECT Status, COUNT(*), AVG(Amount) FROM Orders GROUP BY Status").ExecuteReader();
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

    public void UpdateAmount(int id, double amount) =>
        Prepared("UPDATE Orders SET Amount = $p0 WHERE Id = $p1", amount, id).ExecuteNonQuery();

    public void Delete(int id) => Prepared("DELETE FROM Orders WHERE Id = $p0", id).ExecuteNonQuery();

    public void Close()
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

    public void Reopen()
    {
        Close();
        OpenConnection();
    }

    public long SizeOnDiskBytes() => SqlWorkloadRunner.DirectorySize(Path.GetDirectoryName(_path!)!);

    public void Dispose() => Close();

    private SqliteConnection Conn => _connection ?? throw new InvalidOperationException("SQLite is not open.");

    private void OpenConnection()
    {
        _connection = new SqliteConnection($"Data Source={_path};Pooling=False");
        _connection.Open();
        Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
    }

    private void Exec(string sql)
    {
        using SqliteCommand command = Conn.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private SqliteCommand Prepared(string sql, params object[] values)
    {
        if (!_prepared.TryGetValue(sql, out SqliteCommand? command))
        {
            command = Conn.CreateCommand();
            command.CommandText = sql;
            for (int i = 0; i < values.Length; i++)
            {
                command.Parameters.Add(new SqliteParameter("$p" + i, values[i]));
            }

            command.Prepare();
            _prepared[sql] = command;
        }

        for (int i = 0; i < values.Length; i++)
        {
            command.Parameters[i].Value = values[i];
        }

        return command;
    }

    private static long CountRows(SqliteCommand command)
    {
        using SqliteDataReader reader = command.ExecuteReader();
        long rows = 0;
        while (reader.Read())
        {
            rows++;
        }

        return rows;
    }
}
