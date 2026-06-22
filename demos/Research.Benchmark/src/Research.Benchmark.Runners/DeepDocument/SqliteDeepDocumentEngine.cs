using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DeepDocument;

/// <summary>
/// SQLite deep-document engine: like DataVo it NORMALIZES the order across Orders / OrderItems / Addresses,
/// but it has a B-tree index on the child <c>OrderId</c> columns, so reconstruction uses indexed lookups
/// (O(log n)) rather than table scans. In-memory; prepared parameterized commands; inserts in one
/// transaction. This isolates DataVo's specific gap (its compiled-query path doesn't use secondary indexes).
/// </summary>
public sealed class SqliteDeepDocumentEngine : IDeepDocumentEngine
{
    private SqliteConnection? _connection;
    private SqliteCommand? _insertOrder, _insertItem, _insertAddress;
    private SqliteCommand? _loadOrder, _loadItems, _loadAddresses;
    private SqliteTransaction? _transaction;
    private int _nextItemId = 1;
    private int _nextAddressId = 1;

    public string Name => "SQLite";

    public void Initialize()
    {
        DisposeCore();
        _nextItemId = 1;
        _nextAddressId = 1;
        _connection = new SqliteConnection($"Data Source=FairPlayDeepDoc-{Guid.NewGuid():N};Mode=Memory;Cache=Shared");
        _connection.Open();

        using (SqliteCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = """
                CREATE TABLE Orders (Id INTEGER PRIMARY KEY, Customer TEXT NOT NULL, Total REAL NOT NULL);
                CREATE TABLE OrderItems (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku INTEGER NOT NULL, Name TEXT NOT NULL, Quantity INTEGER NOT NULL, UnitPrice REAL NOT NULL);
                CREATE TABLE Addresses (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Kind TEXT NOT NULL, Street TEXT NOT NULL, City TEXT NOT NULL, PostalCode TEXT NOT NULL);
                CREATE INDEX IX_OrderItems_OrderId ON OrderItems (OrderId);
                CREATE INDEX IX_Addresses_OrderId ON Addresses (OrderId);
                """;
            schema.ExecuteNonQuery();
        }

        _insertOrder = Prepare("INSERT INTO Orders (Id, Customer, Total) VALUES ($id, $customer, $total);",
            ("$id", SqliteType.Integer), ("$customer", SqliteType.Text), ("$total", SqliteType.Real));
        _insertItem = Prepare("INSERT INTO OrderItems (Id, OrderId, Sku, Name, Quantity, UnitPrice) VALUES ($id, $orderId, $sku, $name, $qty, $price);",
            ("$id", SqliteType.Integer), ("$orderId", SqliteType.Integer), ("$sku", SqliteType.Integer),
            ("$name", SqliteType.Text), ("$qty", SqliteType.Integer), ("$price", SqliteType.Real));
        _insertAddress = Prepare("INSERT INTO Addresses (Id, OrderId, Kind, Street, City, PostalCode) VALUES ($id, $orderId, $kind, $street, $city, $postal);",
            ("$id", SqliteType.Integer), ("$orderId", SqliteType.Integer), ("$kind", SqliteType.Text),
            ("$street", SqliteType.Text), ("$city", SqliteType.Text), ("$postal", SqliteType.Text));

        _loadOrder = Prepare("SELECT Customer, Total FROM Orders WHERE Id = $id;", ("$id", SqliteType.Integer));
        _loadItems = Prepare("SELECT Sku, Name, Quantity, UnitPrice FROM OrderItems WHERE OrderId = $orderId;", ("$orderId", SqliteType.Integer));
        _loadAddresses = Prepare("SELECT Kind, Street, City, PostalCode FROM Addresses WHERE OrderId = $orderId;", ("$orderId", SqliteType.Integer));
    }

    public void BeginBatch()
    {
        _transaction = Connection().BeginTransaction();
        _insertOrder!.Transaction = _transaction;
        _insertItem!.Transaction = _transaction;
        _insertAddress!.Transaction = _transaction;
    }

    public void CompleteBatch()
    {
        _transaction?.Commit();
        _transaction?.Dispose();
        _transaction = null;
        _insertOrder!.Transaction = null;
        _insertItem!.Transaction = null;
        _insertAddress!.Transaction = null;
    }

    public void Save(DeepOrder order)
    {
        _insertOrder!.Parameters["$id"].Value = order.Id;
        _insertOrder.Parameters["$customer"].Value = order.Customer;
        _insertOrder.Parameters["$total"].Value = order.Total;
        _insertOrder.ExecuteNonQuery();

        foreach (OrderItem item in order.Items)
        {
            _insertItem!.Parameters["$id"].Value = _nextItemId++;
            _insertItem.Parameters["$orderId"].Value = order.Id;
            _insertItem.Parameters["$sku"].Value = item.Sku;
            _insertItem.Parameters["$name"].Value = item.Name;
            _insertItem.Parameters["$qty"].Value = item.Quantity;
            _insertItem.Parameters["$price"].Value = item.UnitPrice;
            _insertItem.ExecuteNonQuery();
        }

        foreach (OrderAddress address in order.Addresses)
        {
            _insertAddress!.Parameters["$id"].Value = _nextAddressId++;
            _insertAddress.Parameters["$orderId"].Value = order.Id;
            _insertAddress.Parameters["$kind"].Value = address.Kind;
            _insertAddress.Parameters["$street"].Value = address.Street;
            _insertAddress.Parameters["$city"].Value = address.City;
            _insertAddress.Parameters["$postal"].Value = address.PostalCode;
            _insertAddress.ExecuteNonQuery();
        }
    }

    public DeepOrder? Load(long id)
    {
        string customer;
        double total;
        _loadOrder!.Parameters["$id"].Value = id;
        using (SqliteDataReader reader = _loadOrder.ExecuteReader())
        {
            if (!reader.Read())
            {
                return null;
            }

            customer = reader.GetString(0);
            total = reader.GetDouble(1);
        }

        var items = new List<OrderItem>();
        _loadItems!.Parameters["$orderId"].Value = id;
        using (SqliteDataReader reader = _loadItems.ExecuteReader())
        {
            while (reader.Read())
            {
                items.Add(new OrderItem(reader.GetInt32(0), reader.GetString(1), reader.GetInt32(2), reader.GetDouble(3)));
            }
        }

        var addresses = new List<OrderAddress>();
        _loadAddresses!.Parameters["$orderId"].Value = id;
        using (SqliteDataReader reader = _loadAddresses.ExecuteReader())
        {
            while (reader.Read())
            {
                addresses.Add(new OrderAddress(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }
        }

        return new DeepOrder(id, customer, total, items, addresses);
    }

    public void Dispose() => DisposeCore();

    private SqliteConnection Connection() =>
        _connection ?? throw new InvalidOperationException("SQLite deep-document engine has not been initialized.");

    private SqliteCommand Prepare(string sql, params (string Name, SqliteType Type)[] parameters)
    {
        SqliteCommand command = Connection().CreateCommand();
        command.CommandText = sql;
        foreach ((string name, SqliteType type) in parameters)
        {
            command.Parameters.Add(name, type);
        }

        command.Prepare();
        return command;
    }

    private void DisposeCore()
    {
        _transaction?.Dispose();
        _transaction = null;
        foreach (SqliteCommand? command in new[] { _insertOrder, _insertItem, _insertAddress, _loadOrder, _loadItems, _loadAddresses })
        {
            command?.Dispose();
        }

        _insertOrder = _insertItem = _insertAddress = _loadOrder = _loadItems = _loadAddresses = null;
        _connection?.Dispose();
        _connection = null;
    }
}
