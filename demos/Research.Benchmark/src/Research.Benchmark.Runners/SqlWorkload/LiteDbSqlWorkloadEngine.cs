using LiteDB;

namespace Research.Benchmark.Runners.SqlWorkload;

/// <summary>
/// LiteDB, the common pure-.NET embedded document store. It has no joins or GROUP BY over
/// collections, so those two queries run in application code the way a LiteDB user would write
/// them (scan, then group in LINQ; join through a dictionary of customers).
/// </summary>
public sealed class LiteDbSqlWorkloadEngine : ISqlWorkloadEngine
{
    private LiteDatabase? _database;
    private string? _path;

    public string Name => "LiteDB";

    public string Notes => "Default journal; BsonDocument API with InsertBulk; GROUP BY and JOIN done in application code (LiteDB has no relational joins).";

    public void Create(string directory)
    {
        Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, "bench.litedb");
        OpenDatabase();
        Orders.EnsureIndex("CustomerId");
    }

    public void LoadOrders(SqlWorkloadData data)
    {
        Customers.InsertBulk(Enumerable.Range(1, data.Customers).Select(static i => new BsonDocument
        {
            ["_id"] = i,
            ["Name"] = "customer-" + i,
            ["Region"] = SqlWorkloadData.RegionOf(i),
        }), batchSize: 50_000);

        int customers = data.Customers;
        Orders.InsertBulk(Enumerable.Range(1, data.Orders).Select(i => new BsonDocument
        {
            ["_id"] = i,
            ["CustomerId"] = SqlWorkloadData.CustomerOf(i, customers),
            ["Status"] = SqlWorkloadData.StatusOf(i),
            ["Amount"] = SqlWorkloadData.AmountOf(i),
            ["Qty"] = SqlWorkloadData.QtyOf(i),
        }), batchSize: 50_000);
    }

    public void LoadGuids(SqlWorkloadData data)
    {
        Guid[] keys = data.Guids;
        GuidItems.InsertBulk(Enumerable.Range(1, keys.Length).Select(i => new BsonDocument
        {
            ["_id"] = keys[i - 1],
            ["Name"] = "item-" + (i % 1000),
            ["Value"] = i % 97,
        }), batchSize: 50_000);
    }

    public long PointSelect(int id)
    {
        BsonDocument? document = Orders.FindById(id);
        return document is null ? -1 : document["Qty"].AsInt32;
    }

    public long GuidPointSelect(Guid key) => GuidItems.FindById(key) is null ? 0 : 1;

    public long SecondaryLookup(int customerId) => Orders.Find(Query.EQ("CustomerId", customerId)).LongCount();

    public long RangeScan(int fromId, int width) => Orders.Find(Query.Between("_id", fromId, fromId + width - 1)).LongCount();

    public long FilteredCount() => Orders.Count(Query.EQ("Status", "paid"));

    public long GroupByStatus() =>
        Orders.FindAll().GroupBy(static d => d["Status"].AsString).Sum(static g => (long)g.Count());

    public long JoinByRegion()
    {
        var regions = Customers.FindAll().ToDictionary(static c => c["_id"].AsInt32, static c => c["Region"].AsString);
        var totals = new Dictionary<string, double>(StringComparer.Ordinal);
        foreach (BsonDocument order in Orders.FindAll())
        {
            if (regions.TryGetValue(order["CustomerId"].AsInt32, out string? region))
            {
                totals[region] = totals.GetValueOrDefault(region) + order["Amount"].AsDouble;
            }
        }

        return totals.Count;
    }

    public long TopTen() => Orders.Query().OrderByDescending("Amount").Limit(10).ToList().Count;

    public void UpdateAmount(int id, double amount)
    {
        BsonDocument? document = Orders.FindById(id);
        if (document is not null)
        {
            document["Amount"] = amount;
            Orders.Update(document);
        }
    }

    public void Delete(int id) => Orders.Delete(id);

    public void Close()
    {
        _database?.Dispose();
        _database = null;
    }

    public void Reopen()
    {
        Close();
        OpenDatabase();
    }

    public long SizeOnDiskBytes() => SqlWorkloadRunner.DirectorySize(Path.GetDirectoryName(_path!)!);

    public void Dispose() => Close();

    private LiteDatabase Db => _database ?? throw new InvalidOperationException("LiteDB is not open.");

    private ILiteCollection<BsonDocument> Orders => Db.GetCollection("orders");

    private ILiteCollection<BsonDocument> Customers => Db.GetCollection("customers");

    private ILiteCollection<BsonDocument> GuidItems => Db.GetCollection("guiditems");

    private void OpenDatabase() => _database = new LiteDatabase($"Filename={_path};Connection=direct");
}
