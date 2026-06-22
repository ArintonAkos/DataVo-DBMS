using LiteDB;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DeepDocument;

/// <summary>
/// LiteDB deep-document engine: the whole order (header + nested items + addresses) is one BSON document,
/// saved and loaded as a unit via <c>FindById</c> — LiteDB's natural strength for nested aggregates.
/// In-memory over a <see cref="MemoryStream"/> so BSON serialize/deserialize is measured.
/// </summary>
public sealed class LiteDbDeepDocumentEngine : IDeepDocumentEngine
{
    private LiteDatabase? _database;
    private ILiteCollection<OrderDocument>? _collection;

    public string Name => "LiteDB";

    public void Initialize()
    {
        _database?.Dispose();
        _database = new LiteDatabase(new MemoryStream());
        _collection = _database.GetCollection<OrderDocument>("orders");
    }

    public void BeginBatch() => Database().BeginTrans();

    public void CompleteBatch() => Database().Commit();

    public void Save(DeepOrder order)
    {
        Collection().Insert(new OrderDocument
        {
            Id = order.Id,
            Customer = order.Customer,
            Total = order.Total,
            Items = order.Items
                .Select(item => new ItemDocument
                {
                    Sku = item.Sku,
                    Name = item.Name,
                    Quantity = item.Quantity,
                    UnitPrice = item.UnitPrice,
                })
                .ToList(),
            Addresses = order.Addresses
                .Select(address => new AddressDocument
                {
                    Kind = address.Kind,
                    Street = address.Street,
                    City = address.City,
                    PostalCode = address.PostalCode,
                })
                .ToList(),
        });
    }

    public DeepOrder? Load(long id)
    {
        OrderDocument? document = Collection().FindById(id);
        if (document is null)
        {
            return null;
        }

        return new DeepOrder(
            document.Id,
            document.Customer,
            document.Total,
            document.Items.Select(item => new OrderItem(item.Sku, item.Name, item.Quantity, item.UnitPrice)).ToList(),
            document.Addresses.Select(a => new OrderAddress(a.Kind, a.Street, a.City, a.PostalCode)).ToList());
    }

    public void Dispose()
    {
        _database?.Dispose();
        _database = null;
        _collection = null;
    }

    private ILiteCollection<OrderDocument> Collection() =>
        _collection ?? throw new InvalidOperationException("LiteDB deep-document engine has not been initialized.");

    private LiteDatabase Database() =>
        _database ?? throw new InvalidOperationException("LiteDB deep-document engine has not been initialized.");

    private sealed class OrderDocument
    {
        public long Id { get; set; }
        public string Customer { get; set; } = string.Empty;
        public double Total { get; set; }
        public List<ItemDocument> Items { get; set; } = [];
        public List<AddressDocument> Addresses { get; set; } = [];
    }

    private sealed class ItemDocument
    {
        public int Sku { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public double UnitPrice { get; set; }
    }

    private sealed class AddressDocument
    {
        public string Kind { get; set; } = string.Empty;
        public string Street { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
        public string PostalCode { get; set; } = string.Empty;
    }
}
