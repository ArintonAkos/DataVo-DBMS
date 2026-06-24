using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DeepDocument;

/// <summary>
/// DataVo deep-document engine: the order is NORMALIZED across Orders / OrderItems / Addresses tables, so a
/// load reconstructs the nested aggregate from three keyed compiled queries (header by primary key, children
/// by order id). This deliberately exercises DataVo's multi-table reconstruction against LiteDB's single
/// nested-document read.
/// </summary>
public sealed class DataVoDeepDocumentEngine : IDeepDocumentEngine
{
    private static readonly ReactiveRowSchema OrderSchema = new("Id", "Customer", "Total");
    private static readonly ReactiveRowSchema ItemSchema = new("Id", "OrderId", "Sku", "Name", "Quantity", "UnitPrice");
    private static readonly ReactiveRowSchema AddressSchema = new("Id", "OrderId", "Kind", "Street", "City", "PostalCode");

    private static readonly DataVoCompiledQueryPlan OrderByIdPlan = DataVoCompiledQueryPlan.SelectSingle(
        "Orders", ["Id", "Customer", "Total"], whereColumn: "Id", parameterName: "id");
    private static readonly DataVoCompiledQueryPlan ItemsByOrderPlan = DataVoCompiledQueryPlan.SelectMany(
        "OrderItems", ["Sku", "Name", "Quantity", "UnitPrice"], whereColumn: "OrderId", parameterName: "orderId");
    private static readonly DataVoCompiledQueryPlan AddressesByOrderPlan = DataVoCompiledQueryPlan.SelectMany(
        "Addresses", ["Kind", "Street", "City", "PostalCode"], whereColumn: "OrderId", parameterName: "orderId");

    private static readonly Func<Dictionary<string, object?>, OrderItem> ItemMapper = row => new OrderItem(
        Convert.ToInt32(row["Sku"]), Convert.ToString(row["Name"]) ?? string.Empty,
        Convert.ToInt32(row["Quantity"]), Convert.ToDouble(row["UnitPrice"]));
    private static readonly Func<Dictionary<string, object?>, OrderAddress> AddressMapper = row => new OrderAddress(
        Convert.ToString(row["Kind"]) ?? string.Empty, Convert.ToString(row["Street"]) ?? string.Empty,
        Convert.ToString(row["City"]) ?? string.Empty, Convert.ToString(row["PostalCode"]) ?? string.Empty);

    private readonly CellValue[] _orderCells = new CellValue[3];
    private readonly CellValue[] _itemCells = new CellValue[6];
    private readonly CellValue[] _addressCells = new CellValue[6];
    private DataVoContext? _context;
    private int _nextItemId = 1;
    private int _nextAddressId = 1;

    public string Name => "DataVo";

    public void Initialize()
    {
        _context?.Dispose();
        _nextItemId = 1;
        _nextAddressId = 1;
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE DeepDocBenchmark");
        ExecuteOk("USE DeepDocBenchmark");
        ExecuteOk("CREATE TABLE Orders (Id INT PRIMARY KEY, Customer VARCHAR(40), Total FLOAT)");
        ExecuteOk("CREATE TABLE OrderItems (Id INT PRIMARY KEY, OrderId INT, Sku INT, Name VARCHAR(40), Quantity INT, UnitPrice FLOAT)");
        ExecuteOk("CREATE TABLE Addresses (Id INT PRIMARY KEY, OrderId INT, Kind VARCHAR(10), Street VARCHAR(40), City VARCHAR(40), PostalCode VARCHAR(12))");

        // The order is loaded by its child rows' OrderId (a non-primary-key column). Index that column on
        // both child tables so the compiled child queries use an O(log n) index lookup instead of scanning
        // the whole child table once per loaded order (which is the O(n^2) reconstruction cost).
        ExecuteOk("CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId)");
        ExecuteOk("CREATE INDEX ix_Addresses_OrderId ON Addresses (OrderId)");
    }

    public void BeginBatch() { }

    public void CompleteBatch() { }

    public void Save(DeepOrder order)
    {
        int orderId = checked((int)order.Id);
        _orderCells[0] = CellValue.From(orderId);
        _orderCells[1] = CellValue.From(order.Customer);
        _orderCells[2] = CellValue.From(order.Total);
        Ctx().InsertTyped("Orders", OrderSchema, _orderCells);

        foreach (OrderItem item in order.Items)
        {
            _itemCells[0] = CellValue.From(_nextItemId++);
            _itemCells[1] = CellValue.From(orderId);
            _itemCells[2] = CellValue.From(item.Sku);
            _itemCells[3] = CellValue.From(item.Name);
            _itemCells[4] = CellValue.From(item.Quantity);
            _itemCells[5] = CellValue.From(item.UnitPrice);
            Ctx().InsertTyped("OrderItems", ItemSchema, _itemCells);
        }

        foreach (OrderAddress address in order.Addresses)
        {
            _addressCells[0] = CellValue.From(_nextAddressId++);
            _addressCells[1] = CellValue.From(orderId);
            _addressCells[2] = CellValue.From(address.Kind);
            _addressCells[3] = CellValue.From(address.Street);
            _addressCells[4] = CellValue.From(address.City);
            _addressCells[5] = CellValue.From(address.PostalCode);
            Ctx().InsertTyped("Addresses", AddressSchema, _addressCells);
        }
    }

    public DeepOrder? Load(long id)
    {
        int orderId = checked((int)id);
        DataVoContext context = Ctx();

        // Map the header to a reference type so an absent order is genuinely null. (SelectSingle returns
        // default(TResult) for "no row", which for a value tuple would be a non-null (null, 0).)
        OrderHeader? header = DataVoCompiledQuery.SelectSingle(
            context, OrderByIdPlan, [new DataVoCompiledQueryParameter("id", orderId)],
            static row => new OrderHeader(Convert.ToString(row["Customer"]) ?? string.Empty, Convert.ToDouble(row["Total"])));
        if (header is null)
        {
            return null;
        }

        IReadOnlyList<OrderItem> items = DataVoCompiledQuery.SelectMany(
            context, ItemsByOrderPlan, [new DataVoCompiledQueryParameter("orderId", orderId)], ItemMapper);
        IReadOnlyList<OrderAddress> addresses = DataVoCompiledQuery.SelectMany(
            context, AddressesByOrderPlan, [new DataVoCompiledQueryParameter("orderId", orderId)], AddressMapper);

        return new DeepOrder(id, header.Customer, header.Total, items, addresses);
    }

    public void Dispose()
    {
        _context?.Dispose();
        _context = null;
    }

    private DataVoContext Ctx() =>
        _context ?? throw new InvalidOperationException("DataVo deep-document engine has not been initialized.");

    private void ExecuteOk(string sql)
    {
        QueryResult result = Ctx().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }

    private sealed record OrderHeader(string Customer, double Total);
}
