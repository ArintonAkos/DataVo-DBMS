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
        "Orders", ["Id", "Customer", "Total"], whereColumn: "Id", parameterName: "id",
        accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "_PK_Orders");
    private static readonly DataVoCompiledQueryPlan ItemsByOrderPlan = DataVoCompiledQueryPlan.SelectMany(
        "OrderItems", ["Sku", "Name", "Quantity", "UnitPrice"], whereColumn: "OrderId", parameterName: "orderId",
        accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_OrderItems_OrderId");
    private static readonly DataVoCompiledQueryPlan AddressesByOrderPlan = DataVoCompiledQueryPlan.SelectMany(
        "Addresses", ["Kind", "Street", "City", "PostalCode"], whereColumn: "OrderId", parameterName: "orderId",
        accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_Addresses_OrderId");

    private DataVoContext? _context;
    private DataVoPreparedSelectSingle<OrderHeader>? _orderById;
    private DataVoPreparedSelectMany<OrderItem>? _itemsByOrder;
    private DataVoPreparedSelectMany<OrderAddress>? _addressesByOrder;
    private List<CellValue[]>? _orderBatch;
    private List<CellValue[]>? _itemBatch;
    private List<CellValue[]>? _addressBatch;
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

        _orderById = DataVoCompiledQuery.PrepareSelectSingleTyped(Ctx(), OrderByIdPlan, MapOrderHeader);
        _itemsByOrder = DataVoCompiledQuery.PrepareSelectManyTyped(Ctx(), ItemsByOrderPlan, MapItem);
        _addressesByOrder = DataVoCompiledQuery.PrepareSelectManyTyped(Ctx(), AddressesByOrderPlan, MapAddress);
    }

    public void BeginBatch()
    {
        _orderBatch = new List<CellValue[]>(8192);
        _itemBatch = new List<CellValue[]>(40_960);
        _addressBatch = new List<CellValue[]>(16_384);
    }

    public void CompleteBatch()
    {
        DataVoContext context = Ctx();
        if (_orderBatch is { Count: > 0 } orders)
        {
            context.InsertTypedBatch("Orders", OrderSchema, orders);
        }

        if (_itemBatch is { Count: > 0 } items)
        {
            context.InsertTypedBatch("OrderItems", ItemSchema, items);
        }

        if (_addressBatch is { Count: > 0 } addresses)
        {
            context.InsertTypedBatch("Addresses", AddressSchema, addresses);
        }

        _orderBatch = null;
        _itemBatch = null;
        _addressBatch = null;
    }

    public void Save(DeepOrder order)
    {
        int orderId = checked((int)order.Id);
        CellValue[] orderCells =
        [
            CellValue.From(orderId),
            CellValue.From(order.Customer),
            CellValue.From(order.Total)
        ];

        if (_orderBatch is not null)
        {
            _orderBatch.Add(orderCells);
        }
        else
        {
            Ctx().InsertTyped("Orders", OrderSchema, orderCells);
        }

        foreach (OrderItem item in order.Items)
        {
            CellValue[] itemCells =
            [
                CellValue.From(_nextItemId++),
                CellValue.From(orderId),
                CellValue.From(item.Sku),
                CellValue.From(item.Name),
                CellValue.From(item.Quantity),
                CellValue.From(item.UnitPrice)
            ];

            if (_itemBatch is not null)
            {
                _itemBatch.Add(itemCells);
            }
            else
            {
                Ctx().InsertTyped("OrderItems", ItemSchema, itemCells);
            }
        }

        foreach (OrderAddress address in order.Addresses)
        {
            CellValue[] addressCells =
            [
                CellValue.From(_nextAddressId++),
                CellValue.From(orderId),
                CellValue.From(address.Kind),
                CellValue.From(address.Street),
                CellValue.From(address.City),
                CellValue.From(address.PostalCode)
            ];

            if (_addressBatch is not null)
            {
                _addressBatch.Add(addressCells);
            }
            else
            {
                Ctx().InsertTyped("Addresses", AddressSchema, addressCells);
            }
        }
    }

    public DeepOrder? Load(long id)
    {
        int orderId = checked((int)id);
        DataVoContext context = Ctx();

        OrderHeader? header = OrderById().Execute(orderId);
        if (header is null)
        {
            return null;
        }

        IReadOnlyList<OrderItem> items = ItemsByOrder().Execute(orderId);
        IReadOnlyList<OrderAddress> addresses = AddressesByOrder().Execute(orderId);

        return new DeepOrder(id, header.Customer, header.Total, items, addresses);
    }

    public void Dispose()
    {
        _context?.Dispose();
        _context = null;
        _orderById = null;
        _itemsByOrder = null;
        _addressesByOrder = null;
        _orderBatch = null;
        _itemBatch = null;
        _addressBatch = null;
    }

    private DataVoContext Ctx() =>
        _context ?? throw new InvalidOperationException("DataVo deep-document engine has not been initialized.");

    private DataVoPreparedSelectSingle<OrderHeader> OrderById() =>
        _orderById ?? throw new InvalidOperationException("DataVo deep-document order lookup has not been prepared.");

    private DataVoPreparedSelectMany<OrderItem> ItemsByOrder() =>
        _itemsByOrder ?? throw new InvalidOperationException("DataVo deep-document item lookup has not been prepared.");

    private DataVoPreparedSelectMany<OrderAddress> AddressesByOrder() =>
        _addressesByOrder ?? throw new InvalidOperationException("DataVo deep-document address lookup has not been prepared.");

    private static OrderHeader MapOrderHeader(CompiledRowReader row) => new(
        row.GetString(1) ?? string.Empty,
        row.GetDouble(2));

    private static OrderItem MapItem(CompiledRowReader row) => new(
        row.GetInt32(0),
        row.GetString(1) ?? string.Empty,
        row.GetInt32(2),
        row.GetDouble(3));

    private static OrderAddress MapAddress(CompiledRowReader row) => new(
        row.GetString(0) ?? string.Empty,
        row.GetString(1) ?? string.Empty,
        row.GetString(2) ?? string.Empty,
        row.GetString(3) ?? string.Empty);

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
