using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ComplexVip;

public sealed class DataVoComplexVipExposureEngine : IComplexVipExposureEngine
{
    private readonly object _gate = new();
    private readonly Dictionary<string, decimal> _exposure = new(StringComparer.OrdinalIgnoreCase);
    private DataVoContext? _context;
    private IDisposable? _subscription;
    private long _nextOrderId = 1;
    private static readonly ReactiveRowSchema OrdersSchema = new("Id", "AccountId", "MarketId", "Stake");
    private readonly CellValue[] _orderCells = new CellValue[4];

    public string Name => "DataVo";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedReactiveIvm;

    public ValueTask InitializeAsync(ComplexVipExposureScenario scenario, CancellationToken cancellationToken = default)
    {
        DisposeDataVo();

        _nextOrderId = 1;
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE ComplexVipBenchmark");
        ExecuteOk("USE ComplexVipBenchmark");
        ExecuteOk("CREATE TABLE Accounts (Id INT, IsVip BIT)");
        ExecuteOk("CREATE TABLE Markets (Id INT, Category VARCHAR(40))");
        ExecuteOk("CREATE TABLE Orders (Id INT, AccountId INT, MarketId INT, Stake INT)");

        EnsureContext().BulkInsert("Accounts", Enumerable.Range(1, scenario.AccountCount).Select(id =>
            (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["Id"] = id,
                ["IsVip"] = ComplexVipTickFactory.IsVipAccount(id, scenario)
            }));

        EnsureContext().BulkInsert("Markets", Enumerable.Range(1, scenario.MarketCount).Select(id =>
            (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["Id"] = id,
                ["Category"] = ComplexVipTickFactory.CategoryForMarket(id)
            }));

        _subscription = EnsureContext().SubscribeZeroAlloc(ComplexVipSql.Query, ApplyChange);

        List<IReadOnlyDictionary<string, object?>> baseline = Enumerable
            .Range(0, scenario.InitialOrderCount)
            .Select(i => ToRow(ComplexVipTickFactory.CreateOrder(_nextOrderId + i, scenario)))
            .ToList();

        if (baseline.Count > 0)
        {
            EnsureContext().BulkInsert("Orders", baseline);
            EnsureContext().DispatchPendingNotifications();
            _nextOrderId += baseline.Count;
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask IngestOrderAsync(ComplexOrderTick order, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _orderCells[0] = CellValue.From(checked((int)order.Id));
        _orderCells[1] = CellValue.From(order.AccountId);
        _orderCells[2] = CellValue.From(order.MarketId);
        _orderCells[3] = CellValue.From(checked((int)decimal.Round(order.Stake, 0)));
        EnsureContext().InsertTyped("Orders", OrdersSchema, _orderCells);
        EnsureContext().DispatchPendingNotifications();
        _nextOrderId = Math.Max(_nextOrderId, order.Id + 1);
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<CategoryExposure>> QueryExposureAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            return ValueTask.FromResult((IReadOnlyList<CategoryExposure>)_exposure
                .Select(pair => new CategoryExposure(pair.Key, pair.Value))
                .OrderBy(row => row.Category)
                .ToList());
        }
    }

    public ValueTask DisposeAsync()
    {
        DisposeDataVo();
        return ValueTask.CompletedTask;
    }

    private void ApplyChange(in QueryChangeRef change)
    {
        lock (_gate)
        {
            for (int i = 0; i < change.Removed.Count; i++)
            {
                _exposure.Remove(change.Removed[i]["Category"].AsString()!);
            }

            for (int i = 0; i < change.Added.Count; i++)
            {
                _exposure[change.Added[i]["Category"].AsString()!] = change.Added[i]["TotalExposure"].AsDecimal();
            }

            for (int i = 0; i < change.Updated.Count; i++)
            {
                _exposure[change.Updated[i]["Category"].AsString()!] = change.Updated[i]["TotalExposure"].AsDecimal();
            }
        }
    }

    private static IReadOnlyDictionary<string, object?> ToRow(ComplexOrderTick order) =>
        new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["Id"] = order.Id,
            ["AccountId"] = order.AccountId,
            ["MarketId"] = order.MarketId,
            ["Stake"] = Decimal.ToInt64(decimal.Round(order.Stake, 0))
        };

    private DataVoContext EnsureContext() =>
        _context ?? throw new InvalidOperationException("DataVo complex VIP engine has not been initialized.");

    private void ExecuteOk(string sql)
    {
        QueryResult result = EnsureContext().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }

    private void DisposeDataVo()
    {
        _subscription?.Dispose();
        _subscription = null;
        _context?.Dispose();
        _context = null;
        lock (_gate)
        {
            _exposure.Clear();
        }
    }
}
