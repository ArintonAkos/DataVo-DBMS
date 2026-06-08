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

        _subscription = EnsureContext().Subscribe(ComplexVipSql.Query, ApplyChange);

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
        EnsureContext().BulkInsert("Orders", [ToRow(order)]);
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

    private void ApplyChange(QueryChange change)
    {
        lock (_gate)
        {
            foreach (IReadOnlyDictionary<string, object?> row in change.Removed)
            {
                _exposure.Remove((string)row["Category"]!);
            }

            foreach (IReadOnlyDictionary<string, object?> row in change.Added.Concat(change.Updated))
            {
                _exposure[(string)row["Category"]!] = Convert.ToDecimal(row["TotalExposure"]);
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
