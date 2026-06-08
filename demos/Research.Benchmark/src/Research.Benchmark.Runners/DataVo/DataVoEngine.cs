using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DataVo;

public sealed class DataVoEngine : IBettingRiskEngine
{
    private const string RunnerRiskSql = """
        SELECT MarketId, RunnerId, MAX(Price) AS BestBack, MIN(Price) AS BestLay, SUM(Stake) AS OpenExposure
        FROM Orders
        WHERE Status = 'OPEN'
        GROUP BY MarketId, RunnerId
        """;

    private const string AccountRiskSql = """
        SELECT AccountId, MarketId, SUM(Stake) AS OpenExposure
        FROM Orders
        WHERE Status = 'OPEN'
        GROUP BY AccountId, MarketId
        """;

    private readonly object _gate = new();
    private readonly Dictionary<RunnerExposureKey, RunnerExposure> _runnerExposure = [];
    private readonly Dictionary<AccountExposureKey, AccountExposure> _accountExposure = [];
    private readonly Dictionary<RunnerAccountExposureKey, decimal> _pointExposure = [];
    private readonly Dictionary<int, decimal> _marketOpenExposure = [];
    private readonly List<MarketRiskSummary> _defaultMarketRisk = [];
    private readonly IReadOnlyList<RunnerExposure> _runnerExposureView;
    private readonly IReadOnlyList<AccountExposure> _accountExposureView;
    private DataVoContext? _context;
    private IDisposable? _runnerSubscription;
    private IDisposable? _accountSubscription;
    private BettingRiskScenario _scenario = new(0, 0, 0, 0, 0);
    private long _nextOrderId = 1;
    private RiskReadModel _defaultReadModel;

    public DataVoEngine()
    {
        _runnerExposureView = new DictionaryValueReadOnlyList<RunnerExposureKey, RunnerExposure>(_runnerExposure);
        _accountExposureView = new DictionaryValueReadOnlyList<AccountExposureKey, AccountExposure>(_accountExposure);
        _defaultReadModel = new RiskReadModel(_runnerExposureView, _accountExposureView, _defaultMarketRisk, DateTimeOffset.MinValue);
    }

    public string Name => "DataVo";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedReactiveIvm;

    public ValueTask InitializeAsync(BettingRiskScenario scenario, CancellationToken cancellationToken = default)
    {
        DisposeDataVo();

        _scenario = scenario;
        _nextOrderId = 1;
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE ResearchBenchmark");
        ExecuteOk("USE ResearchBenchmark");
        ExecuteOk("""
            CREATE TABLE Orders (
                OrderId INT,
                MarketId INT,
                RunnerId INT,
                AccountId INT,
                Side VARCHAR(8),
                Price INT,
                Stake INT,
                Status VARCHAR(12)
            )
            """);

        List<IReadOnlyDictionary<string, object?>> baseline = Enumerable
            .Range(0, scenario.InitialOrderCount)
            .Select(i => Research.Benchmark.Runners.BenchmarkTickFactory.CreateBaselineOrder(_nextOrderId + i, scenario))
            .Select(tick => ToRow(tick))
            .ToList();

        if (baseline.Count > 0)
        {
            _context.BulkInsert("Orders", baseline);
            _nextOrderId += baseline.Count;
            foreach (IReadOnlyDictionary<string, object?> row in baseline)
            {
                AddPointExposure(
                    RiskModelProjection.ToInt32(row["RunnerId"]),
                    RiskModelProjection.ToInt32(row["AccountId"]),
                    RiskModelProjection.ToDecimal(row["Stake"]));
            }
        }

        _runnerSubscription = _context.Subscribe(RunnerRiskSql, ApplyRunnerChange);
        _accountSubscription = _context.Subscribe(AccountRiskSql, ApplyAccountChange);
        LoadInitialReadModel();

        return ValueTask.CompletedTask;
    }

    public ValueTask IngestTickAsync(MarketTick tick, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        long orderId = OrderIdFor(tick);
        DataVoContext context = EnsureContext();
        context.BulkInsert("Orders", [ToRow(tick, orderId)]);
        context.DispatchPendingNotifications();
        AddPointExposure(tick.RunnerId, tick.AccountId, tick.Stake);
        _nextOrderId = Math.Max(_nextOrderId, tick.Sequence + 1);
        return ValueTask.CompletedTask;
    }

    public ValueTask IngestBatchAsync(IReadOnlyList<MarketTick> ticks, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (ticks.Count == 0)
        {
            return ValueTask.CompletedTask;
        }

        DataVoContext context = EnsureContext();
        context.BulkInsert("Orders", ticks.Select(tick => ToRow(tick)));
        context.DispatchPendingNotifications();

        foreach (MarketTick tick in ticks)
        {
            AddPointExposure(tick.RunnerId, tick.AccountId, tick.Stake);
            _nextOrderId = Math.Max(_nextOrderId, tick.Sequence + 1);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<RiskReadModel> QueryRiskAsync(RiskQuery query, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            if (query.RunnerId is not null && query.AccountId is not null)
            {
                _pointExposure.TryGetValue(
                    new RunnerAccountExposureKey(query.RunnerId.Value, query.AccountId.Value),
                    out decimal pointExposure);
                return ValueTask.FromResult(RiskModelProjection.BuildPoint(
                    query.RunnerId.Value,
                    query.AccountId.Value,
                    pointExposure,
                    _scenario.SubscriberCount));
            }

            if (IsDefaultQuery(query))
            {
                return ValueTask.FromResult(_defaultReadModel);
            }

            return ValueTask.FromResult(BuildRiskReadModel(query));
        }
    }

    public ValueTask ResetAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ExecuteOk("DELETE FROM Orders");
        _context!.DispatchPendingNotifications();

        lock (_gate)
        {
            _runnerExposure.Clear();
            _accountExposure.Clear();
            _pointExposure.Clear();
            RefreshDefaultReadModelLocked();
        }

        _nextOrderId = 1;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        DisposeDataVo();
        return ValueTask.CompletedTask;
    }

    private void LoadInitialReadModel()
    {
        lock (_gate)
        {
            _runnerExposure.Clear();
            _marketOpenExposure.Clear();
            foreach (Dictionary<string, object?> row in QueryRows(RunnerRiskSql))
            {
                UpsertRunner(row);
            }

            _accountExposure.Clear();
            foreach (Dictionary<string, object?> row in QueryRows(AccountRiskSql))
            {
                UpsertAccount(row);
            }

            RefreshDefaultReadModelLocked();
        }
    }

    private void ApplyRunnerChange(QueryChange change)
    {
        lock (_gate)
        {
            foreach (IReadOnlyDictionary<string, object?> row in change.Removed)
            {
                var key = new RunnerExposureKey(
                    RiskModelProjection.ToInt32(row["MarketId"]),
                    RiskModelProjection.ToInt32(row["RunnerId"]));
                RemoveRunner(key);
            }

            foreach (IReadOnlyDictionary<string, object?> row in change.Added)
            {
                UpsertRunner(row);
            }

            foreach (IReadOnlyDictionary<string, object?> row in change.Updated)
            {
                UpsertRunner(row);
            }

            RefreshDefaultReadModelLocked();
        }
    }

    private void ApplyAccountChange(QueryChange change)
    {
        lock (_gate)
        {
            foreach (IReadOnlyDictionary<string, object?> row in change.Removed)
            {
                _accountExposure.Remove(new AccountExposureKey(
                    RiskModelProjection.ToInt32(row["AccountId"]),
                    RiskModelProjection.ToInt32(row["MarketId"])));
            }

            foreach (IReadOnlyDictionary<string, object?> row in change.Added)
            {
                UpsertAccount(row);
            }

            foreach (IReadOnlyDictionary<string, object?> row in change.Updated)
            {
                UpsertAccount(row);
            }

            TouchDefaultReadModelAsOfLocked();
        }
    }

    private void UpsertRunner(IReadOnlyDictionary<string, object?> row)
    {
        int marketId = RiskModelProjection.ToInt32(row["MarketId"]);
        int runnerId = RiskModelProjection.ToInt32(row["RunnerId"]);
        var key = new RunnerExposureKey(marketId, runnerId);
        var exposure = new RunnerExposure(
            marketId,
            runnerId,
            RiskModelProjection.ToDecimal(row["BestBack"]),
            RiskModelProjection.ToDecimal(row["BestLay"]),
            0m,
            RiskModelProjection.ToDecimal(row["OpenExposure"]));

        if (_runnerExposure.TryGetValue(key, out RunnerExposure? old))
        {
            AddMarketExposure(marketId, -old.OpenExposure);
        }

        _runnerExposure[key] = exposure;
        AddMarketExposure(marketId, exposure.OpenExposure);
    }

    private void UpsertAccount(IReadOnlyDictionary<string, object?> row)
    {
        int accountId = RiskModelProjection.ToInt32(row["AccountId"]);
        int marketId = RiskModelProjection.ToInt32(row["MarketId"]);
        _accountExposure[new AccountExposureKey(accountId, marketId)] = new AccountExposure(
            accountId,
            marketId,
            RiskModelProjection.ToDecimal(row["OpenExposure"]),
            0m);
    }

    private long OrderIdFor(MarketTick tick) => tick.Sequence > 0 ? tick.Sequence : _nextOrderId;

    private static IReadOnlyDictionary<string, object?> ToRow(MarketTick tick, long? orderId = null) =>
        new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["OrderId"] = orderId ?? tick.Sequence,
            ["MarketId"] = tick.MarketId,
            ["RunnerId"] = tick.RunnerId,
            ["AccountId"] = tick.AccountId,
            ["Side"] = tick.Side,
            ["Price"] = Decimal.ToInt64(decimal.Round(tick.Price, 0)),
            ["Stake"] = Decimal.ToInt64(decimal.Round(tick.Stake, 0)),
            ["Status"] = StatusFor(tick)
        };

    private static string StatusFor(MarketTick tick) =>
        tick.Kind == TickKind.OrderCancelled ? "CANCELLED" : "OPEN";

    private static bool IsDefaultQuery(RiskQuery query) =>
        query.MarketId is null && query.AccountId is null && query.RunnerId is null && query.TopMarkets == 10;

    private void AddPointExposure(int runnerId, int accountId, decimal exposure)
    {
        lock (_gate)
        {
            var key = new RunnerAccountExposureKey(runnerId, accountId);
            _pointExposure.TryGetValue(key, out decimal current);
            _pointExposure[key] = current + exposure;
        }
    }

    private RiskReadModel BuildRiskReadModel(RiskQuery query) =>
        RiskModelProjection.Build(
            _runnerExposure.Values.ToList(),
            _accountExposure.Values.ToList(),
            query,
            _scenario.SubscriberCount);

    private void RefreshDefaultReadModelLocked()
    {
        _defaultMarketRisk.Clear();
        _defaultMarketRisk.AddRange(_marketOpenExposure
            .Where(row => row.Value != 0m)
            .Select(row => new MarketRiskSummary(row.Key, row.Value, 0m, _scenario.SubscriberCount))
            .OrderByDescending(row => row.TotalOpenExposure)
            .ThenBy(row => row.MarketId)
            .Take(10));
        TouchDefaultReadModelAsOfLocked();
    }

    private void TouchDefaultReadModelAsOfLocked()
    {
        _defaultReadModel = new RiskReadModel(_runnerExposureView, _accountExposureView, _defaultMarketRisk, DateTimeOffset.UtcNow);
    }

    private void RemoveRunner(RunnerExposureKey key)
    {
        if (_runnerExposure.Remove(key, out RunnerExposure? removed))
        {
            AddMarketExposure(key.MarketId, -removed.OpenExposure);
        }
    }

    private void AddMarketExposure(int marketId, decimal delta)
    {
        _marketOpenExposure.TryGetValue(marketId, out decimal current);
        decimal next = current + delta;
        if (next == 0m)
        {
            _marketOpenExposure.Remove(marketId);
        }
        else
        {
            _marketOpenExposure[marketId] = next;
        }
    }

    private DataVoContext EnsureContext() =>
        _context ?? throw new InvalidOperationException("DataVo engine has not been initialized.");

    private void ExecuteOk(string sql)
    {
        QueryResult result = EnsureContext().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }

    private IReadOnlyList<Dictionary<string, object?>> QueryRows(string sql)
    {
        QueryResult result = EnsureContext().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }

        return result.Data;
    }

    private void DisposeDataVo()
    {
        _runnerSubscription?.Dispose();
        _runnerSubscription = null;
        _accountSubscription?.Dispose();
        _accountSubscription = null;
        _context?.Dispose();
        _context = null;

        lock (_gate)
        {
            _runnerExposure.Clear();
            _accountExposure.Clear();
            _pointExposure.Clear();
            _marketOpenExposure.Clear();
            _defaultMarketRisk.Clear();
            _defaultReadModel = new RiskReadModel(_runnerExposureView, _accountExposureView, _defaultMarketRisk, DateTimeOffset.MinValue);
        }
    }

    private sealed class DictionaryValueReadOnlyList<TKey, TValue>(Dictionary<TKey, TValue> values) : IReadOnlyList<TValue>
        where TKey : notnull
    {
        public int Count => values.Count;

        public TValue this[int index] => values.Values.ElementAt(index);

        public IEnumerator<TValue> GetEnumerator() => values.Values.GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
