using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;
using Research.Benchmark.Runners;

namespace Research.Benchmark.Runners.DataVo;

public sealed class DataVoEngine : IBettingRiskEngine
{
    private const string RunnerRiskSql = """
        SELECT MarketId, RunnerId, MAX(Price) AS BestBack, MIN(Price) AS BestLay, SUM(Stake) AS OpenExposure
        FROM Orders
        WHERE IsOpen = true
        GROUP BY MarketId, RunnerId
        """;

    private const string AccountRiskSql = """
        SELECT AccountId, MarketId, SUM(Stake) AS OpenExposure
        FROM Orders
        WHERE IsOpen = true
        GROUP BY AccountId, MarketId
        """;

    private static readonly ReactiveRowSchema OrdersSchema =
        new("OrderId", "MarketId", "RunnerId", "AccountId", "Side", "Price", "Stake", "IsOpen");

    private readonly object _gate = new();
    private readonly Dictionary<RunnerExposureKey, RunnerExposure> _runnerExposure = [];
    private readonly Dictionary<AccountExposureKey, AccountExposure> _accountExposure = [];
    private readonly Dictionary<RunnerAccountExposureKey, decimal> _pointExposure = [];
    private readonly Dictionary<int, decimal> _marketOpenExposure = [];
    private readonly List<MarketRiskSummary> _defaultMarketRisk = [];
    private readonly IReadOnlyList<RunnerExposure> _runnerExposureView;
    private readonly IReadOnlyList<AccountExposure> _accountExposureView;
    private readonly CellValue[] _orderCells = new CellValue[8];
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

    public string Name => DataVoBenchmarkName.Format("DataVo");

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
                IsOpen BIT
            )
            """);

        for (int i = 0; i < scenario.InitialOrderCount; i++)
        {
            MarketTick tick = BenchmarkTickFactory.CreateBaselineOrder(_nextOrderId + i, scenario);
            InsertTickTyped(_context, tick, _nextOrderId + i);
            AddBaselineExposure(tick);
            AddPointExposure(tick.RunnerId, tick.AccountId, tick.Stake);
        }

        _nextOrderId += scenario.InitialOrderCount;
        _runnerSubscription = _context.SubscribeZeroAlloc(RunnerRiskSql, ApplyRunnerChange);
        _accountSubscription = _context.SubscribeZeroAlloc(AccountRiskSql, ApplyAccountChange);
        lock (_gate)
        {
            RefreshDefaultReadModelLocked();
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask IngestTickAsync(MarketTick tick, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        long orderId = OrderIdFor(tick);
        DataVoContext context = EnsureContext();
        InsertTickTyped(context, tick, orderId);
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
        foreach (MarketTick tick in ticks)
        {
            InsertTickTyped(context, tick, OrderIdFor(tick));
        }

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

    private void ApplyRunnerChange(in QueryChangeRef change)
    {
        lock (_gate)
        {
            for (int i = 0; i < change.Removed.Count; i++)
            {
                RowRef row = change.Removed[i];
                var key = new RunnerExposureKey(
                    row["MarketId"].AsInt32(),
                    row["RunnerId"].AsInt32());
                RemoveRunner(key);
            }

            for (int i = 0; i < change.Added.Count; i++)
            {
                UpsertRunner(change.Added[i]);
            }

            for (int i = 0; i < change.Updated.Count; i++)
            {
                UpsertRunner(change.Updated[i]);
            }

            RefreshDefaultReadModelLocked();
        }
    }

    private void ApplyAccountChange(in QueryChangeRef change)
    {
        lock (_gate)
        {
            for (int i = 0; i < change.Removed.Count; i++)
            {
                RowRef row = change.Removed[i];
                _accountExposure.Remove(new AccountExposureKey(
                    row["AccountId"].AsInt32(),
                    row["MarketId"].AsInt32()));
            }

            for (int i = 0; i < change.Added.Count; i++)
            {
                UpsertAccount(change.Added[i]);
            }

            for (int i = 0; i < change.Updated.Count; i++)
            {
                UpsertAccount(change.Updated[i]);
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

    private void UpsertRunner(RowRef row)
    {
        int marketId = row["MarketId"].AsInt32();
        int runnerId = row["RunnerId"].AsInt32();
        var key = new RunnerExposureKey(marketId, runnerId);
        var exposure = new RunnerExposure(
            marketId,
            runnerId,
            ToDecimal(row["BestBack"]),
            ToDecimal(row["BestLay"]),
            0m,
            ToDecimal(row["OpenExposure"]));

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

    private void UpsertAccount(RowRef row)
    {
        int accountId = row["AccountId"].AsInt32();
        int marketId = row["MarketId"].AsInt32();
        _accountExposure[new AccountExposureKey(accountId, marketId)] = new AccountExposure(
            accountId,
            marketId,
            ToDecimal(row["OpenExposure"]),
            0m);
    }

    private long OrderIdFor(MarketTick tick) => tick.Sequence > 0 ? tick.Sequence : _nextOrderId;

    private void AddBaselineExposure(MarketTick tick)
    {
        if (tick.Kind == TickKind.OrderCancelled)
        {
            return;
        }

        decimal price = decimal.Round(tick.Price, 0);
        decimal stake = decimal.Round(tick.Stake, 0);

        lock (_gate)
        {
            var runnerKey = new RunnerExposureKey(tick.MarketId, tick.RunnerId);
            if (_runnerExposure.TryGetValue(runnerKey, out RunnerExposure? existingRunner))
            {
                _runnerExposure[runnerKey] = existingRunner with
                {
                    BestBack = Math.Max(existingRunner.BestBack, price),
                    BestLay = Math.Min(existingRunner.BestLay, price),
                    OpenExposure = existingRunner.OpenExposure + stake
                };
            }
            else
            {
                _runnerExposure[runnerKey] = new RunnerExposure(
                    tick.MarketId,
                    tick.RunnerId,
                    price,
                    price,
                    0m,
                    stake);
            }

            AddMarketExposure(tick.MarketId, stake);

            var accountKey = new AccountExposureKey(tick.AccountId, tick.MarketId);
            if (_accountExposure.TryGetValue(accountKey, out AccountExposure? existingAccount))
            {
                _accountExposure[accountKey] = existingAccount with
                {
                    OpenExposure = existingAccount.OpenExposure + stake
                };
            }
            else
            {
                _accountExposure[accountKey] = new AccountExposure(tick.AccountId, tick.MarketId, stake, 0m);
            }
        }
    }

    private void InsertTickTyped(DataVoContext context, MarketTick tick, long orderId)
    {
        _orderCells[0] = CellValue.From(checked((int)orderId));
        _orderCells[1] = CellValue.From(tick.MarketId);
        _orderCells[2] = CellValue.From(tick.RunnerId);
        _orderCells[3] = CellValue.From(tick.AccountId);
        _orderCells[4] = CellValue.From(tick.Side);
        _orderCells[5] = CellValue.From(ToInt32Rounded(tick.Price));
        _orderCells[6] = CellValue.From(ToInt32Rounded(tick.Stake));
        _orderCells[7] = CellValue.From(tick.Kind != TickKind.OrderCancelled);
        context.InsertTyped("Orders", OrdersSchema, _orderCells);
    }

    private static int ToInt32Rounded(decimal value) => checked((int)decimal.Round(value, 0));

    private static bool IsDefaultQuery(RiskQuery query) =>
        query.MarketId is null && query.AccountId is null && query.RunnerId is null && query.TopMarkets == 10;

    private static decimal ToDecimal(CellValue value) => value.Type switch
    {
        CellType.Null => 0m,
        CellType.Int32 => value.AsInt32(),
        CellType.Int64 => value.AsInt64(),
        CellType.Double => Convert.ToDecimal(value.AsDouble()),
        CellType.Decimal => value.AsDecimal(),
        _ => Convert.ToDecimal(value.ToObject())
    };

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
        foreach ((int marketId, decimal exposure) in _marketOpenExposure)
        {
            if (exposure == 0m)
            {
                continue;
            }

            InsertDefaultMarketRisk(new MarketRiskSummary(marketId, exposure, 0m, _scenario.SubscriberCount));
        }

        TouchDefaultReadModelAsOfLocked();
    }

    private void InsertDefaultMarketRisk(MarketRiskSummary candidate)
    {
        int insertAt = 0;
        while (insertAt < _defaultMarketRisk.Count
               && ComesBeforeOrEqual(_defaultMarketRisk[insertAt], candidate))
        {
            insertAt++;
        }

        if (insertAt >= 10)
        {
            return;
        }

        _defaultMarketRisk.Insert(insertAt, candidate);
        if (_defaultMarketRisk.Count > 10)
        {
            _defaultMarketRisk.RemoveAt(_defaultMarketRisk.Count - 1);
        }
    }

    private static bool ComesBeforeOrEqual(MarketRiskSummary current, MarketRiskSummary candidate)
    {
        int exposureOrder = current.TotalOpenExposure.CompareTo(candidate.TotalOpenExposure);
        if (exposureOrder != 0)
        {
            return exposureOrder > 0;
        }

        return current.MarketId <= candidate.MarketId;
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
