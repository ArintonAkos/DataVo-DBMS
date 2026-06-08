using System.Diagnostics;
using DataVo.Benchmarks.Common;
using DataVo.Core;

namespace TradingOrderBook.Demo;

internal static class Program
{
    private static readonly string[] LiveQueries =
    [
        "SELECT Symbol, MAX(Price) AS BestBid FROM Orders WHERE Side = 'B' AND Status = 'OPEN' GROUP BY Symbol",
        "SELECT Symbol, MIN(Price) AS BestAsk FROM Orders WHERE Side = 'S' AND Status = 'OPEN' GROUP BY Symbol",
        "SELECT AccountId, Symbol, SUM(Qty) AS NetQty FROM Positions GROUP BY AccountId, Symbol",
        "SELECT Symbol, Price, Qty FROM Trades ORDER BY Ts DESC LIMIT 100"
    ];

    public static void Main(string[] args)
    {
        BenchmarkOptions options = BenchmarkOptions.Parse(args, new BenchmarkOptions
        {
            Rows = 20_000,
            Ticks = 500,
            WarmupTicks = 50,
            MutationsPerTick = 100,
            Seed = 20260621
        });

        List<ScenarioRunResult> results = [];
        if (options.Mode.Equals("both", StringComparison.OrdinalIgnoreCase)
            || options.Mode.Equals("reactive", StringComparison.OrdinalIgnoreCase))
        {
            results.Add(Run(options, reactive: true));
        }

        if (options.Mode.Equals("both", StringComparison.OrdinalIgnoreCase)
            || options.Mode.Equals("polling", StringComparison.OrdinalIgnoreCase))
        {
            results.Add(Run(options, reactive: false));
        }

        if (!string.IsNullOrWhiteSpace(options.OutputPath) && results.Count > 1)
        {
            string directory = Path.GetDirectoryName(Path.GetFullPath(options.OutputPath)) ?? ".";
            string stem = Path.GetFileNameWithoutExtension(options.OutputPath);
            string extension = Path.GetExtension(options.OutputPath);
            for (int i = 0; i < results.Count; i++)
            {
                string path = Path.Combine(directory, $"{stem}.{results[i].Architecture}{extension}");
                ScenarioResultWriter.Write(results[i], path);
            }
        }
        else
        {
            foreach (ScenarioRunResult result in results)
            {
                ScenarioResultWriter.Write(result, options.OutputPath);
            }
        }
    }

    private static ScenarioRunResult Run(BenchmarkOptions options, bool reactive)
    {
        using BenchmarkDatabase database = DataVoBenchmarkContext.Create(options.Storage, "trading_order_book");
        DataVoContext context = database.Context;
        Setup(context, options.Rows);

        var deltaCounter = new ReactiveDeltaCounter();
        List<IDisposable> subscriptions = [];
        if (reactive)
        {
            foreach (string query in LiveQueries)
            {
                subscriptions.Add(context.Subscribe(query, deltaCounter.Apply));
            }
        }

        var random = new Random(options.Seed);
        var tickLatency = new LatencyRecorder();
        var mutationLatency = new LatencyRecorder();
        var viewLatency = new LatencyRecorder();
        long pollingRows = 0;
        DateTimeOffset started = DateTimeOffset.UtcNow;
        GcRecorder beforeGc = GcRecorder.Capture();
        var total = Stopwatch.StartNew();
        int nextTradeId = options.Rows + 1;

        for (int tick = 0; tick < options.WarmupTicks + options.Ticks; tick++)
        {
            bool measure = tick >= options.WarmupTicks;
            long tickStart = tickLatency.Start();

            long mutationStart = mutationLatency.Start();
            ApplyMutations(context, random, options.Rows, options.MutationsPerTick, tick, ref nextTradeId);
            if (measure)
            {
                mutationLatency.AddElapsed(mutationStart);
            }

            long viewStart = viewLatency.Start();
            if (reactive)
            {
                context.DispatchPendingNotifications();
            }
            else
            {
                foreach (string query in LiveQueries)
                {
                    pollingRows += context.Query(query).Count;
                }
            }

            if (measure)
            {
                viewLatency.AddElapsed(viewStart);
                tickLatency.AddElapsed(tickStart);
            }
        }

        total.Stop();
        GcSummary gc = GcRecorder.Capture().Since(beforeGc);

        foreach (IDisposable subscription in subscriptions)
        {
            subscription.Dispose();
        }

        return new ScenarioRunResult(
            Scenario: "trading-order-book-risk",
            Architecture: reactive ? "datavo-reactive" : "polling-full-recompute",
            Storage: options.Storage,
            Rows: options.Rows,
            Ticks: options.Ticks,
            WarmupTicks: options.WarmupTicks,
            MutationsPerTick: options.MutationsPerTick,
            Seed: options.Seed,
            StartedAtUtc: started,
            Duration: total.Elapsed,
            TickLatency: tickLatency.Snapshot(),
            MutationLatency: mutationLatency.Snapshot(),
            ViewMaintenanceLatency: viewLatency.Snapshot(),
            Gc: gc,
            ReactiveDeltas: reactive ? deltaCounter.Snapshot() : null,
            PollingRowsReturned: pollingRows,
            FrameBudgetMissRate60Hz: tickLatency.RateOver(16.666),
            FrameBudgetMissRate120Hz: tickLatency.RateOver(8.333),
            Notes: new Dictionary<string, string>
            {
                ["comparison"] = "DataVo reactive subscriptions are compared with full live-view polling for trading dashboards and risk state.",
                ["workload"] = "Order churn, top-of-book MIN/MAX, positions SUM, and recent-trades top-k.",
                ["honesty"] = "This models realtime read-model maintenance, not a full exchange matching engine."
            });
    }

    private static void Setup(DataVoContext context, int orders)
    {
        context.ExecuteOk("CREATE DATABASE Trading");
        context.ExecuteOk("USE Trading");
        context.ExecuteOk("CREATE TABLE Orders (OrderId INT PRIMARY KEY, Symbol VARCHAR(12), Side VARCHAR(1), Price INT, Qty INT, Status VARCHAR(10))");
        context.ExecuteOk("CREATE TABLE Trades (TradeId INT PRIMARY KEY, Symbol VARCHAR(12), Price INT, Qty INT, Ts INT)");
        context.ExecuteOk("CREATE TABLE Positions (PositionId INT PRIMARY KEY, AccountId INT, Symbol VARCHAR(12), Qty INT)");

        for (int id = 1; id <= orders; id++)
        {
            string symbol = Symbol(id);
            string side = id % 2 == 0 ? "B" : "S";
            int price = 100_000 + (id % 2500);
            int qty = 1 + (id % 100);
            string status = id % 11 == 0 ? "CLOSED" : "OPEN";
            context.ExecuteOk($"INSERT INTO Orders VALUES ({id}, '{symbol}', '{side}', {price}, {qty}, '{status}')");
        }

        int positions = Math.Max(1000, orders / 10);
        for (int id = 1; id <= positions; id++)
        {
            context.ExecuteOk($"INSERT INTO Positions VALUES ({id}, {1 + (id % 250)}, '{Symbol(id)}', {(id % 2 == 0 ? 1 : -1) * (id % 500)})");
        }

        for (int id = 1; id <= 500; id++)
        {
            context.ExecuteOk($"INSERT INTO Trades VALUES ({id}, '{Symbol(id)}', {100_000 + (id % 2500)}, {1 + (id % 100)}, {id})");
        }
    }

    private static void ApplyMutations(DataVoContext context, Random random, int orders, int mutations, int tick, ref int nextTradeId)
    {
        for (int i = 0; i < mutations; i++)
        {
            int orderId = random.Next(1, orders + 1);
            string symbol = Symbol(orderId);
            int price = 99_000 + random.Next(0, 4_000);
            int qty = 1 + random.Next(0, 250);
            string status = (tick + i + orderId) % 23 == 0 ? "CLOSED" : "OPEN";
            context.ExecuteOk($"UPDATE Orders SET Price = {price}, Qty = {qty}, Status = '{status}' WHERE OrderId = {orderId}");

            if (i % 5 == 0)
            {
                int tradeId = nextTradeId++;
                context.ExecuteOk($"INSERT INTO Trades VALUES ({tradeId}, '{symbol}', {price}, {qty}, {tick * mutations + i + 1000})");
            }

            if (i % 7 == 0)
            {
                int positionId = 1 + random.Next(0, Math.Max(1000, orders / 10));
                int positionQty = random.Next(-1000, 1001);
                context.ExecuteOk($"UPDATE Positions SET Qty = {positionQty} WHERE PositionId = {positionId}");
            }
        }
    }

    private static string Symbol(int id) => $"SYM{id % 250:D3}";
}
