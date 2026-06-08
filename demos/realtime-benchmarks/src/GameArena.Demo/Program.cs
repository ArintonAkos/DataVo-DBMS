using System.Diagnostics;
using DataVo.Benchmarks.Common;
using DataVo.Core;

namespace GameArena.Demo;

internal static class Program
{
    private static readonly string[] LiveQueries =
    [
        "SELECT Id, X, Y, Health FROM Players WHERE Zone = 'arena-7'",
        "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team",
        "SELECT Id, Score FROM Players ORDER BY Score DESC LIMIT 20",
        "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7'"
    ];

    public static void Main(string[] args)
    {
        BenchmarkOptions options = BenchmarkOptions.Parse(args, new BenchmarkOptions
        {
            Rows = 10_000,
            Ticks = 500,
            WarmupTicks = 50,
            MutationsPerTick = 100,
            Seed = 20260620
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
        using BenchmarkDatabase database = DataVoBenchmarkContext.Create(options.Storage, "game_arena");
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

        for (int tick = 0; tick < options.WarmupTicks + options.Ticks; tick++)
        {
            bool measure = tick >= options.WarmupTicks;
            long tickStart = tickLatency.Start();

            long mutationStart = mutationLatency.Start();
            ApplyMutations(context, random, options.Rows, options.MutationsPerTick, tick);
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
            Scenario: "game-arena-120hz",
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
                ["comparison"] = "DataVo reactive subscriptions are compared with SQLite-style full live-view polling.",
                ["workload"] = "Player movement, health, score, inventory joins, team aggregate, and leaderboard top-k.",
                ["honesty"] = "This does not claim DataVo is faster than SQLite for every query; it measures live-view maintenance under small frequent mutations."
            });
    }

    private static void Setup(DataVoContext context, int players)
    {
        context.ExecuteOk("CREATE DATABASE GameArena");
        context.ExecuteOk("USE GameArena");
        context.ExecuteOk("CREATE TABLE Players (Id INT PRIMARY KEY, Zone VARCHAR(20), Team VARCHAR(10), X INT, Y INT, Health INT, Score INT)");
        context.ExecuteOk("CREATE TABLE Inventory (InventoryId INT PRIMARY KEY, PlayerId INT, ItemId INT, Slot INT)");

        for (int id = 1; id <= players; id++)
        {
            string zone = id % 10 == 0 ? "arena-7" : $"arena-{id % 10}";
            string team = id % 2 == 0 ? "blue" : "red";
            int health = 50 + (id % 51);
            int score = id % 1000;
            context.ExecuteOk($"INSERT INTO Players VALUES ({id}, '{zone}', '{team}', {id % 500}, {(id * 3) % 500}, {health}, {score})");

            if (id % 3 == 0)
            {
                context.ExecuteOk($"INSERT INTO Inventory VALUES ({id}, {id}, {10_000 + id}, {id % 8})");
            }
        }
    }

    private static void ApplyMutations(DataVoContext context, Random random, int players, int mutations, int tick)
    {
        for (int i = 0; i < mutations; i++)
        {
            int id = random.Next(1, players + 1);
            int x = random.Next(0, 1000);
            int y = random.Next(0, 1000);
            int health = random.Next(0, 101);
            int score = (tick * 17 + id + i) % 50_000;
            string zone = (tick + i + id) % 25 == 0 ? "arena-7" : $"arena-{id % 10}";
            context.ExecuteOk($"UPDATE Players SET X = {x}, Y = {y}, Health = {health}, Score = {score}, Zone = '{zone}' WHERE Id = {id}");
        }
    }
}
