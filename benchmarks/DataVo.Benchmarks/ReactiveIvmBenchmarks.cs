using BenchmarkDotNet.Attributes;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Benchmarks;

/// <summary>
/// The write and view-upkeep cost of a reactive game backend, per updated row. Each invocation runs one
/// transaction of 1,000 prepared single-row updates, commits it and dispatches the live views. The tables keep
/// their size, so every iteration measures the same steady state. DataVo only.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description><see cref="ScoreUpdatesWithTopTen"/>: <c>UPDATE standings SET Score = Score + @points WHERE
/// PlayerId = @id</c> with <c>SELECT ... ORDER BY Score DESC LIMIT 10</c> maintained incrementally.</description></item>
/// <item><description><see cref="EventUpdatesWithJoinFeed"/>: <c>UPDATE events SET Points = @points WHERE Id = @id</c>
/// with the filtered two-table join <c>events JOIN roster WHERE Points &gt;= 95</c> maintained incrementally.</description></item>
/// </list>
/// </remarks>
[MemoryDiagnoser]
public class ReactiveIvmBenchmarks
{
    private const int Players = 2_000;
    private const int Events = 20_000;
    private const int UpdatesPerTransaction = 1_000;

    private ScratchDirectory _scratch = null!;
    private DataVoContext _db = null!;
    private DataVoPreparedStatement _addScore = null!;
    private DataVoPreparedStatement _setPoints = null!;
    private IDisposable _topTen = null!;
    private IDisposable _feed = null!;
    private Random _random = null!;
    private long _deltas;

    [ParamsAllValues]
    public Storage Storage { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _scratch = new ScratchDirectory();
        _db = Engines.OpenDataVo(Storage, _scratch.Path);
        Run("CREATE TABLE roster (Id INT PRIMARY KEY, Name VARCHAR(24), Team INT)");
        Run("CREATE TABLE standings (PlayerId INT PRIMARY KEY, Name VARCHAR(24), Team INT, Score INT)");
        Run("CREATE TABLE events (Id INT PRIMARY KEY, PlayerId INT, Team INT, Points INT)");

        var random = new Random(2026);
        Run("BEGIN");
        DataVoPreparedStatement addPlayer = _db.Prepare("INSERT INTO roster (Id, Name, Team) VALUES (@id, @name, @team)");
        DataVoPreparedStatement addStanding = _db.Prepare("INSERT INTO standings (PlayerId, Name, Team, Score) VALUES (@id, @name, @team, 0)");
        DataVoPreparedStatement addEvent = _db.Prepare("INSERT INTO events (Id, PlayerId, Team, Points) VALUES (@id, @player, @team, @points)");
        for (int id = 1; id <= Players; id++)
        {
            Check(addPlayer.Execute(("id", id), ("name", $"player-{id:D4}"), ("team", id % 8)));
            Check(addStanding.Execute(("id", id), ("name", $"player-{id:D4}"), ("team", id % 8)));
        }

        for (int id = 1; id <= Events; id++)
        {
            int player = random.Next(1, Players + 1);
            Check(addEvent.Execute(("id", id), ("player", player), ("team", player % 8), ("points", random.Next(1, 101))));
        }

        Run("COMMIT");

        _topTen = _db.SubscribeZeroAlloc(
            "SELECT PlayerId, Name, Score FROM standings ORDER BY Score DESC LIMIT 10",
            (in QueryChangeRef change) => _deltas += change.Added.Count + change.Updated.Count);
        _feed = _db.SubscribeZeroAlloc(
            "SELECT e.Id, p.Name, e.Points FROM events e INNER JOIN roster p ON e.PlayerId = p.Id WHERE e.Points >= 95",
            (in QueryChangeRef change) => _deltas += change.Added.Count + change.Removed.Count);

        _addScore = _db.Prepare("UPDATE standings SET Score = Score + @points WHERE PlayerId = @id");
        _setPoints = _db.Prepare("UPDATE events SET Points = @points WHERE Id = @id");
        _random = new Random(7);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _topTen.Dispose();
        _feed.Dispose();
        _db.Dispose();
        _scratch.Dispose();
    }

    /// <summary>One transaction of score increments, its commit, and the top-10 view's upkeep; per update.</summary>
    [Benchmark(OperationsPerInvoke = UpdatesPerTransaction)]
    public long ScoreUpdatesWithTopTen()
    {
        Run("BEGIN");
        for (int i = 0; i < UpdatesPerTransaction; i++)
        {
            int player = 1 + (int)(Math.Pow(_random.NextDouble(), 1.6) * Players);
            Check(_addScore.Execute(("points", _random.Next(1, 101)), ("id", Math.Min(player, Players))));
        }

        Run("COMMIT");
        _db.DispatchPendingNotifications();
        return _deltas;
    }

    /// <summary>One transaction of event re-scores, its commit, and the join feed's upkeep; per update.</summary>
    [Benchmark(OperationsPerInvoke = UpdatesPerTransaction)]
    public long EventUpdatesWithJoinFeed()
    {
        Run("BEGIN");
        for (int i = 0; i < UpdatesPerTransaction; i++)
        {
            Check(_setPoints.Execute(("points", _random.Next(1, 101)), ("id", _random.Next(1, Events + 1))));
        }

        Run("COMMIT");
        _db.DispatchPendingNotifications();
        return _deltas;
    }

    private void Run(string sql) => Check(_db.Execute(sql)[^1]);

    private static void Check(QueryResult result)
    {
        if (result.IsError)
        {
            throw new InvalidOperationException(string.Join(" | ", result.Messages));
        }
    }
}
