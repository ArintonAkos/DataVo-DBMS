using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using RowRef = DataVo.Core.Runtime.Reactive.RowRef;

// A live game leaderboard. Every tick, a burst of match events is written in one transaction; three standing
// reactive views are maintained incrementally from those deltas (DBSP-style Z-set IVM) and drained with
// DispatchPendingNotifications. No view ever re-reads its base tables.
//
//   dotnet run                      InMemory engine, 200 ticks x 1,000 events
//   dotnet run -- --lsm             the durable LSM engine (WAL without fsync per commit)
//   dotnet run -- --ticks 500 --events 2000
const int Players = 2_000;
const int Teams = 8;
bool lsm = args.Contains("--lsm");
int ticks = IntArg(args, "--ticks", 200);
int eventsPerTick = IntArg(args, "--events", 1_000);
bool batched = !args.Contains("--autocommit"); // one transaction per tick (default) or one per statement
string only = StringArg(args, "--only") ?? "all"; // teams | top | feed | none: keep one view, to see what each costs
string dataDirectory = Path.Combine(Path.GetTempPath(), $"datavo-leaderboard-{Guid.NewGuid():N}");

using var db = new DataVoContext(lsm
    ? new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = dataDirectory, LsmStrictFsync = false }
    : new DataVoConfig { StorageMode = StorageMode.InMemory });
try
{
    Run(db, "CREATE DATABASE Arena");
    Run(db, "USE Arena");
    Run(db, "CREATE TABLE players (Id INT PRIMARY KEY, Name VARCHAR(24), Team INT)");
    Run(db, "CREATE TABLE standings (PlayerId INT PRIMARY KEY, Name VARCHAR(24), Team INT, Score INT)");
    Run(db, "CREATE TABLE events (Id INT PRIMARY KEY, PlayerId INT, Team INT, Points INT, At DATETIME)");

    Run(db, "BEGIN");
    DataVoPreparedStatement addPlayer = db.Prepare("INSERT INTO players (Id, Name, Team) VALUES (@id, @name, @team)");
    DataVoPreparedStatement addStanding = db.Prepare("INSERT INTO standings (PlayerId, Name, Team, Score) VALUES (@id, @name, @team, 0)");
    for (int id = 1; id <= Players; id++)
    {
        string name = $"player-{id:D4}";
        Check(addPlayer.Execute(("id", id), ("name", name), ("team", id % Teams)));
        Check(addStanding.Execute(("id", id), ("name", name), ("team", id % Teams)));
    }

    Run(db, "COMMIT");

    // --- The three standing views -------------------------------------------------------------------------------
    // 1. Team totals: an incremental GROUP BY. Each event changes one group; only that group is re-emitted.
    var teamEvents = new long[Teams];
    var teamPoints = new long[Teams];
    long teamDeltas = 0;
    using IDisposable? teams = only is not ("all" or "teams") ? null : db.SubscribeZeroAlloc(
        "SELECT Team, COUNT(*) AS Events, SUM(Points) AS Points FROM events GROUP BY Team",
        (in QueryChangeRef change) =>
        {
            ApplyTeams(change.Added, teamEvents, teamPoints);
            ApplyTeams(change.Updated, teamEvents, teamPoints);
            teamDeltas += change.Added.Count + change.Updated.Count;
        });

    // 2. Top-10 leaderboard: an incremental ORDER BY ... LIMIT over the standings table.
    var top = new (int Id, string Name, int Score)[10];
    long topDeltas = 0;
    var leaderboard = new SortedSet<(int Score, int Id)>(Comparer<(int Score, int Id)>.Create((a, b) => b.Score != a.Score ? b.Score.CompareTo(a.Score) : a.Id.CompareTo(b.Id)));
    var names = new Dictionary<int, string>();
    using IDisposable? topTen = only is not ("all" or "top") ? null : db.SubscribeZeroAlloc(
        "SELECT PlayerId, Name, Score FROM standings ORDER BY Score DESC LIMIT 10",
        (in QueryChangeRef change) =>
        {
            RowSet removed = change.Removed;
            for (int i = 0; i < removed.Count; i++)
            {
                RowRef row = removed[i];
                leaderboard.Remove((row[2].AsInt32(), row[0].AsInt32()));
            }

            RowSet before = change.UpdatedBefore;
            for (int i = 0; i < before.Count; i++)
            {
                RowRef row = before[i];
                leaderboard.Remove((row[2].AsInt32(), row[0].AsInt32()));
            }

            AddRows(change.Added, leaderboard, names);
            AddRows(change.Updated, leaderboard, names);
            topDeltas += removed.Count + change.Added.Count + change.Updated.Count;
        });

    // 3. "Big plays" feed: an incremental two-table JOIN with a filter, events x players.
    long bigPlays = 0;
    string lastBigPlay = "";
    using IDisposable? feed = only is not ("all" or "feed") ? null : db.SubscribeZeroAlloc(
        "SELECT e.Id, p.Name, e.Points FROM events e INNER JOIN players p ON e.PlayerId = p.Id WHERE e.Points >= 95",
        (in QueryChangeRef change) =>
        {
            RowSet added = change.Added;
            bigPlays += added.Count;
            if (added.Count > 0)
            {
                RowRef last = added[added.Count - 1];
                lastBigPlay = $"{last[1].AsString()} scored {last[2].AsInt32()}";
            }
        });

    // --- The game loop ------------------------------------------------------------------------------------------
    DataVoPreparedStatement addEvent = db.Prepare("INSERT INTO events (Id, PlayerId, Team, Points, At) VALUES (@id, @player, @team, @points, @at)");
    DataVoPreparedStatement addScore = db.Prepare("UPDATE standings SET Score = Score + @points WHERE PlayerId = @player");
    var random = new Random(2026);
    var clock = new DateTime(2026, 11, 24, 18, 0, 0, DateTimeKind.Utc);
    int nextEvent = 1;
    double writeMs = 0;
    double dispatchMs = 0;
    long dispatchAllocated = 0;

    for (int tick = 1; tick <= ticks; tick++)
    {
        long start = Stopwatch.GetTimestamp();
        if (batched)
        {
            Run(db, "BEGIN");
        }

        for (int i = 0; i < eventsPerTick; i++)
        {
            int player = 1 + (int)(Math.Pow(random.NextDouble(), 1.6) * Players); // a few players score more often
            int points = random.Next(1, 101);
            clock = clock.AddMilliseconds(3);
            Check(addEvent.Execute(("id", nextEvent++), ("player", player), ("team", player % Teams), ("points", points), ("at", clock)));
            Check(addScore.Execute(("points", points), ("player", player)));
        }

        if (batched)
        {
            Run(db, "COMMIT");
        }

        long written = Stopwatch.GetTimestamp();

        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        db.DispatchPendingNotifications();
        dispatchAllocated += GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        long dispatched = Stopwatch.GetTimestamp();

        writeMs += Stopwatch.GetElapsedTime(start, written).TotalMilliseconds;
        dispatchMs += Stopwatch.GetElapsedTime(written, dispatched).TotalMilliseconds;

        if (tick % Math.Max(1, ticks / 4) == 0 || tick == ticks)
        {
            Render(tick, leaderboard, names, teamEvents, teamPoints, bigPlays, lastBigPlay);
        }
    }

    // --- Cost of keeping the views live, against re-running them --------------------------------------------------
    long totalEvents = (long)ticks * eventsPerTick;
    long inputDeltas = totalEvents * 2; // one INSERT and one UPDATE per event
    double rerunMs = TimeRequery(db);
    Console.WriteLine();
    Console.WriteLine($"Engine:                {(lsm ? "Lsm" : "InMemory")}");
    Console.WriteLine($"Events written:        {totalEvents:N0} ({totalEvents / (writeMs / 1000):N0} events/s, {inputDeltas:N0} row deltas)");
    Console.WriteLine($"Incremental upkeep:    {dispatchMs:N1} ms total for 3 views = {dispatchMs * 1_000_000 / inputDeltas:N0} ns per input row delta");
    Console.WriteLine($"Per tick:              {dispatchMs * 1000 / ticks:N1} µs to update all views vs {rerunMs * 1000:N1} µs to re-run the same 3 queries once");
    Console.WriteLine($"Dispatch allocations:  {dispatchAllocated / (double)ticks:N0} B per tick (views + this program's callbacks)");
    Console.WriteLine($"View deltas emitted:   teams {teamDeltas:N0}, top-10 {topDeltas:N0}, big plays {bigPlays:N0}");
    if (only == "all")
    {
        Verify(db, leaderboard, teamPoints);
    }
    return 0;
}
finally
{
    if (Directory.Exists(dataDirectory))
    {
        Directory.Delete(dataDirectory, recursive: true);
    }
}

static void ApplyTeams(RowSet rows, long[] events, long[] points)
{
    for (int i = 0; i < rows.Count; i++)
    {
        RowRef row = rows[i];
        int team = row[0].AsInt32();
        events[team] = row[1].AsInt64();
        points[team] = row[2].Type == CellType.Int64 ? row[2].AsInt64() : row[2].AsInt32();
    }
}

static void AddRows(RowSet rows, SortedSet<(int Score, int Id)> board, Dictionary<int, string> names)
{
    for (int i = 0; i < rows.Count; i++)
    {
        RowRef row = rows[i];
        int id = row[0].AsInt32();
        board.Add((row[2].AsInt32(), id));
        if (!names.ContainsKey(id))
        {
            names[id] = row[1].AsString()!;
        }
    }
}

static void Render(int tick, SortedSet<(int Score, int Id)> board, Dictionary<int, string> names, long[] teamEvents, long[] teamPoints, long bigPlays, string lastBigPlay)
{
    Console.WriteLine();
    Console.WriteLine($"── tick {tick} ─────────────────────────────────────────");
    int rank = 1;
    foreach ((int score, int id) in board.Take(10))
    {
        Console.WriteLine($"  {rank++,2}. {names[id],-12} {score,8:N0}");
    }

    Console.WriteLine("  teams: " + string.Join("  ", Enumerable.Range(0, teamPoints.Length).Select(t => $"T{t}={teamPoints[t]:N0}")));
    Console.WriteLine($"  big plays: {bigPlays:N0} (latest: {lastBigPlay})");
}

static double TimeRequery(DataVoContext db)
{
    const int Runs = 20;
    var timer = Stopwatch.StartNew();
    for (int i = 0; i < Runs; i++)
    {
        Run(db, "SELECT Team, COUNT(*) AS Events, SUM(Points) AS Points FROM events GROUP BY Team");
        Run(db, "SELECT PlayerId, Name, Score FROM standings ORDER BY Score DESC LIMIT 10");
        Run(db, "SELECT e.Id, p.Name, e.Points FROM events e INNER JOIN players p ON e.PlayerId = p.Id WHERE e.Points >= 95");
    }

    return timer.Elapsed.TotalMilliseconds / Runs;
}

// The incrementally maintained state must equal a from-scratch query of the base tables.
static void Verify(DataVoContext db, SortedSet<(int Score, int Id)> board, long[] teamPoints)
{
    // Ties at 10th place may admit either tied player, so compare the scores, then each listed player's real score.
    QueryResult top = db.Execute("SELECT PlayerId, Score FROM standings ORDER BY Score DESC LIMIT 10").Last();
    Check(top);
    bool topMatches = top.Data.Select(r => Convert.ToInt32(r["Score"])).SequenceEqual(board.Take(10).Select(e => e.Score))
        && board.Take(10).All(e => Convert.ToInt32(db.Execute($"SELECT Score FROM standings WHERE PlayerId = {e.Id}").Last().Data[0]["Score"]) == e.Score);

    QueryResult totals = db.Execute("SELECT Team, SUM(Points) AS Points FROM events GROUP BY Team").Last();
    Check(totals);
    bool teamsMatch = totals.Data.All(r => Convert.ToInt64(r["Points"]) == teamPoints[Convert.ToInt32(r["Team"])]);

    Console.WriteLine($"Views match a full re-query: top-10 {(topMatches ? "yes" : "NO")}, team totals {(teamsMatch ? "yes" : "NO")}");
    if (!topMatches || !teamsMatch)
    {
        throw new InvalidOperationException("An incremental view diverged from its base tables.");
    }
}

static void Run(DataVoContext db, string sql) => Check(db.Execute(sql).Last());

static void Check(QueryResult result)
{
    if (result.IsError)
    {
        throw new InvalidOperationException(string.Join(" | ", result.Messages));
    }
}

static string? StringArg(string[] args, string name)
{
    int at = Array.IndexOf(args, name);
    return at >= 0 && at + 1 < args.Length ? args[at + 1] : null;
}

static int IntArg(string[] args, string name, int fallback)
{
    int at = Array.IndexOf(args, name);
    return at >= 0 && at + 1 < args.Length && int.TryParse(args[at + 1], out int value) ? value : fallback;
}
