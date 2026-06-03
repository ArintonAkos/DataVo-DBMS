using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveJoinTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Inner_MaintainsJoin_OnBothSides(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT)");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, Kind VARCHAR(20))");

        var live = new Dictionary<string, (int rid, string kind)>(); // key "rid|sid"
        using var sub = ctx.Subscribe("SELECT R.Id, S.Kind FROM R INNER JOIN S ON R.Gid = S.Id", qc =>
        {
            foreach (var r in qc.Added.Concat(qc.Updated))
                live[r["R.Id"] + "|" + r["__sid"]] = (Convert.ToInt32(r["R.Id"]), (string)r["S.Kind"]!);
            foreach (var r in qc.Removed) live.Remove(r["R.Id"] + "|" + r["__sid"]);
        });

        ctx.Execute("INSERT INTO S VALUES (100, 'gold')");
        ctx.Execute("INSERT INTO R VALUES (1, 100)");   // matches -> Added
        ctx.Execute("INSERT INTO R VALUES (2, 999)");   // no match -> nothing
        ctx.DispatchPendingNotifications();

        var expected = ctx.Execute("SELECT R.Id, S.Kind FROM R INNER JOIN S ON R.Gid = S.Id")
            .Single().Data.Select(d => (Convert.ToInt32(d["R.Id"]), (string)d["S.Kind"]!)).OrderBy(x => x).ToArray();
        Assert.Equal(expected, live.Values.OrderBy(x => (x.rid, x.kind)).Select(x => (x.rid, x.kind)).ToArray());

        ctx.Execute("DELETE FROM S WHERE Id = 100"); // retracts the joined row
        ctx.DispatchPendingNotifications();
        Assert.Empty(live);
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Left_NullPads_AndTransitionsWithMatches(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT)");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, Kind VARCHAR(20))");

        // Live view keyed by output identity: rid|sid, with sid "∅" for a null-padded row.
        var live = new Dictionary<string, (int rid, string? kind)>();
        using var sub = ctx.Subscribe("SELECT R.Id, S.Kind FROM R LEFT JOIN S ON R.Gid = S.Id", qc =>
        {
            foreach (var r in qc.Added.Concat(qc.Updated))
                live[Key(r)] = (Convert.ToInt32(r["R.Id"]), r["S.Kind"] as string);
            foreach (var r in qc.Removed) live.Remove(Key(r));
        });

        static string Key(IReadOnlyDictionary<string, object?> r) =>
            (string)r["__rid"]! + "|" + ((r["__sid"] as string) ?? "∅");

        // Unmatched left row -> appears null-padded.
        ctx.Execute("INSERT INTO R VALUES (1, 100)");
        ctx.DispatchPendingNotifications();
        Assert.Single(live);
        Assert.Equal((1, (string?)null), live.Values.Single());

        // The matching right row arrives -> null-padded row retracted, real match added.
        ctx.Execute("INSERT INTO S VALUES (100, 'gold')");
        ctx.DispatchPendingNotifications();
        Assert.Single(live);
        Assert.Equal((1, "gold"), live.Values.Single());

        // The last match leaves -> null-padded row reappears.
        ctx.Execute("DELETE FROM S WHERE Id = 100");
        ctx.DispatchPendingNotifications();
        Assert.Single(live);
        Assert.Equal((1, (string?)null), live.Values.Single());

        // Match it against the recompute for good measure.
        var expected = ctx.Execute("SELECT R.Id, S.Kind FROM R LEFT JOIN S ON R.Gid = S.Id")
            .Single().Data.Select(d => (Convert.ToInt32(d["R.Id"]), d["S.Kind"] as string)).OrderBy(x => x).ToArray();
        Assert.Equal(expected, live.Values.OrderBy(x => x).ToArray());
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Full_NullPads_BothSides(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT)");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, Kind VARCHAR(20))");

        var live = new Dictionary<string, (int? rid, string? kind)>();
        using var sub = ctx.Subscribe("SELECT R.Id, S.Kind FROM R FULL JOIN S ON R.Gid = S.Id", qc =>
        {
            foreach (var r in qc.Added.Concat(qc.Updated))
                live[Key(r)] = (r["R.Id"] is null ? null : Convert.ToInt32(r["R.Id"]), r["S.Kind"] as string);
            foreach (var r in qc.Removed) live.Remove(Key(r));
        });

        static string Key(IReadOnlyDictionary<string, object?> r) =>
            ((r["__rid"] as string) ?? "∅") + "|" + ((r["__sid"] as string) ?? "∅");

        ctx.Execute("INSERT INTO R VALUES (1, 100)");  // unmatched left
        ctx.Execute("INSERT INTO S VALUES (200, 'x')"); // unmatched right
        ctx.Execute("INSERT INTO S VALUES (100, 'm')"); // matches R(1)
        ctx.DispatchPendingNotifications();

        var expected = ctx.Execute("SELECT R.Id, S.Kind FROM R FULL JOIN S ON R.Gid = S.Id")
            .Single().Data.Select(d => (d["R.Id"] is null ? (int?)null : Convert.ToInt32(d["R.Id"]), d["S.Kind"] as string))
            .OrderBy(x => x.Item1 ?? -1).ThenBy(x => x.Item2).ToArray();

        var actual = live.Values.OrderBy(x => x.rid ?? -1).ThenBy(x => x.kind).ToArray();
        Assert.Equal(expected, actual);
    }
}
