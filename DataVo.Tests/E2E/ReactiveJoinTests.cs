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
}
