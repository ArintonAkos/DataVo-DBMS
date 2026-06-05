using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveUnionTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void Union_Dedups_UnionAll_Keeps(StorageMode mode)
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE A (Id INT PRIMARY KEY, V INT)");
        ctx.Execute("CREATE TABLE B (Id INT PRIMARY KEY, V INT)");

        var u = new List<int>();
        var ua = new List<int>();
        using var s1 = ctx.Subscribe("SELECT V FROM A UNION SELECT V FROM B", qc => Apply(u, qc));
        using var s2 = ctx.Subscribe("SELECT V FROM A UNION ALL SELECT V FROM B", qc => Apply(ua, qc));

        ctx.Execute("INSERT INTO A VALUES (1, 7)");
        ctx.Execute("INSERT INTO B VALUES (2, 7)");
        ctx.DispatchPendingNotifications();

        Assert.Equal(new[] { 7 }, u.OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 7, 7 }, ua.OrderBy(x => x).ToArray());

        static void Apply(List<int> sink, QueryChange qc)
        {
            foreach (var r in qc.Added) sink.Add(Convert.ToInt32(r["V"]));
            foreach (var r in qc.Removed) sink.Remove(Convert.ToInt32(r["V"]));
        }
    }
}
