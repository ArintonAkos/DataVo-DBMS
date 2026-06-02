using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveJoinRoutingTests
{
    [Fact]
    public void Accepts_TwoTableEquiJoin_RejectsThreeTable()
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(StorageMode.InMemory, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT, Name VARCHAR(20))");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, Kind VARCHAR(20))");

        using var inner = ctx.Subscribe("SELECT R.Id, S.Kind FROM R INNER JOIN S ON R.Gid = S.Id", _ => { });
        using var left = ctx.Subscribe("SELECT R.Id, S.Kind FROM R LEFT JOIN S ON R.Gid = S.Id", _ => { });
        Assert.Throws<NotSupportedException>(() =>
            ctx.Subscribe("SELECT R.Id FROM R JOIN S ON R.Gid=S.Id JOIN R x ON x.Id=R.Id", _ => { }));
    }
}
