using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class ReactiveShapeRoutingTests
{
    [Fact]
    public void Accepts_Aggregate_And_TopK_Rejects_Join()
    {
        using var ctx = ChangeCaptureIntegrationTests.NewContext(StorageMode.InMemory, out _);
        ctx.Execute("CREATE TABLE P (Id INT PRIMARY KEY, Team VARCHAR(20), Score INT)");

        using var agg = ctx.Subscribe("SELECT Team, COUNT(*) AS Cnt FROM P GROUP BY Team", _ => { });
        using var top = ctx.Subscribe("SELECT Id, Score FROM P ORDER BY Score DESC LIMIT 3", _ => { });
        Assert.Throws<NotSupportedException>(() =>
            ctx.Subscribe("SELECT a.Id FROM P a JOIN P b ON a.Id=b.Id", _ => { }));
    }
}
