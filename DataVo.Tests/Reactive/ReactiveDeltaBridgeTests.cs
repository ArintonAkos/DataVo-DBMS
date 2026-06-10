using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

public class ReactiveDeltaBridgeTests
{
    [Fact]
    public void Bridge_RoutesBuilderThroughRegistry_MaterializesOwnedQueryChange()
    {
        using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute("CREATE DATABASE BridgeDb");
        ctx.Execute("USE BridgeDb");
        ctx.Execute("CREATE TABLE Probe (Id INT, Stake INT)");

        QueryChange? received = null;
        var probe = new BridgeProbeReactiveQuery();
        using IDisposable sub = ctx.Engine.Reactive.AddCompiledForTest(ctx, probe, qc => received = qc);

        ctx.Execute("INSERT INTO Probe VALUES (7, 250)");
        ctx.DispatchPendingNotifications();

        Assert.NotNull(received);
        Assert.Single(received!.Added);
        IReadOnlyDictionary<string, object?> row = received.Added[0];
        Assert.Equal(7, Convert.ToInt32(row["Id"]));
        Assert.Equal(250, Convert.ToInt32(row["Stake"]));
    }
}
