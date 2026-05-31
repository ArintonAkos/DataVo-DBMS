using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Tests.E2E;

namespace DataVo.Tests.Property;

public class ReactiveIvmOracleTests
{
    [Theory]
    [InlineData(StorageMode.InMemory, 1)]
    [InlineData(StorageMode.InMemory, 2)]
    [InlineData(StorageMode.Disk, 3)]
    public void Incremental_Equals_Recompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE T (Id INT PRIMARY KEY, V INT)");

        // Maintain incremental view via subscription.
        var live = new Dictionary<int, IReadOnlyDictionary<string, object?>>();
        using var sub = ctx.Subscribe("SELECT Id, V FROM T WHERE V < 50", qc =>
        {
            foreach (var r in qc.Added) live[(int)r["Id"]!] = r;
            foreach (var r in qc.Updated) live[(int)r["Id"]!] = r;
            foreach (var r in qc.Removed) live.Remove((int)r["Id"]!);
        });

        for (int i = 0; i < 300; i++)
        {
            int id = rng.Next(1, 30);
            int op = rng.Next(3);
            if (op == 0) ctx.Execute($"INSERT INTO T VALUES ({id}, {rng.Next(0, 100)})"); // may no-op on PK clash
            else if (op == 1) ctx.Execute($"UPDATE T SET V = {rng.Next(0, 100)} WHERE Id = {id}");
            else ctx.Execute($"DELETE FROM T WHERE Id = {id}");
            ctx.DispatchPendingNotifications();
        }

        // Oracle: full recompute.
        var expected = ctx.Execute("SELECT Id, V FROM T WHERE V < 50").Single().Data
            .ToDictionary(r => (int)r["Id"]!, r => (int)r["V"]!);

        Assert.Equal(
            expected.OrderBy(k => k.Key).ToArray(),
            live.OrderBy(k => k.Key).Select(k => new KeyValuePair<int, int>(k.Key, (int)k.Value["V"]!)).ToArray());
    }
}
