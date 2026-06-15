using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// GC Reduction Slice 4, P2.2: reactive subscription seeding should consume typed storage reads and
/// avoid pre-materializing one dictionary per stored row before seeding the operator.
/// </summary>
public class ReactiveSeedTypedReadTests
{
    private const string Sql = """
        SELECT Market, SUM(Stake) AS Total
        FROM Orders
        GROUP BY Market
        """;

    [Fact]
    public void SubscribeZeroAlloc_Seed_StaysBelowDictionaryMaterializationBudget()
    {
        // Warm parser/JIT on an equivalent shape so the measured region is dominated by row seeding.
        using (DataVoContext warm = NewContext("SeedWarm"))
        {
            SeedOrders(warm, rowCount: 10);
            using IDisposable _ = warm.SubscribeZeroAlloc(Sql, (in QueryChangeRef _) => { });
        }

        using DataVoContext ctx = NewContext("SeedMeasured");
        SeedOrders(ctx, rowCount: 1_000);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        using IDisposable sub = ctx.SubscribeZeroAlloc(Sql, (in QueryChangeRef _) => { });
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < 680_000, $"seed allocated {allocated:N0} bytes");
    }

    private static DataVoContext NewContext(string databaseName)
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ctx.Execute($"CREATE DATABASE {databaseName}");
        ctx.Execute($"USE {databaseName}");
        ctx.Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, Market VARCHAR(20), Stake INT)");
        return ctx;
    }

    private static void SeedOrders(DataVoContext ctx, int rowCount)
    {
        var rows = new List<IReadOnlyDictionary<string, object?>>(rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            rows.Add(new Dictionary<string, object?>
            {
                ["Id"] = i + 1,
                ["Market"] = "sports",
                ["Stake"] = 10,
            });
        }

        ctx.BulkInsert("Orders", rows);
    }
}
