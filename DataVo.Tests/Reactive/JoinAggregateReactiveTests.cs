using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Tests.E2E;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Coverage for the general N-way inner-join aggregate operator (<c>JoinAggregateReactiveQuery</c>), which replaced
/// the benchmark-specific VIP operator: dimension flips, deletes, bag semantics without primary keys, seeding,
/// a randomized differential against an in-test oracle, and the rejected shapes.
/// </summary>
public class JoinAggregateReactiveTests
{
    private const string VipSql =
        "SELECT m.Category, SUM(o.Stake) AS Total, COUNT(*) AS Cnt, MIN(o.Stake) AS Lo, MAX(o.Stake) AS Hi " +
        "FROM Orders o " +
        "JOIN Accounts a ON o.AccountId = a.Id " +
        "JOIN Markets m ON o.MarketId = m.Id " +
        "WHERE a.IsVip = true " +
        "GROUP BY m.Category";

    private sealed record Group(long Total, long Count, long Lo, long Hi);

    private static DataVoContext NewContext(StorageMode mode, bool primaryKeys)
    {
        DataVoContext ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        string pk = primaryKeys ? " PRIMARY KEY" : string.Empty;
        Execute(ctx, $"CREATE TABLE Accounts (Id INT{pk}, IsVip BIT)");
        Execute(ctx, $"CREATE TABLE Markets (Id INT{pk}, Category VARCHAR(20))");
        Execute(ctx, $"CREATE TABLE Orders (Id INT{pk}, AccountId INT, MarketId INT, Stake INT)");
        return ctx;
    }

    private static void Execute(DataVoContext ctx, string sql) =>
        Assert.False(ctx.Execute(sql).Last().IsError, sql);

    /// <summary>Applies owned deltas to a live view keyed by category.</summary>
    private static Action<QueryChange> Track(Dictionary<string, Group> live) => change =>
    {
        foreach (IReadOnlyDictionary<string, object?> row in change.Removed)
        {
            Assert.True(live.Remove((string)row["Category"]!), "removed a group that was never delivered");
        }

        foreach (IReadOnlyDictionary<string, object?> row in change.Added.Concat(change.Updated))
        {
            live[(string)row["Category"]!] = new Group(
                Convert.ToInt64(row["Total"]),
                Convert.ToInt64(row["Cnt"]),
                Convert.ToInt64(row["Lo"]),
                Convert.ToInt64(row["Hi"]));
        }
    };

    [Theory]
    [InlineData(StorageMode.InMemory, true)]
    [InlineData(StorageMode.InMemory, false)]
    [InlineData(StorageMode.Lsm, true)]
    public void VipExposure_FollowsDimensionFlipsCategoryMovesAndDeletes(StorageMode mode, bool primaryKeys)
    {
        using DataVoContext ctx = NewContext(mode, primaryKeys);
        var live = new Dictionary<string, Group>();
        using IDisposable sub = ctx.Subscribe(VipSql, Track(live));

        Execute(ctx, "INSERT INTO Accounts VALUES (1, true)");
        Execute(ctx, "INSERT INTO Accounts VALUES (2, false)");
        Execute(ctx, "INSERT INTO Markets VALUES (10, 'sports')");
        Execute(ctx, "INSERT INTO Markets VALUES (11, 'casino')");
        Execute(ctx, "INSERT INTO Orders VALUES (100, 1, 10, 25)");
        Execute(ctx, "INSERT INTO Orders VALUES (101, 2, 10, 99)");
        Execute(ctx, "INSERT INTO Orders VALUES (102, 1, 11, 10)");
        ctx.DispatchPendingNotifications();

        Assert.Equal(new Group(25, 1, 25, 25), live["sports"]);
        Assert.Equal(new Group(10, 1, 10, 10), live["casino"]);

        // Account 2 becomes VIP: its existing sports order joins the group.
        Execute(ctx, "UPDATE Accounts SET IsVip = true WHERE Id = 2");
        ctx.DispatchPendingNotifications();
        Assert.Equal(new Group(124, 2, 25, 99), live["sports"]);

        // Market 11 moves to sports: the casino group empties and is removed.
        Execute(ctx, "UPDATE Markets SET Category = 'sports' WHERE Id = 11");
        ctx.DispatchPendingNotifications();
        Assert.False(live.ContainsKey("casino"));
        Assert.Equal(new Group(134, 3, 10, 99), live["sports"]);

        // Deleting the largest order surfaces the next MAX.
        Execute(ctx, "DELETE FROM Orders WHERE Id = 101");
        ctx.DispatchPendingNotifications();
        Assert.Equal(new Group(35, 2, 10, 25), live["sports"]);

        // Deleting the only VIP account drops every group.
        Execute(ctx, "DELETE FROM Accounts WHERE Id = 1");
        Execute(ctx, "DELETE FROM Accounts WHERE Id = 2");
        ctx.DispatchPendingNotifications();
        Assert.Empty(live);
    }

    [Fact]
    public void WithoutPrimaryKeys_DuplicateRowsCountAsABag()
    {
        using DataVoContext ctx = NewContext(StorageMode.InMemory, primaryKeys: false);
        Execute(ctx, "INSERT INTO Accounts VALUES (1, true)");
        Execute(ctx, "INSERT INTO Markets VALUES (10, 'sports')");
        Execute(ctx, "INSERT INTO Markets VALUES (10, 'sports')"); // duplicate dimension row doubles each order
        var live = new Dictionary<string, Group>();
        using IDisposable sub = ctx.Subscribe(VipSql, Track(live));

        Execute(ctx, "INSERT INTO Orders VALUES (1, 1, 10, 5)");
        Execute(ctx, "INSERT INTO Orders VALUES (1, 1, 10, 5)");
        ctx.DispatchPendingNotifications();
        Assert.Equal(new Group(20, 4, 5, 5), live["sports"]);

        // The delete removes both identical order rows.
        Execute(ctx, "DELETE FROM Orders WHERE Id = 1");
        ctx.DispatchPendingNotifications();
        Assert.Empty(live);
    }

    [Fact]
    public void SeededBaseline_IsNotDelivered_AndLaterChangesBuildOnIt()
    {
        using DataVoContext ctx = NewContext(StorageMode.InMemory, primaryKeys: true);
        Execute(ctx, "INSERT INTO Accounts VALUES (1, true)");
        Execute(ctx, "INSERT INTO Markets VALUES (10, 'sports')");
        Execute(ctx, "INSERT INTO Orders VALUES (1, 1, 10, 7)");
        Execute(ctx, "INSERT INTO Orders VALUES (2, 1, 10, 3)");

        var deliveries = new List<QueryChange>();
        using IDisposable sub = ctx.Subscribe(VipSql, deliveries.Add);
        ctx.DispatchPendingNotifications();
        Assert.Empty(deliveries);

        Execute(ctx, "INSERT INTO Orders VALUES (3, 1, 10, 10)");
        ctx.DispatchPendingNotifications();

        IReadOnlyDictionary<string, object?> row = Assert.Single(Assert.Single(deliveries).Added);
        Assert.Equal(20L, Convert.ToInt64(row["Total"]));
        Assert.Equal(3L, Convert.ToInt64(row["Cnt"]));
        Assert.Equal(3, Convert.ToInt32(row["Lo"]));
        Assert.Equal(10, Convert.ToInt32(row["Hi"]));
    }

    [Fact]
    public void ZeroAllocSubscriber_ReceivesTheSameDeltas()
    {
        using DataVoContext ctx = NewContext(StorageMode.InMemory, primaryKeys: false);
        Execute(ctx, "INSERT INTO Accounts VALUES (1, true)");
        Execute(ctx, "INSERT INTO Markets VALUES (10, 'sports')");
        long total = 0;
        using IDisposable sub = ctx.SubscribeZeroAlloc(VipSql, (in QueryChangeRef change) =>
        {
            for (int i = 0; i < change.Added.Count; i++)
            {
                total = change.Added[i]["Total"].AsInt64();
            }

            for (int i = 0; i < change.Updated.Count; i++)
            {
                total = change.Updated[i]["Total"].AsInt64();
            }
        });

        Execute(ctx, "INSERT INTO Orders VALUES (1, 1, 10, 40)");
        Execute(ctx, "INSERT INTO Orders VALUES (2, 1, 10, 2)");
        ctx.DispatchPendingNotifications();

        Assert.Equal(42, total);
    }

    [Theory]
    [InlineData(StorageMode.InMemory, 11)]
    [InlineData(StorageMode.InMemory, 29)]
    [InlineData(StorageMode.Lsm, 47)]
    public void RandomizedWorkload_MatchesOracleAfterEveryBatch(StorageMode mode, int seed)
    {
        using DataVoContext ctx = NewContext(mode, primaryKeys: false);
        var random = new Random(seed);
        var accounts = new Dictionary<int, bool>();
        var markets = new Dictionary<int, string>();
        var orders = new List<(int Id, int Account, int Market, int Stake)>();
        string[] categories = ["sports", "casino", "poker"];
        var live = new Dictionary<string, Group>();
        using IDisposable sub = ctx.Subscribe(VipSql, Track(live));

        int nextOrder = 1;
        for (int batch = 0; batch < 40; batch++)
        {
            for (int op = 0; op < 6; op++)
            {
                switch (random.Next(7))
                {
                    case 0:
                    {
                        int id = random.Next(1, 6);
                        bool vip = random.Next(2) == 0;
                        if (accounts.TryAdd(id, vip))
                        {
                            Execute(ctx, $"INSERT INTO Accounts VALUES ({id}, {(vip ? "true" : "false")})");
                        }
                        else
                        {
                            accounts[id] = vip;
                            Execute(ctx, $"UPDATE Accounts SET IsVip = {(vip ? "true" : "false")} WHERE Id = {id}");
                        }

                        break;
                    }

                    case 1:
                    {
                        int id = random.Next(1, 5);
                        string category = categories[random.Next(categories.Length)];
                        if (markets.TryAdd(id, category))
                        {
                            Execute(ctx, $"INSERT INTO Markets VALUES ({id}, '{category}')");
                        }
                        else
                        {
                            markets[id] = category;
                            Execute(ctx, $"UPDATE Markets SET Category = '{category}' WHERE Id = {id}");
                        }

                        break;
                    }

                    case 2:
                    case 3:
                    {
                        var order = (nextOrder++, random.Next(1, 7), random.Next(1, 6), random.Next(1, 50));
                        orders.Add(order);
                        Execute(ctx, $"INSERT INTO Orders VALUES ({order.Item1}, {order.Item2}, {order.Item3}, {order.Item4})");
                        break;
                    }

                    case 4 when orders.Count > 0:
                    {
                        int index = random.Next(orders.Count);
                        int stake = random.Next(1, 50);
                        orders[index] = orders[index] with { Stake = stake };
                        Execute(ctx, $"UPDATE Orders SET Stake = {stake} WHERE Id = {orders[index].Id}");
                        break;
                    }

                    case 5 when orders.Count > 0:
                    {
                        int index = random.Next(orders.Count);
                        Execute(ctx, $"DELETE FROM Orders WHERE Id = {orders[index].Id}");
                        orders.RemoveAt(index);
                        break;
                    }

                    case 6 when accounts.Count > 0:
                    {
                        int id = accounts.Keys.ElementAt(random.Next(accounts.Count));
                        accounts.Remove(id);
                        Execute(ctx, $"DELETE FROM Accounts WHERE Id = {id}");
                        break;
                    }
                }
            }

            ctx.DispatchPendingNotifications();
            Assert.Equal(Oracle(accounts, markets, orders), live.OrderBy(pair => pair.Key).ToArray());
        }
    }

    private static KeyValuePair<string, Group>[] Oracle(
        Dictionary<int, bool> accounts,
        Dictionary<int, string> markets,
        List<(int Id, int Account, int Market, int Stake)> orders) =>
        orders
            .Where(order => accounts.TryGetValue(order.Account, out bool vip) && vip && markets.ContainsKey(order.Market))
            .GroupBy(order => markets[order.Market])
            .Select(group => new KeyValuePair<string, Group>(
                group.Key,
                new Group(group.Sum(o => (long)o.Stake), group.LongCount(), group.Min(o => o.Stake), group.Max(o => o.Stake))))
            .OrderBy(pair => pair.Key)
            .ToArray();

    [Theory]
    [InlineData("SELECT a.Id, COUNT(*) AS Cnt FROM Accounts a LEFT JOIN Orders o ON o.AccountId = a.Id GROUP BY a.Id")]
    [InlineData("SELECT o.Id, COUNT(*) AS Cnt FROM Orders o JOIN Orders p ON o.Id = p.Id GROUP BY o.Id")]
    [InlineData("SELECT Id, COUNT(*) AS Cnt FROM Orders o JOIN Accounts a ON o.AccountId = a.Id GROUP BY Id")]
    [InlineData("SELECT m.Category, SUM(o.Stake) AS T FROM Orders o JOIN Markets m ON o.MarketId = m.Id GROUP BY m.Category HAVING SUM(o.Stake) > 1")]
    public void UnsupportedJoinAggregateShapes_AreRejected(string sql)
    {
        using DataVoContext ctx = NewContext(StorageMode.InMemory, primaryKeys: true);
        Assert.Throws<NotSupportedException>(() => ctx.Subscribe(sql, _ => { }));
    }
}
