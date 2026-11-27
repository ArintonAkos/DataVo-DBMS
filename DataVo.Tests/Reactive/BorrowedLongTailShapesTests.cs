using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Tests.E2E;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// The long-tail operators (linear, DISTINCT, UNION, IN/EXISTS) emit borrowed deltas, so every shape the owned
/// <c>Subscribe</c> accepts is also served by <c>SubscribeZeroAlloc</c>, with the same rows.
/// </summary>
public class BorrowedLongTailShapesTests
{
    private static DataVoContext NewContext()
    {
        DataVoContext ctx = ChangeCaptureIntegrationTests.NewContext(StorageMode.InMemory, out _);
        Run(ctx, "CREATE TABLE R (Id INT PRIMARY KEY, Gid INT, Name VARCHAR(20))");
        Run(ctx, "CREATE TABLE S (Id INT PRIMARY KEY, Gid INT)");
        return ctx;
    }

    private static void Run(DataVoContext ctx, string sql) => Assert.False(ctx.Execute(sql).Last().IsError, sql);

    /// <summary>Subscribes both ways and returns a function reading the net (added minus removed) rows of each.</summary>
    private static (List<string> Owned, List<string> Borrowed, IDisposable Subscriptions) Both(DataVoContext ctx, string sql, string column)
    {
        var owned = new List<string>();
        var borrowed = new List<string>();
        IDisposable a = ctx.Subscribe(sql, change =>
        {
            owned.AddRange(change.Added.Select(row => "+" + row[column]));
            owned.AddRange(change.Removed.Select(row => "-" + row[column]));
        });
        IDisposable b = ctx.SubscribeZeroAlloc(sql, (in QueryChangeRef change) =>
        {
            for (int i = 0; i < change.Added.Count; i++)
            {
                borrowed.Add("+" + change.Added[i][column].ToObject());
            }

            for (int i = 0; i < change.Removed.Count; i++)
            {
                borrowed.Add("-" + change.Removed[i][column].ToObject());
            }
        });

        return (owned, borrowed, new Pair(a, b));
    }

    private sealed class Pair(IDisposable first, IDisposable second) : IDisposable
    {
        public void Dispose()
        {
            first.Dispose();
            second.Dispose();
        }
    }

    [Fact]
    public void Distinct_EmitsOnlyBoundaryTransitions()
    {
        using DataVoContext ctx = NewContext();
        (List<string> owned, List<string> borrowed, IDisposable subs) = Both(ctx, "SELECT DISTINCT Gid FROM R", "Gid");
        using (subs)
        {
            Run(ctx, "INSERT INTO R VALUES (1, 7, 'a')");
            Run(ctx, "INSERT INTO R VALUES (2, 7, 'b')");
            ctx.DispatchPendingNotifications();
            Run(ctx, "DELETE FROM R WHERE Id = 1");
            ctx.DispatchPendingNotifications();
            Run(ctx, "DELETE FROM R WHERE Id = 2");
            ctx.DispatchPendingNotifications();
        }

        Assert.Equal(["+7", "-7"], owned);
        Assert.Equal(owned, borrowed);
    }

    [Fact]
    public void Union_FoldsBothBranchesIntoOneSet()
    {
        using DataVoContext ctx = NewContext();
        (List<string> owned, List<string> borrowed, IDisposable subs) = Both(ctx, "SELECT Gid FROM R UNION SELECT Gid FROM S", "Gid");
        using (subs)
        {
            Run(ctx, "INSERT INTO R VALUES (1, 5, 'a')");
            Run(ctx, "INSERT INTO S VALUES (1, 5)");
            ctx.DispatchPendingNotifications();
            Run(ctx, "DELETE FROM R WHERE Id = 1");
            ctx.DispatchPendingNotifications();
            Run(ctx, "UPDATE S SET Gid = 6 WHERE Id = 1");
            ctx.DispatchPendingNotifications();
        }

        Assert.Equal(["+5", "+6", "-5"], owned);
        Assert.Equal(owned, borrowed);
    }

    [Fact]
    public void InSubquery_TracksInnerMembership()
    {
        using DataVoContext ctx = NewContext();
        (List<string> owned, List<string> borrowed, IDisposable subs) = Both(ctx, "SELECT Id, Name FROM R WHERE Gid IN (SELECT Gid FROM S)", "Name");
        using (subs)
        {
            Run(ctx, "INSERT INTO R VALUES (1, 5, 'a')");
            ctx.DispatchPendingNotifications();
            Run(ctx, "INSERT INTO S VALUES (1, 5)");
            ctx.DispatchPendingNotifications();
            Run(ctx, "DELETE FROM S WHERE Id = 1");
            ctx.DispatchPendingNotifications();
        }

        Assert.Equal(["+a", "-a"], owned);
        Assert.Equal(owned, borrowed);
    }

    [Fact]
    public void CorrelatedNotExists_TracksTheCorrelationKey()
    {
        using DataVoContext ctx = NewContext();
        (List<string> owned, List<string> borrowed, IDisposable subs) = Both(
            ctx,
            "SELECT Id, Name FROM R WHERE NOT EXISTS (SELECT 1 FROM S WHERE S.Gid = R.Gid)",
            "Name");
        using (subs)
        {
            Run(ctx, "INSERT INTO R VALUES (1, 5, 'a')");
            Run(ctx, "INSERT INTO R VALUES (2, 6, 'b')");
            ctx.DispatchPendingNotifications();
            Run(ctx, "INSERT INTO S VALUES (1, 5)");
            ctx.DispatchPendingNotifications();
        }

        Assert.Equal(["+a", "+b", "-a"], owned.Take(2).OrderBy(x => x).Concat(owned.Skip(2)).ToArray());
        Assert.Equal(owned, borrowed);
    }
}
