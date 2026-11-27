using DataVo.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Verifies prepared statement reactive subscriptions (<c>Subscribe</c> and <c>SubscribeZeroAlloc</c>)
/// including AST isolation across concurrent parameter bindings, validation before hook registration,
/// and all supported parameter overloads.
/// </summary>
public sealed class PreparedStatementReactiveTests
{
    private static DataVoContext CreateContext(StorageMode mode = StorageMode.InMemory)
    {
        var ctx = new DataVoContext(new DataVoConfig { StorageMode = mode });
        ctx.Execute("CREATE DATABASE PrepReactiveDb");
        ctx.Execute("USE PrepReactiveDb");
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Team VARCHAR(32), Score INT)");
        return ctx;
    }

    [Fact]
    public void Subscribe_PreparedStatement_AstIsolationAcrossConcurrentParameterBindings()
    {
        using DataVoContext ctx = CreateContext();
        DataVoPreparedStatement byTeam = ctx.Prepare(
            "SELECT Id, Name, Score FROM Players WHERE Team = @team AND Score >= @minScore");

        var redAdded = new List<(int Id, string Name, int Score)>();
        var blueAdded = new List<(int Id, string Name, int Score)>();

        using IDisposable subRed = ctx.Subscribe(
            byTeam,
            change =>
            {
                foreach (IReadOnlyDictionary<string, object?> row in change.Added)
                {
                    redAdded.Add((Convert.ToInt32(row["Id"]), (string)row["Name"]!, Convert.ToInt32(row["Score"])));
                }
            },
            ("team", "Red"),
            ("minScore", 100));

        using IDisposable subBlue = ctx.Subscribe(
            byTeam,
            change =>
            {
                foreach (IReadOnlyDictionary<string, object?> row in change.Added)
                {
                    blueAdded.Add((Convert.ToInt32(row["Id"]), (string)row["Name"]!, Convert.ToInt32(row["Score"])));
                }
            },
            new Dictionary<string, object?> { ["team"] = "Blue", ["minScore"] = 200 });

        ctx.Execute("INSERT INTO Players VALUES (1, 'Ada', 'Red', 120)");
        ctx.Execute("INSERT INTO Players VALUES (2, 'Bob', 'Red', 50)");
        ctx.Execute("INSERT INTO Players VALUES (3, 'Lin', 'Blue', 180)");
        ctx.Execute("INSERT INTO Players VALUES (4, 'Zed', 'Blue', 250)");
        ctx.Execute("INSERT INTO Players VALUES (5, 'Kai', 'Green', 500)");
        ctx.DispatchPendingNotifications();

        Assert.Equal([(1, "Ada", 120)], redAdded);
        Assert.Equal([(4, "Zed", 250)], blueAdded);

        // Template AST inside the prepared statement remains intact for normal execution with new parameters.
        var greenResult = byTeam.Execute(("team", "Green"), ("minScore", 400));
        Assert.False(greenResult.IsError);
        Assert.Single(greenResult.Data);
        Assert.Equal("Kai", greenResult.Data[0]["Name"]);
    }

    [Fact]
    public void Subscribe_PreparedStatement_ParameterlessAndSpanOverloads()
    {
        using DataVoContext ctx = CreateContext();
        DataVoPreparedStatement allPlayers = ctx.Prepare("SELECT Id, Score FROM Players");
        DataVoPreparedStatement minScore = ctx.Prepare("SELECT Id, Score FROM Players WHERE Score >= @min");

        int allCount = 0;
        int highCount = 0;

        using IDisposable subAll = ctx.Subscribe(allPlayers, change => allCount += change.Added.Count);
        ReadOnlySpan<DataVoQueryParameter> spanParams = [new DataVoQueryParameter("min", 150)];
        using IDisposable subHigh = ctx.Subscribe(minScore, change => highCount += change.Added.Count, spanParams);

        ctx.Execute("INSERT INTO Players VALUES (1, 'A', 'Red', 100)");
        ctx.Execute("INSERT INTO Players VALUES (2, 'B', 'Red', 200)");
        ctx.DispatchPendingNotifications();

        Assert.Equal(2, allCount);
        Assert.Equal(1, highCount);
    }

    [Fact]
    public void SubscribeZeroAlloc_PreparedStatement_TopKAndAggregate_IsolatedAcrossBindings()
    {
        using DataVoContext ctx = CreateContext();
        DataVoPreparedStatement topByTeam = ctx.Prepare(
            "SELECT Id, Score FROM Players WHERE Team = @team ORDER BY Score DESC LIMIT @k");
        DataVoPreparedStatement teamTotal = ctx.Prepare(
            "SELECT Team, SUM(Score) AS Total FROM Players WHERE Score >= @min GROUP BY Team");

        var redTop = new List<int>();
        var blueTop = new List<int>();
        long totalSum = 0;

        using IDisposable subRed = ctx.SubscribeZeroAlloc(
            topByTeam,
            (in QueryChangeRef change) =>
            {
                for (int i = 0; i < change.Added.Count; i++)
                {
                    redTop.Add(change.Added[i]["Id"].AsInt32());
                }

                for (int i = 0; i < change.Removed.Count; i++)
                {
                    redTop.Remove(change.Removed[i]["Id"].AsInt32());
                }
            },
            ("team", "Red"),
            ("k", 2));

        using IDisposable subBlue = ctx.SubscribeZeroAlloc(
            topByTeam,
            (in QueryChangeRef change) =>
            {
                for (int i = 0; i < change.Added.Count; i++)
                {
                    blueTop.Add(change.Added[i]["Id"].AsInt32());
                }
            },
            new Dictionary<string, object?> { ["team"] = "Blue", ["k"] = 1 });

        using IDisposable subAgg = ctx.SubscribeZeroAlloc(
            teamTotal,
            (in QueryChangeRef change) =>
            {
                for (int i = 0; i < change.Added.Count; i++)
                {
                    totalSum += change.Added[i]["Total"].AsInt64();
                }

                for (int i = 0; i < change.Updated.Count; i++)
                {
                    totalSum = change.Updated[i]["Total"].AsInt64();
                }
            },
            ("min", 50));

        ctx.Execute("INSERT INTO Players VALUES (1, 'A', 'Red', 100)");
        ctx.Execute("INSERT INTO Players VALUES (2, 'B', 'Red', 300)");
        ctx.Execute("INSERT INTO Players VALUES (3, 'C', 'Red', 200)");
        ctx.Execute("INSERT INTO Players VALUES (4, 'D', 'Blue', 150)");
        ctx.Execute("INSERT INTO Players VALUES (5, 'E', 'Blue', 400)");
        ctx.DispatchPendingNotifications();

        Assert.Equal([2, 3], redTop.OrderBy(static id => id).ToArray());
        Assert.Equal([4, 5], blueTop.OrderBy(static id => id).ToArray()); // 4 entered first, then 5 displaced it
        Assert.True(totalSum > 0);
    }

    [Fact]
    public void Subscribe_PreparedStatement_QuotedBoundStringDoesNotTrimQuotes()
    {
        using DataVoContext ctx = CreateContext();
        DataVoPreparedStatement byName = ctx.Prepare("SELECT Id, Name FROM Players WHERE Name = @name");

        var matched = new List<string>();
        using IDisposable sub = ctx.Subscribe(
            byName,
            change =>
            {
                foreach (IReadOnlyDictionary<string, object?> row in change.Added)
                {
                    matched.Add((string)row["Name"]!);
                }
            },
            ("name", "'Literal'"));

        ctx.Execute("INSERT INTO Players VALUES (1, 'Literal', 'Red', 10)");
        ctx.Execute("INSERT INTO Players VALUES (2, '''Literal''', 'Red', 20)");
        ctx.DispatchPendingNotifications();

        Assert.Equal(["'Literal'"], matched);
    }

    [Fact]
    public void SubscribeZeroAlloc_PreparedLinearFilter_DeliversBorrowedRows()
    {
        using DataVoContext ctx = CreateContext();
        DataVoPreparedStatement linearStmt = ctx.Prepare("SELECT Id, Score FROM Players WHERE Score > @min");
        var added = new List<(int Id, int Score)>();
        var removed = new List<int>();
        using IDisposable sub = ctx.SubscribeZeroAlloc(linearStmt, (in QueryChangeRef change) =>
        {
            for (int i = 0; i < change.Added.Count; i++)
            {
                added.Add((change.Added[i]["Id"].AsInt32(), change.Added[i]["Score"].AsInt32()));
            }

            for (int i = 0; i < change.Removed.Count; i++)
            {
                removed.Add(change.Removed[i]["Id"].AsInt32());
            }
        }, ("min", 10));

        ctx.Execute("INSERT INTO Players VALUES (1, 'A', 'Red', 5)");
        ctx.Execute("INSERT INTO Players VALUES (2, 'B', 'Red', 50)");
        ctx.Execute("UPDATE Players SET Score = 1 WHERE Id = 2");
        ctx.DispatchPendingNotifications();

        Assert.Equal([(2, 50)], added);
        Assert.Equal([2], removed);
    }

    [Fact]
    public void Subscribe_PreparedStatement_InvalidStatementsAndCap_FailCleanlyWithoutSideEffects()
    {
        using DataVoContext ctx = CreateContext();
        Assert.False(ctx.Changes.Enabled);

        DataVoPreparedStatement insertStmt = ctx.Prepare("INSERT INTO Players VALUES (1, 'A', 'Red', 10)");
        DataVoPreparedStatement multiStmt = ctx.Prepare("SELECT Id FROM Players; SELECT Id FROM Players;");
        DataVoPreparedStatement badSql = ctx.Prepare("SELECT FROM WHERE ???");
        DataVoPreparedStatement missingParam = ctx.Prepare("SELECT Id FROM Players WHERE Team = @team");
        DataVoPreparedStatement linearStmt = ctx.Prepare("SELECT Id FROM Players WHERE Score > @min");
        DataVoPreparedStatement distinctAgg = ctx.Prepare("SELECT COUNT(DISTINCT Team) FROM Players");

        Assert.Throws<NotSupportedException>(() => ctx.Subscribe(insertStmt, _ => { }));
        Assert.Throws<NotSupportedException>(() => ctx.Subscribe(multiStmt, _ => { }));
        Assert.Throws<NotSupportedException>(() => ctx.Subscribe(badSql, _ => { }));
        Assert.Throws<BindingException>(() => ctx.Subscribe(missingParam, _ => { }));
        Assert.Throws<NotSupportedException>(() => ctx.Subscribe(distinctAgg, _ => { }));
        Assert.Throws<NotSupportedException>(() => ctx.SubscribeZeroAlloc(distinctAgg, (in QueryChangeRef _) => { }));

        // None of the rejected attempts should have enabled change capture.
        Assert.False(ctx.Changes.Enabled);

        // Subscription cap enforced before attaching change hook.
        ctx.SetMaxReactiveSubscriptions(0);
        Assert.Throws<InvalidOperationException>(() => ctx.Subscribe(linearStmt, _ => { }, ("min", 10)));
        Assert.False(ctx.Changes.Enabled);

        // Cross-context prepared statement rejected cleanly.
        using DataVoContext other = CreateContext();
        DataVoPreparedStatement foreignStmt = other.Prepare("SELECT Id FROM Players");
        ctx.SetMaxReactiveSubscriptions(4);
        Assert.Throws<ArgumentException>(() => ctx.Subscribe(foreignStmt, _ => { }));
        Assert.False(ctx.Changes.Enabled);
    }

    [Fact]
    public void Subscribe_PreparedStatement_RollbackEmitsNothing()
    {
        using DataVoContext ctx = CreateContext();
        DataVoPreparedStatement linearStmt = ctx.Prepare("SELECT Id, Score FROM Players WHERE Team = @team");
        DataVoPreparedStatement topKStmt = ctx.Prepare("SELECT Id, Score FROM Players WHERE Team = @team ORDER BY Score DESC LIMIT 3");

        int ownedCalls = 0;
        int borrowedCalls = 0;
        using IDisposable subOwned = ctx.Subscribe(linearStmt, _ => ownedCalls++, ("team", "Red"));
        using IDisposable subBorrowed = ctx.SubscribeZeroAlloc(topKStmt, (in QueryChangeRef _) => borrowedCalls++, ("team", "Red"));

        ctx.Execute("BEGIN");
        ctx.Execute("INSERT INTO Players VALUES (1, 'Ghost', 'Red', 999)");
        ctx.Execute("ROLLBACK");
        ctx.DispatchPendingNotifications();

        Assert.Equal(0, ownedCalls);
        Assert.Equal(0, borrowedCalls);
    }
}
