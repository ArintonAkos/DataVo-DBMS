using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Tests.E2E;

namespace DataVo.Tests.Reactive;

/// <summary>
/// Coverage for the typed (packed-cell) arrangements inside the reactive top-K and join operators: text sort
/// keys, composite primary keys, ties and OFFSET for top-K; mixed integer widths, NULL join keys, cross-side
/// WHERE and the hidden identity text for joins. Each randomized case maintains a live view from deltas and
/// compares it with a from-scratch query after every step.
/// </summary>
public class TypedArrangementTests
{
    [Theory]
    [InlineData(StorageMode.InMemory, 7)]
    [InlineData(StorageMode.Lsm, 8)]
    public void TopK_TextSortKey_CompositeKey_TiesAndOffset_EqualsRecompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using DataVoContext ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE Board (Region VARCHAR(8), Id INT, Name VARCHAR(16), Score INT, PRIMARY KEY (Region, Id))");

        // Name and Score repeat often, so ties are common; the full key breaks them deterministically.
        const string sql = "SELECT Region, Id, Name, Score FROM Board ORDER BY Name ASC, Score DESC, Region ASC, Id ASC LIMIT 5 OFFSET 2";
        var live = new Dictionary<(string, int), (string Name, int Score)>();
        using IDisposable sub = ctx.Subscribe(sql, change =>
        {
            foreach (var row in change.Removed)
            {
                live.Remove(((string)row["Region"]!, Convert.ToInt32(row["Id"])));
            }

            foreach (var row in change.Added.Concat(change.Updated))
            {
                live[((string)row["Region"]!, Convert.ToInt32(row["Id"]))] = ((string)row["Name"]!, Convert.ToInt32(row["Score"]));
            }
        });

        int peak = 0;
        string[] regions = ["eu", "us", "ap"];
        string[] names = ["ada", "bob", "cy", "dee"];
        for (int step = 0; step < 300; step++)
        {
            string region = regions[rng.Next(regions.Length)];
            int id = rng.Next(1, 6);
            string name = names[rng.Next(names.Length)];
            int score = rng.Next(0, 4) * 10;
            try
            {
                switch (rng.Next(3))
                {
                    case 0: ctx.Execute($"INSERT INTO Board VALUES ('{region}', {id}, '{name}', {score})"); break;
                    case 1: ctx.Execute($"UPDATE Board SET Name = '{name}', Score = {score} WHERE Region = '{region}' AND Id = {id}"); break;
                    default: ctx.Execute($"DELETE FROM Board WHERE Region = '{region}' AND Id = {id}"); break;
                }
            }
            catch (InvalidOperationException)
            {
                // A duplicate key on insert: the oracle is unaffected.
            }

            ctx.DispatchPendingNotifications();

            var expected = ctx.Execute(sql).Last().Data
                .Select(r => ((string)r["Region"]!, Convert.ToInt32(r["Id"]), (string)r["Name"]!, Convert.ToInt32(r["Score"])))
                .OrderBy(x => x).ToArray();
            var actual = live.Select(e => (e.Key.Item1, e.Key.Item2, e.Value.Name, e.Value.Score)).OrderBy(x => x).ToArray();
            Assert.Equal(expected, actual);
            peak = Math.Max(peak, actual.Length);
        }

        Assert.Equal(5, peak);
    }

    [Theory]
    [InlineData(StorageMode.InMemory, "INNER", 11)]
    [InlineData(StorageMode.InMemory, "LEFT", 12)]
    [InlineData(StorageMode.Lsm, "INNER", 13)]
    [InlineData(StorageMode.Lsm, "LEFT", 14)]
    public void Join_MixedKeyWidths_NullKeys_CrossSideWhere_EqualsReference(StorageMode mode, string kind, int seed)
    {
        var rng = new Random(seed);
        using DataVoContext ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, Cust BIGINT, Amount INT)");
        ctx.Execute("CREATE TABLE Custs (Cid INT PRIMARY KEY, Tier INT, Name VARCHAR(12))");

        // The join compares a BIGINT column with an INT key; the WHERE reads one column from each side.
        string sql = $"SELECT o.Id, o.Amount, c.Name FROM Orders o {kind} JOIN Custs c ON o.Cust = c.Cid WHERE o.Amount >= 20";
        var live = new Dictionary<string, (int Id, int Amount, string? Name)>();
        using IDisposable sub = ctx.Subscribe(sql, change =>
        {
            foreach (var row in change.Removed)
            {
                live.Remove(Identity(row));
            }

            foreach (var row in change.Added.Concat(change.Updated))
            {
                live[Identity(row)] = (Convert.ToInt32(row["o.Id"]), Convert.ToInt32(row["o.Amount"]), (string?)row["c.Name"]);
            }
        });

        int matchedPeak = 0;
        int paddedPeak = 0;
        for (int step = 0; step < 400; step++)
        {
            try
            {
                MutateJoinTables(ctx, rng);
            }
            catch (InvalidOperationException)
            {
                // A duplicate key on insert: the oracle is unaffected.
            }

            ctx.DispatchPendingNotifications();

            var expected = JoinReference(ctx, kind).OrderBy(x => x.Id).ThenBy(x => x.Name).ToArray();
            var actual = live.Values.OrderBy(x => x.Id).ThenBy(x => x.Name).ToArray();
            Assert.Equal(expected, actual);
            matchedPeak = Math.Max(matchedPeak, actual.Count(x => x.Name is not null));
            paddedPeak = Math.Max(paddedPeak, actual.Count(x => x.Name is null));
        }

        Assert.True(matchedPeak > 0, "the join never produced a matched row");
        Assert.True(kind != "LEFT" || paddedPeak > 0, "the LEFT join never produced a null-padded row");
    }

    [Fact]
    public void Join_HiddenIdentityColumns_KeepTheirTextFormat()
    {
        using DataVoContext ctx = ChangeCaptureIntegrationTests.NewContext(StorageMode.InMemory, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT)");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, Kind VARCHAR(10))");
        var added = new List<IReadOnlyDictionary<string, object?>>();
        using IDisposable sub = ctx.Subscribe("SELECT R.Id, S.Kind FROM R INNER JOIN S ON R.Gid = S.Id", change => added.AddRange(change.Added));

        ctx.Execute("INSERT INTO S VALUES (100, 'gold')");
        ctx.Execute("INSERT INTO R VALUES (7, 100)");
        ctx.DispatchPendingNotifications();

        IReadOnlyDictionary<string, object?> row = Assert.Single(added);
        Assert.Equal("v1:7", row["__rid"]);
        Assert.Equal("v3:100", row["__sid"]);
    }

    [Fact]
    public void Join_UpdateThatKeepsProjectedValues_EmitsNothing()
    {
        using DataVoContext ctx = ChangeCaptureIntegrationTests.NewContext(StorageMode.InMemory, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT, Note VARCHAR(10))");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, Kind VARCHAR(10))");
        ctx.Execute("INSERT INTO S VALUES (1, 'gold')");
        ctx.Execute("INSERT INTO R VALUES (1, 1, 'a')");
        int deliveries = 0;
        using IDisposable sub = ctx.SubscribeZeroAlloc(
            "SELECT R.Id, S.Kind FROM R INNER JOIN S ON R.Gid = S.Id",
            (in QueryChangeRef change) => deliveries += change.Added.Count + change.Updated.Count + change.Removed.Count);

        // A new image of the left row whose projected columns are unchanged must not produce a delta.
        ctx.Execute("UPDATE R SET Note = 'b' WHERE Id = 1");
        ctx.DispatchPendingNotifications();

        Assert.Equal(0, deliveries);
    }

    [Fact]
    public void TopK_BorrowedDispatch_OnUpdates_StaysAllocationLight()
    {
        using DataVoContext ctx = ChangeCaptureIntegrationTests.NewContext(StorageMode.InMemory, out _);
        ctx.Execute("CREATE TABLE P (Id INT PRIMARY KEY, Score INT)");
        for (int i = 1; i <= 200; i++)
        {
            ctx.Execute($"INSERT INTO P VALUES ({i}, {i})");
        }

        long sink = 0;
        using IDisposable sub = ctx.SubscribeZeroAlloc(
            "SELECT Id, Score FROM P ORDER BY Score DESC LIMIT 10",
            (in QueryChangeRef change) => sink += change.Added.Count + change.Updated.Count);
        DataVoPreparedStatement bump = ctx.Prepare("UPDATE P SET Score = Score + 1 WHERE Id = @id");

        for (int round = 0; round < 50; round++)
        {
            bump.Execute(("id", 1 + (round % 200)));
            ctx.DispatchPendingNotifications();
        }

        const int Rounds = 500;
        long dispatchBytes = 0;
        for (int round = 0; round < Rounds; round++)
        {
            bump.Execute(("id", 1 + (round % 200)));
            long before = GC.GetAllocatedBytesForCurrentThread();
            ctx.DispatchPendingNotifications();
            dispatchBytes += GC.GetAllocatedBytesForCurrentThread() - before;
        }

        Assert.True(sink >= 0);
        // Dictionary-shaped entries cost ~1 KB per updated row here; the typed arrangement measures ~216 B: the
        // sorted-set node and enumerator plus the dispatcher's own per-drain bookkeeping.
        long perDispatch = dispatchBytes / Rounds;
        Assert.True(perDispatch <= 400, $"top-K dispatch allocated {perDispatch} B per update");
    }

    private static void MutateJoinTables(DataVoContext ctx, Random rng)
    {
        bool orders = rng.Next(2) == 0;
        int op = rng.Next(3);
        if (orders)
        {
            int id = rng.Next(1, 10);
            // Roughly one in five orders has no customer (a NULL join key that never matches).
            string cust = rng.Next(5) == 0 ? "NULL" : rng.Next(1, 6).ToString();
            int amount = rng.Next(0, 5) * 10;
            if (op == 0) ctx.Execute($"INSERT INTO Orders VALUES ({id}, {cust}, {amount})");
            else if (op == 1) ctx.Execute($"UPDATE Orders SET Cust = {cust}, Amount = {amount} WHERE Id = {id}");
            else ctx.Execute($"DELETE FROM Orders WHERE Id = {id}");
            return;
        }

        int cid = rng.Next(1, 6);
        string name = "c" + rng.Next(0, 3);
        if (op == 0) ctx.Execute($"INSERT INTO Custs VALUES ({cid}, {rng.Next(0, 3)}, '{name}')");
        else if (op == 1) ctx.Execute($"UPDATE Custs SET Name = '{name}' WHERE Cid = {cid}");
        else ctx.Execute($"DELETE FROM Custs WHERE Cid = {cid}");
    }

    /// <summary>The INNER or LEFT join of Orders and Custs with the WHERE, computed from single-table reads.</summary>
    private static List<(int Id, int Amount, string? Name)> JoinReference(DataVoContext ctx, string kind)
    {
        var custs = ctx.Execute("SELECT Cid, Name FROM Custs").Last().Data
            .ToDictionary(r => Convert.ToInt64(r["Cid"]), r => (string?)r["Name"]);
        var result = new List<(int, int, string?)>();
        foreach (var order in ctx.Execute("SELECT Id, Cust, Amount FROM Orders").Last().Data)
        {
            int amount = Convert.ToInt32(order["Amount"]);
            if (amount < 20)
            {
                continue;
            }

            bool matched = order["Cust"] is not null && custs.ContainsKey(Convert.ToInt64(order["Cust"]));
            if (matched)
            {
                result.Add((Convert.ToInt32(order["Id"]), amount, custs[Convert.ToInt64(order["Cust"])]));
            }
            else if (kind == "LEFT")
            {
                result.Add((Convert.ToInt32(order["Id"]), amount, null));
            }
        }

        return result;
    }

    private static string Identity(IReadOnlyDictionary<string, object?> row) =>
        ((row["__rid"] as string) ?? "-") + "|" + ((row["__sid"] as string) ?? "-");
}
