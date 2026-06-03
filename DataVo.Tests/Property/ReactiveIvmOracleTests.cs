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

    [Theory]
    [InlineData(StorageMode.InMemory, 11)]
    [InlineData(StorageMode.Disk, 12)]
    public void Aggregate_Incremental_Equals_Recompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE T (Id INT PRIMARY KEY, G INT, V INT)");

        var live = new Dictionary<object, (long Cnt, long Sum, long Max)>();
        using var sub = ctx.Subscribe(
            "SELECT G, COUNT(*) AS C, SUM(V) AS S, MAX(V) AS M FROM T GROUP BY G", qc =>
        {
            foreach (var r in qc.Added.Concat(qc.Updated))
                live[r["G"]!] = (Convert.ToInt64(r["C"]), Convert.ToInt64(r["S"]), Convert.ToInt64(r["M"]));
            foreach (var r in qc.Removed) live.Remove(r["G"]!);
        });

        for (int i = 0; i < 400; i++)
        {
            int id = rng.Next(1, 25), g = rng.Next(0, 4), v = rng.Next(0, 100), op = rng.Next(3);
            try
            {
                if (op == 0) ctx.Execute($"INSERT INTO T VALUES ({id}, {g}, {v})");
                else if (op == 1) ctx.Execute($"UPDATE T SET G = {g}, V = {v} WHERE Id = {id}");
                else ctx.Execute($"DELETE FROM T WHERE Id = {id}");
            }
            catch { /* PK clash on insert: skip; oracle unaffected */ }
            ctx.DispatchPendingNotifications();
        }

        // COUNT and SUM are recomputed via the batch aggregate engine. The batch engine's MIN/MAX path
        // rejects every column type (it validates a column as numeric AND string in the same pass), so
        // MAX(V) cannot be recomputed there; the MAX oracle is computed independently from the raw rows.
        // This keeps the incremental==recompute gate honest for all three aggregates without depending
        // on the engine's broken batch MIN/MAX, which is out of L2 scope to fix.
        var countSum = ctx.Execute("SELECT G, COUNT(*) AS C, SUM(V) AS S FROM T GROUP BY G")
            .Single().Data.ToDictionary(r => Convert.ToInt32(r["G"]!), r => (Convert.ToInt64(r["C"]), Convert.ToInt64(r["S"])));

        var maxByGroup = ctx.Execute("SELECT G, V FROM T").Single().Data
            .GroupBy(r => Convert.ToInt32(r["G"]!))
            .ToDictionary(g => g.Key, g => g.Max(r => Convert.ToInt64(r["V"]!)));

        var expected = countSum.ToDictionary(
            kv => kv.Key,
            kv => (Cnt: kv.Value.Item1, Sum: kv.Value.Item2, Max: maxByGroup[kv.Key]));

        Assert.Equal(
            expected.OrderBy(k => k.Key).ToArray(),
            live.OrderBy(k => Convert.ToInt32(k.Key))
                .Select(k => new KeyValuePair<int, (long Cnt, long Sum, long Max)>(Convert.ToInt32(k.Key), k.Value))
                .ToArray());
    }

    [Theory]
    [InlineData(StorageMode.InMemory, 21)]
    [InlineData(StorageMode.Disk, 22)]
    public void TopK_Incremental_Equals_Recompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE T (Id INT PRIMARY KEY, V INT)");

        const int k = 3;
        var window = new HashSet<int>();
        using var sub = ctx.Subscribe($"SELECT Id, V FROM T WHERE V < 80 ORDER BY V DESC, Id ASC LIMIT {k}", qc =>
        {
            foreach (var r in qc.Added) window.Add(Convert.ToInt32(r["Id"]));
            foreach (var r in qc.Removed) window.Remove(Convert.ToInt32(r["Id"]));
        });

        for (int i = 0; i < 400; i++)
        {
            int id = rng.Next(1, 25), v = rng.Next(0, 100), op = rng.Next(3);
            try
            {
                if (op == 0) ctx.Execute($"INSERT INTO T VALUES ({id}, {v})");
                else if (op == 1) ctx.Execute($"UPDATE T SET V = {v} WHERE Id = {id}");
                else ctx.Execute($"DELETE FROM T WHERE Id = {id}");
            }
            catch { /* PK clash on insert: skip; oracle unaffected */ }
            ctx.DispatchPendingNotifications();

            var expected = ctx.Execute($"SELECT Id, V FROM T WHERE V < 80 ORDER BY V DESC, Id ASC LIMIT {k}")
                .Single().Data.Select(r => Convert.ToInt32(r["Id"])).OrderBy(x => x).ToArray();

            Assert.Equal(expected, window.OrderBy(x => x).ToArray());
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory, 31)]
    [InlineData(StorageMode.InMemory, 32)]
    [InlineData(StorageMode.Disk, 33)]
    [InlineData(StorageMode.Disk, 34)]
    public void InnerJoin_Incremental_Equals_Recompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT, Tag INT)");
        ctx.Execute("CREATE TABLE S (Sid INT PRIMARY KEY, Kind INT)");

        const string sql = "SELECT R.Id, R.Tag, S.Kind FROM R INNER JOIN S ON R.Gid = S.Sid";

        // Maintain the incremental join keyed by the hidden per-side identity columns.
        var live = new Dictionary<string, (int rid, int tag, int kind)>();
        using var sub = ctx.Subscribe(sql, qc =>
        {
            foreach (var r in qc.Added.Concat(qc.Updated))
            {
                string key = (string)r["__rid"]! + "|" + (string)r["__sid"]!;
                live[key] = (Convert.ToInt32(r["R.Id"]), Convert.ToInt32(r["R.Tag"]), Convert.ToInt32(r["S.Kind"]));
            }

            foreach (var r in qc.Removed)
            {
                live.Remove((string)r["__rid"]! + "|" + (string)r["__sid"]!);
            }
        });

        for (int i = 0; i < 500; i++)
        {
            int table = rng.Next(2);
            int op = rng.Next(3);
            try
            {
                if (table == 0)
                {
                    int id = rng.Next(1, 12);
                    if (op == 0) ctx.Execute($"INSERT INTO R VALUES ({id}, {rng.Next(1, 8)}, {rng.Next(0, 100)})");
                    else if (op == 1) ctx.Execute($"UPDATE R SET Gid = {rng.Next(1, 8)}, Tag = {rng.Next(0, 100)} WHERE Id = {id}");
                    else ctx.Execute($"DELETE FROM R WHERE Id = {id}");
                }
                else
                {
                    int sid = rng.Next(1, 8);
                    if (op == 0) ctx.Execute($"INSERT INTO S VALUES ({sid}, {rng.Next(0, 100)})");
                    else if (op == 1) ctx.Execute($"UPDATE S SET Kind = {rng.Next(0, 100)} WHERE Sid = {sid}");
                    else ctx.Execute($"DELETE FROM S WHERE Sid = {sid}");
                }
            }
            catch { /* PK clash on insert: skip; oracle unaffected */ }

            ctx.DispatchPendingNotifications();

            var expected = ctx.Execute(sql).Single().Data
                .Select(d => (Convert.ToInt32(d["R.Id"]), Convert.ToInt32(d["R.Tag"]), Convert.ToInt32(d["S.Kind"])))
                .OrderBy(x => x).ToArray();

            var actual = live.Values
                .Select(v => (v.rid, v.tag, v.kind))
                .OrderBy(x => x).ToArray();

            Assert.Equal(expected, actual);
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory, "LEFT", 41)]
    [InlineData(StorageMode.InMemory, "FULL", 42)]
    [InlineData(StorageMode.Disk, "LEFT", 43)]
    [InlineData(StorageMode.Disk, "FULL", 44)]
    public void OuterJoin_Incremental_Equals_Recompute(StorageMode mode, string kind, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT, Tag INT)");
        ctx.Execute("CREATE TABLE S (Sid INT PRIMARY KEY, Kind INT)");

        string sql = $"SELECT R.Id, R.Tag, S.Kind FROM R {kind} JOIN S ON R.Gid = S.Sid";

        // Live view keyed by output identity (rid|sid with "∅" sentinels for null-padded sides).
        var live = new Dictionary<string, (int? rid, int? tag, int? kind)>();
        using var sub = ctx.Subscribe(sql, qc =>
        {
            foreach (var r in qc.Added.Concat(qc.Updated))
            {
                live[Key(r)] = (
                    r["R.Id"] is null ? null : Convert.ToInt32(r["R.Id"]),
                    r["R.Tag"] is null ? null : Convert.ToInt32(r["R.Tag"]),
                    r["S.Kind"] is null ? null : Convert.ToInt32(r["S.Kind"]));
            }

            foreach (var r in qc.Removed)
            {
                live.Remove(Key(r));
            }
        });

        static string Key(IReadOnlyDictionary<string, object?> r) =>
            ((r["__rid"] as string) ?? "∅") + "|" + ((r["__sid"] as string) ?? "∅");

        for (int i = 0; i < 600; i++)
        {
            int table = rng.Next(2);
            int op = rng.Next(3);
            try
            {
                if (table == 0)
                {
                    int id = rng.Next(1, 12);
                    if (op == 0) ctx.Execute($"INSERT INTO R VALUES ({id}, {rng.Next(1, 8)}, {rng.Next(0, 100)})");
                    else if (op == 1) ctx.Execute($"UPDATE R SET Gid = {rng.Next(1, 8)}, Tag = {rng.Next(0, 100)} WHERE Id = {id}");
                    else ctx.Execute($"DELETE FROM R WHERE Id = {id}");
                }
                else
                {
                    int sid = rng.Next(1, 8);
                    if (op == 0) ctx.Execute($"INSERT INTO S VALUES ({sid}, {rng.Next(0, 100)})");
                    else if (op == 1) ctx.Execute($"UPDATE S SET Kind = {rng.Next(0, 100)} WHERE Sid = {sid}");
                    else ctx.Execute($"DELETE FROM S WHERE Sid = {sid}");
                }
            }
            catch { /* PK clash on insert: skip; oracle unaffected */ }

            ctx.DispatchPendingNotifications();

            // Reference recompute computed independently from the raw single-table rows. The batch join
            // engine drops all right rows when the FROM (left) table is empty — a pre-existing batch bug
            // unrelated to incremental maintenance — so the outer-join oracle must not depend on it
            // (mirroring how the L2 oracle avoids the engine's broken batch MIN/MAX path).
            var expected = OuterJoinReference(ctx, kind)
                .OrderBy(x => x.Item1 ?? -1).ThenBy(x => x.Item2 ?? -1).ThenBy(x => x.Item3 ?? -1).ToArray();

            var actual = live.Values
                .OrderBy(x => x.rid ?? -1).ThenBy(x => x.tag ?? -1).ThenBy(x => x.kind ?? -1).ToArray();

            Assert.Equal(expected, actual);
        }
    }

    /// <summary>
    /// Computes the LEFT/FULL equi-join of R(Id,Gid,Tag) and S(Sid,Kind) on R.Gid = S.Sid from the raw
    /// single-table rows, as the trustworthy recompute reference for the outer-join oracle.
    /// </summary>
    private static List<(int? Rid, int? Tag, int? Kind)> OuterJoinReference(DataVoContext ctx, string kind)
    {
        var rRows = ctx.Execute("SELECT Id, Gid, Tag FROM R").Single().Data
            .Select(d => (Id: Convert.ToInt32(d["Id"]), Gid: Convert.ToInt32(d["Gid"]), Tag: Convert.ToInt32(d["Tag"])))
            .ToList();
        var sRows = ctx.Execute("SELECT Sid, Kind FROM S").Single().Data
            .Select(d => (Sid: Convert.ToInt32(d["Sid"]), Kind: Convert.ToInt32(d["Kind"])))
            .ToList();

        var sByKey = sRows.ToLookup(s => s.Sid);
        var result = new List<(int? Rid, int? Tag, int? Kind)>();

        // Every left row, paired with each matching right row, or null-padded when none (LEFT and FULL).
        foreach (var r in rRows)
        {
            var matches = sByKey[r.Gid].ToList();
            if (matches.Count == 0)
            {
                result.Add((r.Id, r.Tag, null));
            }
            else
            {
                foreach (var s in matches)
                {
                    result.Add((r.Id, r.Tag, s.Kind));
                }
            }
        }

        // FULL additionally emits each unmatched right row null-padded on the left.
        if (kind == "FULL")
        {
            foreach (var s in sRows)
            {
                if (!rRows.Any(r => r.Gid == s.Sid))
                {
                    result.Add((null, null, s.Kind));
                }
            }
        }

        return result;
    }
}
