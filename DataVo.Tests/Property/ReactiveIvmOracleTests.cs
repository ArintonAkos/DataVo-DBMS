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
    [InlineData(StorageMode.InMemory, 51)]
    [InlineData(StorageMode.InMemory, 52)]
    [InlineData(StorageMode.Disk, 53)]
    [InlineData(StorageMode.Disk, 54)]
    public void Distinct_Incremental_Equals_Recompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE T (Id INT PRIMARY KEY, G INT, V INT)");

        const string sql = "SELECT DISTINCT G FROM T WHERE V < 70";
        var live = new HashSet<int>();
        using var sub = ctx.Subscribe(sql, qc =>
        {
            foreach (var r in qc.Added) live.Add(Convert.ToInt32(r["G"]));
            foreach (var r in qc.Removed) live.Remove(Convert.ToInt32(r["G"]));
        });

        for (int i = 0; i < 400; i++)
        {
            int id = rng.Next(1, 25);
            int g = rng.Next(0, 6);
            int v = rng.Next(0, 100);
            int op = rng.Next(3);

            try
            {
                if (op == 0) ctx.Execute($"INSERT INTO T VALUES ({id}, {g}, {v})");
                else if (op == 1) ctx.Execute($"UPDATE T SET G = {g}, V = {v} WHERE Id = {id}");
                else ctx.Execute($"DELETE FROM T WHERE Id = {id}");
            }
            catch { /* PK clash on insert: skip; oracle unaffected */ }

            ctx.DispatchPendingNotifications();

            var expected = ctx.Execute(sql).Single().Data
                .Select(r => Convert.ToInt32(r["G"]))
                .OrderBy(x => x).ToArray();

            Assert.Equal(expected, live.OrderBy(x => x).ToArray());
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory, 61)]
    [InlineData(StorageMode.InMemory, 62)]
    [InlineData(StorageMode.Disk, 63)]
    [InlineData(StorageMode.Disk, 64)]
    public void Union_Incremental_Equals_Recompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE A (Id INT PRIMARY KEY, V INT)");
        ctx.Execute("CREATE TABLE B (Id INT PRIMARY KEY, V INT)");

        const string unionSql = "SELECT V FROM A UNION SELECT V FROM B";
        const string unionAllSql = "SELECT V FROM A UNION ALL SELECT V FROM B";

        var liveUnion = new HashSet<int>();
        var liveUnionAll = new Dictionary<int, int>();

        using var unionSub = ctx.Subscribe(unionSql, qc =>
        {
            foreach (var r in qc.Added) liveUnion.Add(Convert.ToInt32(r["V"]));
            foreach (var r in qc.Removed) liveUnion.Remove(Convert.ToInt32(r["V"]));
        });

        using var unionAllSub = ctx.Subscribe(unionAllSql, qc =>
        {
            foreach (var r in qc.Added) AddCount(liveUnionAll, Convert.ToInt32(r["V"]), +1);
            foreach (var r in qc.Removed) AddCount(liveUnionAll, Convert.ToInt32(r["V"]), -1);
        });

        for (int i = 0; i < 400; i++)
        {
            string table = rng.Next(2) == 0 ? "A" : "B";
            int id = rng.Next(1, 25);
            int v = rng.Next(0, 8);
            int op = rng.Next(3);

            try
            {
                if (op == 0) ctx.Execute($"INSERT INTO {table} VALUES ({id}, {v})");
                else if (op == 1) ctx.Execute($"UPDATE {table} SET V = {v} WHERE Id = {id}");
                else ctx.Execute($"DELETE FROM {table} WHERE Id = {id}");
            }
            catch { /* PK clash on insert: skip; oracle unaffected */ }

            ctx.DispatchPendingNotifications();

            var expectedUnion = ctx.Execute(unionSql).Single().Data
                .Select(r => Convert.ToInt32(r["V"]))
                .OrderBy(x => x).ToArray();

            var expectedUnionAll = ToCounts(ctx.Execute(unionAllSql).Single().Data.Select(r => Convert.ToInt32(r["V"])));

            Assert.Equal(expectedUnion, liveUnion.OrderBy(x => x).ToArray());
            Assert.Equal(
                expectedUnionAll.OrderBy(k => k.Key).ToArray(),
                liveUnionAll.Where(k => k.Value != 0).OrderBy(k => k.Key).ToArray());
        }

        static Dictionary<int, int> ToCounts(IEnumerable<int> values)
        {
            var counts = new Dictionary<int, int>();
            foreach (int value in values)
            {
                AddCount(counts, value, +1);
            }

            return counts;
        }

        static void AddCount(Dictionary<int, int> counts, int value, int delta)
        {
            counts[value] = counts.GetValueOrDefault(value) + delta;
            if (counts[value] == 0)
            {
                counts.Remove(value);
            }
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory, 71)]
    [InlineData(StorageMode.InMemory, 72)]
    [InlineData(StorageMode.Disk, 73)]
    [InlineData(StorageMode.Disk, 74)]
    public void Subquery_Incremental_Equals_Recompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, Gid INT)");
        ctx.Execute("CREATE TABLE S (Gid INT PRIMARY KEY, Flag INT)");

        const string inSql = "SELECT Id FROM R WHERE Gid IN (SELECT Gid FROM S WHERE Flag = 1)";
        const string existsSql = "SELECT Id FROM R WHERE EXISTS (SELECT Gid FROM S WHERE Flag = 1)";

        var liveIn = new HashSet<int>();
        var liveExists = new HashSet<int>();

        using var inSub = ctx.Subscribe(inSql, qc => ApplySet(liveIn, qc));
        using var existsSub = ctx.Subscribe(existsSql, qc => ApplySet(liveExists, qc));

        for (int i = 0; i < 400; i++)
        {
            int table = rng.Next(2);
            int op = rng.Next(3);

            try
            {
                if (table == 0)
                {
                    int id = rng.Next(1, 25);
                    int gid = rng.Next(1, 10);
                    if (op == 0) ctx.Execute($"INSERT INTO R VALUES ({id}, {gid})");
                    else if (op == 1) ctx.Execute($"UPDATE R SET Gid = {gid} WHERE Id = {id}");
                    else ctx.Execute($"DELETE FROM R WHERE Id = {id}");
                }
                else
                {
                    int gid = rng.Next(1, 10);
                    int flag = rng.Next(0, 2);
                    if (op == 0) ctx.Execute($"INSERT INTO S VALUES ({gid}, {flag})");
                    else if (op == 1) ctx.Execute($"UPDATE S SET Flag = {flag} WHERE Gid = {gid}");
                    else ctx.Execute($"DELETE FROM S WHERE Gid = {gid}");
                }
            }
            catch { /* PK clash on insert: skip; oracle unaffected */ }

            ctx.DispatchPendingNotifications();

            Assert.Equal(ExpectedIds(ctx, inSql), liveIn.OrderBy(x => x).ToArray());
            Assert.Equal(ExpectedIds(ctx, existsSql), liveExists.OrderBy(x => x).ToArray());
        }

        static void ApplySet(HashSet<int> live, QueryChange qc)
        {
            foreach (var r in qc.Added.Concat(qc.Updated)) live.Add(Convert.ToInt32(r["Id"]));
            foreach (var r in qc.Removed) live.Remove(Convert.ToInt32(r["Id"]));
        }

        static int[] ExpectedIds(DataVoContext ctx, string sql)
        {
            return ctx.Execute(sql).Single().Data
                .Select(r => Convert.ToInt32(r["Id"]))
                .OrderBy(x => x).ToArray();
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory, 81)]
    [InlineData(StorageMode.InMemory, 82)]
    [InlineData(StorageMode.Disk, 83)]
    [InlineData(StorageMode.Disk, 84)]
    public void CorrelatedSubquery_Incremental_Equals_Recompute(StorageMode mode, int seed)
    {
        var rng = new Random(seed);
        using var ctx = ChangeCaptureIntegrationTests.NewContext(mode, out _);
        ctx.Execute("CREATE TABLE R (Id INT PRIMARY KEY, K INT, C INT)");
        ctx.Execute("CREATE TABLE S (Id INT PRIMARY KEY, K INT, V INT, Flag INT)");

        // Correlated EXISTS (equality correlation + residual inner predicate) and correlated IN
        // (equality correlation + value match). Both reference the single outer table R via S.K = R.K.
        const string existsSql =
            "SELECT Id FROM R WHERE EXISTS (SELECT 1 FROM S WHERE S.K = R.K AND S.Flag = 1)";
        const string inSql =
            "SELECT Id FROM R WHERE R.C IN (SELECT S.V FROM S WHERE S.K = R.K AND S.Flag = 1)";
        const string notExistsSql =
            "SELECT Id FROM R WHERE NOT EXISTS (SELECT 1 FROM S WHERE S.K = R.K AND S.Flag = 1)";

        var liveExists = new HashSet<int>();
        var liveIn = new HashSet<int>();
        var liveNotExists = new HashSet<int>();

        using var existsSub = ctx.Subscribe(existsSql, qc => ApplySet(liveExists, qc));
        using var inSub = ctx.Subscribe(inSql, qc => ApplySet(liveIn, qc));
        using var notExistsSub = ctx.Subscribe(notExistsSql, qc => ApplySet(liveNotExists, qc));

        for (int i = 0; i < 500; i++)
        {
            int table = rng.Next(2);
            int op = rng.Next(3);

            try
            {
                if (table == 0)
                {
                    int id = rng.Next(1, 18);
                    int k = rng.Next(1, 6);
                    int c = rng.Next(0, 6);
                    if (op == 0) ctx.Execute($"INSERT INTO R VALUES ({id}, {k}, {c})");
                    else if (op == 1) ctx.Execute($"UPDATE R SET K = {k}, C = {c} WHERE Id = {id}");
                    else ctx.Execute($"DELETE FROM R WHERE Id = {id}");
                }
                else
                {
                    int id = rng.Next(1, 18);
                    int k = rng.Next(1, 6);
                    int v = rng.Next(0, 6);
                    int flag = rng.Next(0, 2);
                    if (op == 0) ctx.Execute($"INSERT INTO S VALUES ({id}, {k}, {v}, {flag})");
                    else if (op == 1) ctx.Execute($"UPDATE S SET K = {k}, V = {v}, Flag = {flag} WHERE Id = {id}");
                    else ctx.Execute($"DELETE FROM S WHERE Id = {id}");
                }
            }
            catch { /* PK clash on insert: skip; oracle unaffected */ }

            ctx.DispatchPendingNotifications();

            // The batch engine's correlated-subquery path returns empty for every correlated EXISTS/IN
            // (a pre-existing batch bug unrelated to incremental maintenance), so — as with the L2 batch
            // MIN/MAX and the outer-join oracle — the reference is computed independently from the raw
            // single-table R and S rows, keeping the incremental==recompute gate honest against true SQL
            // semantics rather than the broken batch path.
            var (refExists, refIn, refNotExists) = CorrelatedReference(ctx);

            Assert.Equal(refExists, liveExists.OrderBy(x => x).ToArray());
            Assert.Equal(refIn, liveIn.OrderBy(x => x).ToArray());
            Assert.Equal(refNotExists, liveNotExists.OrderBy(x => x).ToArray());
        }

        static void ApplySet(HashSet<int> live, QueryChange qc)
        {
            foreach (var r in qc.Added.Concat(qc.Updated)) live.Add(Convert.ToInt32(r["Id"]));
            foreach (var r in qc.Removed) live.Remove(Convert.ToInt32(r["Id"]));
        }
    }

    /// <summary>
    /// Computes the correlated EXISTS / IN / NOT EXISTS reference for the L4b oracle directly from the raw
    /// R(Id,K,C) and S(Id,K,V,Flag) rows, expressing the true SQL semantics:
    /// EXISTS → some S with S.K=R.K and S.Flag=1; IN → additionally S.V=R.C; NOT EXISTS → the complement.
    /// </summary>
    private static (int[] Exists, int[] In, int[] NotExists) CorrelatedReference(DataVoContext ctx)
    {
        var rRows = ctx.Execute("SELECT Id, K, C FROM R").Single().Data
            .Select(d => (Id: Convert.ToInt32(d["Id"]), K: Convert.ToInt32(d["K"]), C: Convert.ToInt32(d["C"])))
            .ToList();
        var sRows = ctx.Execute("SELECT Id, K, V, Flag FROM S").Single().Data
            .Select(d => (K: Convert.ToInt32(d["K"]), V: Convert.ToInt32(d["V"]), Flag: Convert.ToInt32(d["Flag"])))
            .Where(s => s.Flag == 1)
            .ToList();

        var exists = new List<int>();
        var inList = new List<int>();
        var notExists = new List<int>();

        foreach (var r in rRows)
        {
            bool any = sRows.Any(s => s.K == r.K);
            bool anyValue = sRows.Any(s => s.K == r.K && s.V == r.C);
            if (any) exists.Add(r.Id);
            else notExists.Add(r.Id);
            if (anyValue) inList.Add(r.Id);
        }

        return (
            exists.OrderBy(x => x).ToArray(),
            inList.OrderBy(x => x).ToArray(),
            notExists.OrderBy(x => x).ToArray());
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
