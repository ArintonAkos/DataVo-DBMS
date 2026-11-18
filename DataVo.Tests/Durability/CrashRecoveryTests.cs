using System.Diagnostics;
using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.CrashHarness;
using DataVo.Tests.BrowserParity;
using Xunit.Abstractions;

namespace DataVo.Tests.Durability;

/// <summary>
/// Hard-crash durability: a child process (tests/DataVo.CrashHarness) writes and acknowledges rows
/// until it is killed with SIGKILL at a random moment. The database must then reopen cleanly, keep
/// every acknowledged insert and update, and contain nothing beyond the one write that was in flight.
/// </summary>
[BrowserTranslateIgnore("Process-kill crash recovery is disk-specific")]
[Trait("Category", "Crash")]
public sealed class CrashRecoveryTests(ITestOutputHelper output)
{
    private const int CrashCycles = 6;

    // Disk is legacy: an autocommit UPDATE there is still two separate durable steps (tombstone the old
    // row, append the new one), so a crash between them can lose a committed row. Lsm writes every
    // statement as one commit-log frame and must pass.
    private const string DiskNotCrashSafe =
        "Disk mode is legacy and not crash-safe for UPDATE (non-atomic tombstone + append); see WAL And Durability docs.";

    [Theory]
    [InlineData("lsm")]
    [InlineData("disk", Skip = DiskNotCrashSafe)]
    public void AcknowledgedWrites_SurviveRepeatedHardCrashes(string mode)
    {
        string directory = TestStorageDirectories.Create($"crash_{mode}");
        var random = new Random(20261104);
        var expected = new Dictionary<long, long>();
        long nextId = 1;

        using (DataVoContext setup = Open(directory, mode))
        {
            AssertOk(setup.Execute("CREATE DATABASE CrashDb"));
            AssertOk(setup.Execute("USE CrashDb"));
            AssertOk(setup.Execute("CREATE TABLE Rows (Id BIGINT PRIMARY KEY, Value BIGINT)"));
        }

        for (int cycle = 0; cycle < CrashCycles; cycle++)
        {
            int acknowledgementsBeforeKill = random.Next(20, 400);
            CrashRun run = RunUntilKilled(directory, mode, nextId, acknowledgementsBeforeKill);

            foreach ((long id, long value) in run.Inserts)
            {
                expected[id] = value;
            }

            foreach ((long id, long value) in run.Updates)
            {
                expected[id] = value;
            }

            Dictionary<long, long> actual = ReadAll(directory, mode);
            long maxAcknowledgedId = run.Inserts.Count == 0 ? nextId - 1 : run.Inserts[^1].Id;

            List<long> missing = expected.Keys.Where(id => !actual.ContainsKey(id)).Order().ToList();
            Assert.True(
                missing.Count == 0,
                $"cycle {cycle}: {missing.Count} acknowledged rows missing (of {expected.Count}): " +
                $"[{string.Join(", ", missing.Take(20))}]; updated in this run: " +
                $"[{string.Join(", ", missing.Where(id => run.Updates.Any(u => u.Id == id)).Take(20))}]; " +
                $"last acknowledged insert {maxAcknowledgedId}; update in flight at kill: row {run.LastAttemptedUpdate}");

            foreach ((long id, long value) in expected)
            {
                long stored = actual[id];
                Assert.True(
                    stored == value || IsInFlightUpdate(stored, maxAcknowledgedId),
                    $"cycle {cycle}: row {id} has {stored}, expected acknowledged value {value}");
                expected[id] = stored;
            }

            foreach (long id in actual.Keys)
            {
                Assert.True(
                    expected.ContainsKey(id) || id == maxAcknowledgedId + 1,
                    $"cycle {cycle}: row {id} exists but was never written");
            }

            // An in-flight insert may have become durable before the kill; it now counts as written.
            foreach ((long id, long value) in actual)
            {
                expected[id] = value;
            }

            output.WriteLine($"cycle {cycle}: {run.Inserts.Count} inserts, {run.Updates.Count} updates acknowledged; {actual.Count} rows after recovery");
            nextId = actual.Count == 0 ? 1 : actual.Keys.Max() + 1;
        }
    }

    [Fact]
    public void Transactions_AndMultiRowStatements_AreAtomicAcrossHardCrashes()
    {
        string directory = TestStorageDirectories.Create("crash_txn");
        var random = new Random(4111);
        long maxAcknowledged = 0;

        using (DataVoContext setup = Open(directory, "lsm"))
        {
            AssertOk(setup.Execute("CREATE DATABASE CrashDb"));
            AssertOk(setup.Execute("USE CrashDb"));
            AssertOk(setup.Execute("CREATE TABLE Lefts (Id BIGINT PRIMARY KEY, Gen BIGINT)"));
            AssertOk(setup.Execute("CREATE TABLE Rights (Id BIGINT PRIMARY KEY)"));
        }

        for (int cycle = 0; cycle < CrashCycles; cycle++)
        {
            CrashRun run = RunUntilKilled(directory, "lsm", maxAcknowledged + 1, random.Next(5, 60), "txn");
            if (run.Transactions.Count > 0)
            {
                maxAcknowledged = run.Transactions[^1];
            }

            using DataVoContext context = Open(directory, "lsm");
            AssertOk(context.Execute("USE CrashDb"));
            List<Dictionary<string, object?>> lefts = context.Execute("SELECT Id, Gen FROM Lefts")[0].Data;
            HashSet<long> leftIds = lefts.Select(r => Convert.ToInt64(r["Id"])).ToHashSet();
            HashSet<long> rightIds = context.Execute("SELECT Id FROM Rights")[0].Data.Select(r => Convert.ToInt64(r["Id"])).ToHashSet();
            long highest = leftIds.Count == 0 ? 0 : leftIds.Max();

            Assert.True(leftIds.SetEquals(rightIds), $"cycle {cycle}: a transaction was applied to only one of its two tables");
            Assert.True(highest >= maxAcknowledged, $"cycle {cycle}: acknowledged transaction {maxAcknowledged} is missing (highest {highest})");
            Assert.True(highest <= maxAcknowledged + 1, $"cycle {cycle}: transaction {highest} appeared beyond the one in flight");
            Assert.Equal(Enumerable.Range(1, (int)highest).Select(i => (long)i).ToHashSet(), leftIds);
            Assert.All(lefts, row => Assert.Equal(highest, Convert.ToInt64(row["Gen"])));

            output.WriteLine($"cycle {cycle}: {run.Transactions.Count} transactions acknowledged; {highest} after recovery");
            maxAcknowledged = highest;
        }
    }

    /// <summary>
    /// Four sessions run conflicting money transfers in explicit transactions (with a savepoint rolled back inside
    /// each) and the process is killed at random. After every recovery the balances must equal the initial balances
    /// plus exactly the logged transfers — a transfer is all or nothing — every acknowledged commit must be there,
    /// and the total must be unchanged.
    /// </summary>
    [Fact]
    public void ConcurrentConflictingTransactions_AreAtomicAndDurableAcrossHardCrashes()
    {
        string directory = TestStorageDirectories.Create("crash_mvcc");
        var random = new Random(9173);
        const int Accounts = 8;
        const long Initial = 1_000;

        using (DataVoContext setup = Open(directory, "lsm"))
        {
            AssertOk(setup.Execute("CREATE DATABASE CrashDb"));
            AssertOk(setup.Execute("USE CrashDb"));
            AssertOk(setup.Execute("CREATE TABLE Accounts (Id INT PRIMARY KEY, Bal BIGINT)"));
            AssertOk(setup.Execute("CREATE TABLE Transfers (Id BIGINT PRIMARY KEY, Src INT, Dst INT, Amount BIGINT)"));
            for (int id = 1; id <= Accounts; id++)
            {
                AssertOk(setup.Execute($"INSERT INTO Accounts (Id, Bal) VALUES ({id}, {Initial})"));
            }
        }

        long nextId = 1;
        var acknowledged = new HashSet<long>();
        for (int cycle = 0; cycle < CrashCycles; cycle++)
        {
            CrashRun run = RunUntilKilled(directory, "lsm", nextId, random.Next(20, 120), "mvcc");
            acknowledged.UnionWith(run.Transactions);

            using DataVoContext context = Open(directory, "lsm");
            AssertOk(context.Execute("USE CrashDb"));
            var expected = Enumerable.Range(1, Accounts).ToDictionary(id => id, _ => Initial);
            var logged = new HashSet<long>();
            foreach (Dictionary<string, object?> row in context.Execute("SELECT Id, Src, Dst, Amount FROM Transfers")[0].Data)
            {
                long amount = Convert.ToInt64(row["Amount"]);
                expected[Convert.ToInt32(row["Src"])] -= amount;
                expected[Convert.ToInt32(row["Dst"])] += amount;
                logged.Add(Convert.ToInt64(row["Id"]));
            }

            var balances = context.Execute("SELECT Id, Bal FROM Accounts")[0].Data
                .ToDictionary(row => Convert.ToInt32(row["Id"]), row => Convert.ToInt64(row["Bal"]));

            Assert.Equal(expected, balances);
            Assert.Equal(Accounts * Initial, balances.Values.Sum());
            long[] missing = [.. acknowledged.Where(id => !logged.Contains(id)).Order()];
            Assert.True(
                missing.Length == 0,
                $"cycle {cycle}: acknowledged transfers missing after recovery: {string.Join(",", missing)} (logged {logged.Count}, max {(logged.Count == 0 ? 0 : logged.Max())}, acknowledged this run {run.Transactions.Count})");
            output.WriteLine($"cycle {cycle}: {run.Transactions.Count} transfers acknowledged; {logged.Count} logged in total");
            nextId = (logged.Count == 0 ? 0 : logged.Max()) + 1_000;
        }
    }

    /// <summary>
    /// A table with a <c>USING COLUMNAR</c> index takes interleaved inserts, updates, deletes and multi-row
    /// statements, flushes and compactions, and is killed at random. After every crash its rows must be the
    /// acknowledged ones (plus at most the one transaction in flight), every SSTable must carry a usable columnar
    /// segment, and every covered aggregate must equal both the model and the row scan of an identical table
    /// without the index.
    /// </summary>
    [Fact]
    public void ColumnarIndexedTable_RowsAndAggregates_MatchTheModelAndARowScan_AcrossHardCrashes()
    {
        string directory = TestStorageDirectories.Create("crash_columnar");
        var random = new Random(20261113);
        var model = new SortedDictionary<long, ColumnarCrashWorkload.Cells>();
        long nextId = 1;
        int flushes = 0;
        int compactions = 0;
        int mostFiles = 0;

        using (DataVoContext setup = Open(directory, "lsm"))
        {
            AssertOk(setup.Execute("CREATE DATABASE CrashDb"));
            AssertOk(setup.Execute("USE CrashDb"));
            AssertOk(setup.Execute($"CREATE TABLE {ColumnarCrashWorkload.IndexedTable} ({ColumnarCrashWorkload.Columns})"));
            AssertOk(setup.Execute($"CREATE TABLE {ColumnarCrashWorkload.PlainTable} ({ColumnarCrashWorkload.Columns})"));
            AssertOk(setup.Execute($"CREATE INDEX cx ON {ColumnarCrashWorkload.IndexedTable} ({ColumnarCrashWorkload.IndexedColumns}) USING COLUMNAR"));
            Assert.NotNull(setup.Engine.Columnar.Find("CrashDb", ColumnarCrashWorkload.IndexedTable));
        }

        for (int cycle = 0; cycle < CrashCycles; cycle++)
        {
            CrashRun run = RunUntilKilled(directory, "lsm", nextId, random.Next(60, 220), "columnar");
            flushes += run.Flushes;
            compactions += run.Compactions;
            foreach (ColumnarOp op in run.ColumnarAcks)
            {
                Apply(model, op);
            }

            foreach (ColumnarOp op in run.ColumnarAcks.Append(run.ColumnarInFlight ?? default))
            {
                nextId = op.Kind switch
                {
                    ColumnarCrashWorkload.Insert => Math.Max(nextId, op.A + 1),
                    ColumnarCrashWorkload.BulkInsert => Math.Max(nextId, op.A + op.B),
                    _ => nextId,
                };
            }

            using DataVoContext context = Open(directory, "lsm");
            AssertOk(context.Execute("USE CrashDb"));
            Dictionary<long, ColumnarCrashWorkload.Cells> indexed = ReadCells(context, ColumnarCrashWorkload.IndexedTable);
            Dictionary<long, ColumnarCrashWorkload.Cells> plain = ReadCells(context, ColumnarCrashWorkload.PlainTable);

            // The transaction in flight at the kill may or may not have become durable; nothing else may differ.
            var candidates = new List<SortedDictionary<long, ColumnarCrashWorkload.Cells>> { model };
            if (run.ColumnarInFlight is { } inFlight)
            {
                var applied = new SortedDictionary<long, ColumnarCrashWorkload.Cells>(model);
                Apply(applied, inFlight);
                candidates.Add(applied);
            }

            SortedDictionary<long, ColumnarCrashWorkload.Cells>? recovered = candidates.FirstOrDefault(candidate => SameRows(candidate, indexed));
            Assert.True(
                recovered is not null,
                $"cycle {cycle}: the indexed table does not hold the acknowledged rows ({run.ColumnarAcks.Count} acknowledged, in flight: {run.ColumnarInFlight}); " +
                DescribeDifference(model, indexed));
            Assert.True(SameRows(recovered, plain), $"cycle {cycle}: the plain table differs from the indexed one; " + DescribeDifference(recovered, plain));
            model = recovered;

            AssertPointReads(context, model, random, cycle);
            foreach (string sql in AssertColumnarAggregates(context, model, cycle))
            {
                output.WriteLine($"cycle {cycle}: answered by the index as well: {sql}");
            }

            mostFiles = Math.Max(mostFiles, AssertEverySsTableCarriesItsSegment(directory, cycle));

            output.WriteLine($"cycle {cycle}: {run.ColumnarAcks.Count} transactions acknowledged; {model.Count} rows; {mostFiles} SSTables at most");
        }

        Assert.True(flushes > 0 && compactions > 0, $"The workload flushed {flushes} and compacted {compactions} times; both must happen.");
        Assert.True(mostFiles > 1, "The columnar table never had more than one SSTable.");
    }

    // Covered aggregates, each run against the indexed table (where the index must answer it, if marked so) and
    // the plain table (rows), with NULLs and NaNs in the columns. The integer ones are checked against the model too.
    private static readonly (string Sql, bool Pushed)[] ColumnarStatements =
    [
        ("SELECT COUNT(*) FROM {t}", true),
        ("SELECT COUNT(*), SUM(Level), MIN(Level), MAX(Level) FROM {t} WHERE Level >= 0", true),
        ("SELECT COUNT(Gold), SUM(Gold), MIN(Gold), MAX(Gold) FROM {t} WHERE Gold IS NOT NULL", true),
        ("SELECT COUNT(*) FROM {t} WHERE Gold IS NULL", true),
        ("SELECT COUNT(*), SUM(Gold), MIN(Gold), MAX(Gold) FROM {t} WHERE Level < 25 AND Gold > -500000", true),
        ("SELECT COUNT(Score), COUNT(Ratio) FROM {t} WHERE Level <> 7", true),
        ("SELECT MIN(Score), MAX(Score), MIN(Ratio), MAX(Ratio) FROM {t} WHERE Level > 10", true),
        ("SELECT COUNT(*) FROM {t} WHERE Score IS NULL AND Ratio IS NOT NULL", true),
        ("SELECT AVG(Level), AVG(Gold) FROM {t} WHERE Gold <> 0", true),
        ("SELECT COUNT(*), SUM(Level) FROM {t} WHERE Score > 0", false), // a comparison over a column with NaNs reads rows
        ("SELECT COUNT(*), SUM(Level) FROM {t} WHERE Score < 0", false),
        ("SELECT COUNT(*) FROM {t} WHERE Score <> 12.5", false),
        ("SELECT COUNT(*), MAX(Gold) FROM {t} WHERE Ratio <= 0", false),
        ("SELECT COUNT(*) FROM {t} WHERE Ratio >= -100", false),
        ("SELECT SUM(Score), SUM(Ratio) FROM {t} WHERE Level >= 0", false), // floating-point sums need key order
    ];

    // Returns the statements not marked as pushed that the index answered anyway.
    private static List<string> AssertColumnarAggregates(DataVoContext context, SortedDictionary<long, ColumnarCrashWorkload.Cells> model, int cycle)
    {
        ColumnarIndex index = Assert.IsType<ColumnarIndex>(context.Engine.Columnar.Find("CrashDb", ColumnarCrashWorkload.IndexedTable));
        var alsoPushed = new List<string>();

        // A query asks the index whether a column holds a NaN before it compares against it; after a reopen that
        // knowledge must come from the files' segments too, or a comparison would be pushed down and differ.
        Assert.True(!model.Values.Any(static r => r.Score is double.NaN) || index.AnyNaN(index.SlotOf(3)), $"cycle {cycle}: the index does not know Score holds a NaN");
        Assert.True(!model.Values.Any(static r => r.Ratio is float.NaN) || index.AnyNaN(index.SlotOf(4)), $"cycle {cycle}: the index does not know Ratio holds a NaN");
        foreach ((string sql, bool pushed) in ColumnarStatements)
        {
            long before = index.Scans;
            QueryResult fromIndex = Query(context, sql.Replace("{t}", ColumnarCrashWorkload.IndexedTable));
            long answered = index.Scans - before;
            QueryResult fromRows = Query(context, sql.Replace("{t}", ColumnarCrashWorkload.PlainTable));
            AssertSameResult(fromRows, fromIndex, $"cycle {cycle}: {sql}");
            if (pushed)
            {
                Assert.True(answered == 1, $"cycle {cycle}: '{sql}' was not answered by the columnar index");
            }
            else if (answered > 0)
            {
                alsoPushed.Add(sql);
            }
        }

        List<ColumnarCrashWorkload.Cells> rows = [.. model.Values];
        List<ColumnarCrashWorkload.Cells> positive = [.. rows.Where(static r => r.Level >= 0)];
        List<long> gold = [.. rows.Where(static r => r.Gold is not null).Select(static r => r.Gold!.Value)];
        List<ColumnarCrashWorkload.Cells> poor = [.. rows.Where(static r => r.Level < 25 && r.Gold > -500_000)];
        AssertModel(context, cycle, ColumnarStatements[0].Sql, rows.Count);
        AssertModel(context, cycle, ColumnarStatements[1].Sql, positive.Count, Sum(positive.Select(static r => (long)r.Level)), Min(positive.Select(static r => (long)r.Level)), Max(positive.Select(static r => (long)r.Level)));
        AssertModel(context, cycle, ColumnarStatements[2].Sql, gold.Count, Sum(gold), Min(gold), Max(gold));
        AssertModel(context, cycle, ColumnarStatements[3].Sql, rows.Count - gold.Count);
        AssertModel(context, cycle, ColumnarStatements[4].Sql, poor.Count, Sum(poor.Select(static r => r.Gold!.Value)), Min(poor.Select(static r => r.Gold!.Value)), Max(poor.Select(static r => r.Gold!.Value)));
        AssertModel(context, cycle, ColumnarStatements[5].Sql, rows.Count(static r => r.Level != 7 && r.Score is not null), rows.Count(static r => r.Level != 7 && r.Ratio is not null));
        AssertModel(context, cycle, ColumnarStatements[7].Sql, rows.Count(static r => r.Score is null && r.Ratio is not null));
        return alsoPushed;
    }

    // Point lookups by primary key read the newest version of a row, live or deleted, from the overlay or a file.
    private static void AssertPointReads(DataVoContext context, SortedDictionary<long, ColumnarCrashWorkload.Cells> model, Random random, int cycle)
    {
        long highest = model.Count == 0 ? 10 : model.Keys.Max() + 2;
        for (int i = 0; i < 40; i++)
        {
            long id = model.Count > 0 && i % 2 == 0 ? model.Keys.ElementAt(random.Next(model.Count)) : random.NextInt64(1, highest);
            Dictionary<long, ColumnarCrashWorkload.Cells> found = ReadCells(context, ColumnarCrashWorkload.IndexedTable, $" WHERE Id = {id}");
            if (model.TryGetValue(id, out ColumnarCrashWorkload.Cells expected))
            {
                Assert.True(found.TryGetValue(id, out ColumnarCrashWorkload.Cells actual) && actual.Equals(expected), $"cycle {cycle}: row {id} reads as {(found.Count == 0 ? "missing" : found[id])}, expected {expected}");
            }
            else
            {
                Assert.True(found.Count == 0, $"cycle {cycle}: row {id} should not exist");
            }
        }
    }

    private static long? Sum(IEnumerable<long> values) => values.Any() ? values.Sum() : null;

    private static long? Min(IEnumerable<long> values) => values.Any() ? values.Min() : null;

    private static long? Max(IEnumerable<long> values) => values.Any() ? values.Max() : null;

    // Compares the first row of the indexed table's answer with numbers worked out from the model.
    private static void AssertModel(DataVoContext context, int cycle, string sql, params long?[] expected)
    {
        QueryResult result = Query(context, sql.Replace("{t}", ColumnarCrashWorkload.IndexedTable));
        Assert.Equal(expected.Length, result.Fields.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            object? value = result.Data.Count == 0 ? null : result.Data[0][result.Fields[i]];
            long? actual = value is null ? null : Convert.ToInt64(value, CultureInfo.InvariantCulture);
            Assert.True(
                expected[i] == actual,
                $"cycle {cycle}: '{sql}' column {i} is {value ?? "NULL"}, the model says {expected[i]?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}");
        }
    }

    private static void AssertSameResult(QueryResult expected, QueryResult actual, string what)
    {
        Assert.Equal(expected.Fields, actual.Fields);
        Assert.True(expected.Data.Count == actual.Data.Count, $"{what}: {actual.Data.Count} rows, the row scan has {expected.Data.Count}");
        for (int row = 0; row < expected.Data.Count; row++)
        {
            foreach (string field in expected.Fields)
            {
                object? want = expected.Data[row][field];
                object? got = actual.Data[row][field];
                bool same = (want, got) switch
                {
                    (double a, double b) => BitConverter.DoubleToInt64Bits(a) == BitConverter.DoubleToInt64Bits(b),
                    (float a, float b) => BitConverter.SingleToInt32Bits(a) == BitConverter.SingleToInt32Bits(b),
                    _ => Equals(want, got),
                };
                Assert.True(same, $"{what}: {field} is {Describe(got)} from the index, {Describe(want)} from rows");
            }
        }
    }

    private static string Describe(object? value) => value is null ? "NULL" : $"{value} ({value.GetType().Name})";

    // Every SSTable of the indexed table is a version 8 file with a usable segment for the table's spec.
    private static int AssertEverySsTableCarriesItsSegment(string directory, int cycle)
    {
        string[] files = [.. Directory.GetFiles(directory, "*.sst", SearchOption.AllDirectories)
            .Where(static path => Path.GetExtension(path) == ".sst"
                && string.Equals(Path.GetFileName(Path.GetDirectoryName(path)), ColumnarCrashWorkload.IndexedTable, StringComparison.OrdinalIgnoreCase))];
        ColumnarSpec? spec = null;
        foreach (string path in files)
        {
            using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
            Assert.True(reader.Version == SsTableV2Format.ColumnarVersion, $"cycle {cycle}: {path} is version {reader.Version}");
            SsTableColumnarSegment segment = reader.Columnar!;
            Assert.True(segment.Usable, $"cycle {cycle}: {path} has an unusable segment");
            spec ??= segment.Spec;
            Assert.True(segment.Spec.Matches(spec), $"cycle {cycle}: {path} has a segment for other columns");
        }

        return files.Length;
    }

    private static void Apply(SortedDictionary<long, ColumnarCrashWorkload.Cells> model, ColumnarOp op)
    {
        switch (op.Kind)
        {
            case ColumnarCrashWorkload.Insert:
            case ColumnarCrashWorkload.Update:
                model[op.A] = ColumnarCrashWorkload.CellsOf(op.A, op.B);
                break;
            case ColumnarCrashWorkload.BulkInsert:
                for (long id = op.A; id < op.A + op.B; id++)
                {
                    model[id] = ColumnarCrashWorkload.CellsOf(id, op.A);
                }

                break;
            case ColumnarCrashWorkload.Delete:
                model.Remove(op.A);
                break;
            case ColumnarCrashWorkload.DeleteRange:
                foreach (long id in model.Keys.Where(id => id >= op.A && id < op.A + op.B).ToList())
                {
                    model.Remove(id);
                }

                break;
            case ColumnarCrashWorkload.NullGoldRange:
                foreach (long id in model.Keys.Where(id => id >= op.A && id < op.A + op.B).ToList())
                {
                    model[id] = model[id] with { Gold = null };
                }

                break;
            default:
                throw new InvalidOperationException($"Unknown columnar operation {op}.");
        }
    }

    private static Dictionary<long, ColumnarCrashWorkload.Cells> ReadCells(DataVoContext context, string table, string where = "")
    {
        QueryResult result = Query(context, $"SELECT Id, Level, Gold, Score, Ratio FROM {table}{where}");
        return result.Data.ToDictionary(
            static row => Convert.ToInt64(row["Id"], CultureInfo.InvariantCulture),
            static row => new ColumnarCrashWorkload.Cells(
                Convert.ToInt32(row["Level"], CultureInfo.InvariantCulture),
                row["Gold"] is { } gold ? Convert.ToInt64(gold, CultureInfo.InvariantCulture) : null,
                row["Score"] is { } score ? Convert.ToDouble(score, CultureInfo.InvariantCulture) : null,
                row["Ratio"] is { } ratio ? (float)Convert.ToDouble(ratio, CultureInfo.InvariantCulture) : null));
    }

    private static bool SameRows(IReadOnlyDictionary<long, ColumnarCrashWorkload.Cells> expected, IReadOnlyDictionary<long, ColumnarCrashWorkload.Cells> actual) =>
        expected.Count == actual.Count && expected.All(pair => actual.TryGetValue(pair.Key, out ColumnarCrashWorkload.Cells cells) && cells.Equals(pair.Value));

    private static string DescribeDifference(IReadOnlyDictionary<long, ColumnarCrashWorkload.Cells> expected, IReadOnlyDictionary<long, ColumnarCrashWorkload.Cells> actual)
    {
        IEnumerable<string> missing = expected.Keys.Where(id => !actual.ContainsKey(id)).Take(10).Select(id => $"missing {id}");
        IEnumerable<string> extra = actual.Keys.Where(id => !expected.ContainsKey(id)).Take(10).Select(id => $"extra {id}");
        IEnumerable<string> changed = expected.Where(pair => actual.TryGetValue(pair.Key, out ColumnarCrashWorkload.Cells cells) && !cells.Equals(pair.Value))
            .Take(10)
            .Select(pair => $"row {pair.Key} is {actual[pair.Key]}, expected {pair.Value}");
        return string.Join("; ", missing.Concat(extra).Concat(changed));
    }

    private static QueryResult Query(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    // The harness writes updates as (insert id * 1000 + 1); only the update issued after the last
    // acknowledged insert can be durable without having been acknowledged.
    private static bool IsInFlightUpdate(long stored, long maxAcknowledgedId) =>
        stored % 1000 == 1 && stored / 1000 >= maxAcknowledgedId;

    private static CrashRun RunUntilKilled(string directory, string mode, long firstId, int acknowledgementsBeforeKill, string workload = "rows")
    {
        var start = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        start.ArgumentList.Add(HarnessPath());
        start.ArgumentList.Add(directory);
        start.ArgumentList.Add(mode);
        start.ArgumentList.Add(firstId.ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add(workload);

        using Process process = Process.Start(start) ?? throw new InvalidOperationException("Could not start the crash harness.");
        var run = new CrashRun();
        var deadline = Stopwatch.StartNew();
        int acknowledged = 0;
        string? line;
        while ((line = process.StandardOutput.ReadLine()) is not null)
        {
            if (run.Record(line))
            {
                acknowledged++;
            }

            if (acknowledged >= acknowledgementsBeforeKill || deadline.Elapsed > TimeSpan.FromSeconds(30))
            {
                break;
            }
        }

        if (process.HasExited)
        {
            throw new InvalidOperationException(
                $"Crash harness exited early with code {process.ExitCode}: {process.StandardError.ReadToEnd()}");
        }

        process.Kill(entireProcessTree: true); // SIGKILL on Unix: no finalizers, no Dispose, no flush
        process.WaitForExit();

        // Lines flushed before the kill were acknowledged; a trailing partial line is not complete.
        string remainder = process.StandardOutput.ReadToEnd();
        string[] lines = remainder.Split('\n');
        for (int i = 0; i < lines.Length - 1; i++)
        {
            run.Record(lines[i]);
        }

        return run;
    }

    private static Dictionary<long, long> ReadAll(string directory, string mode)
    {
        using DataVoContext context = Open(directory, mode);
        AssertOk(context.Execute("USE CrashDb"));
        QueryResult result = context.Execute("SELECT Id, Value FROM Rows")[0];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return result.Data.ToDictionary(row => Convert.ToInt64(row["Id"]), row => Convert.ToInt64(row["Value"]));
    }

    private static DataVoContext Open(string directory, string mode) => new(mode == "lsm"
        ? new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory, LsmStrictFsync = true }
        : new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = directory, SyncDiskWrites = true });

    private static string HarnessPath()
    {
        // .../DataVo.Tests/bin/<Configuration>/net10.0/ -> .../tests/DataVo.CrashHarness/bin/<Configuration>/net10.0/
        var testOutput = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar));
        string framework = testOutput.Name;
        string configuration = testOutput.Parent!.Name;
        DirectoryInfo repository = testOutput.Parent.Parent!.Parent!.Parent!;
        string path = Path.Combine(repository.FullName, "tests", "DataVo.CrashHarness", "bin", configuration, framework, "DataVo.CrashHarness.dll");
        Assert.True(File.Exists(path), $"Crash harness not built at {path}");
        return path;
    }

    private static void AssertOk(List<QueryResult> results)
    {
        QueryResult? error = results.FirstOrDefault(r => r.IsError);
        Assert.True(error is null, error is null ? null : string.Join("; ", error.Messages));
    }

    private sealed class CrashRun
    {
        public List<(long Id, long Value)> Inserts { get; } = [];

        public List<(long Id, long Value)> Updates { get; } = [];

        public long? LastAttemptedUpdate { get; private set; }

        public List<long> Transactions { get; } = [];

        /// <summary>Columnar workload: the transactions that committed, in order.</summary>
        public List<ColumnarOp> ColumnarAcks { get; } = [];

        /// <summary>Columnar workload: the transaction that had started and not been acknowledged at the kill.</summary>
        public ColumnarOp? ColumnarInFlight { get; private set; }

        public int Flushes { get; private set; }

        public int Compactions { get; private set; }

        public bool Record(string line)
        {
            string[] parts = line.Trim().Split(' ');
            if (parts.Length == 4)
            {
                return RecordColumnar(parts);
            }

            if (parts.Length != 3)
            {
                return false;
            }

            var entry = (long.Parse(parts[1], CultureInfo.InvariantCulture), long.Parse(parts[2], CultureInfo.InvariantCulture));
            switch (parts[0])
            {
                case "INS":
                    Inserts.Add(entry);
                    return true;
                case "UPD":
                    Updates.Add(entry);
                    return true;
                case "TXN":
                    Transactions.Add(entry.Item1);
                    return true;
                case "TRY":
                    LastAttemptedUpdate = entry.Item1;
                    return false;
                default:
                    return false;
            }
        }

        private bool RecordColumnar(string[] parts)
        {
            if (parts[1].Length != 1)
            {
                return false;
            }

            var op = new ColumnarOp(parts[1][0], long.Parse(parts[2], CultureInfo.InvariantCulture), long.Parse(parts[3], CultureInfo.InvariantCulture));
            switch (parts[0])
            {
                case "TRY":
                    ColumnarInFlight = op;
                    return false;
                case "ACK":
                    Assert.Equal(ColumnarInFlight, op);
                    ColumnarAcks.Add(op);
                    ColumnarInFlight = null;
                    return true;
                case "DIA":
                    Flushes += op.Kind == 'F' ? 1 : 0;
                    Compactions += op.Kind == 'C' ? 1 : 0;
                    return false;
                default:
                    return false;
            }
        }
    }

    /// <summary>One transaction of the columnar workload; see <see cref="ColumnarCrashWorkload"/>.</summary>
    private readonly record struct ColumnarOp(char Kind, long A, long B);
}
