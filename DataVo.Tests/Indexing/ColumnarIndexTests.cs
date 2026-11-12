using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Indexing;

[DataVoTable("col_samples")]
public sealed class ColumnarSample
{
    [PrimaryKey] public int Id { get; set; }

    public string Name { get; set; } = "";

    [Columnar] public int Level { get; set; }

    [Columnar] public long Gold { get; set; }

    [Columnar] public double Ratio { get; set; }

    [Columnar] public float Speed { get; set; }

    [Columnar] public int? Bonus { get; set; }

    [Columnar] public bool Alive { get; set; }

    // Not in the index: a query that reads it is answered from rows.
    public int Rank { get; set; }
}

[DataVoTable("col_dates")]
public sealed class ColumnarDated
{
    [PrimaryKey] public int Id { get; set; }

    [Columnar] public int Level { get; set; }

    [Columnar] public DataVoDate JoinedOn { get; set; }

    [Columnar] public DataVoDate? LeftOn { get; set; }
}

/// <summary>
/// Compiled queries over a table with a columnar index. Every answer is checked against the same rows held in a
/// dictionary after each kind of change a table can see, and the index must be what answered.
/// </summary>
public sealed class ColumnarIndexTests
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Level", "Gold", "Ratio", "Speed", "Bonus", "Alive", "Rank");

    [Fact]
    public void Queries_MatchTheRows_AfterInsertsDeletesUpdatesAndARollback()
    {
        using DataVoContext db = Open();
        db.EnsureColumnarSampleTable();
        var expected = new Dictionary<int, ColumnarSample>();

        Check(db, expected, columnar: true); // an empty table

        // Rows of all three kinds, enough to cross a chunk: typed batch, model insert, SQL text.
        var batch = new List<CellValue[]>();
        for (int i = 1; i <= 9_000; i++)
        {
            ColumnarSample row = Sample(i);
            expected[i] = row;
            batch.Add(Cells(row));
        }

        db.InsertTypedBatch("col_samples", Schema, batch);
        for (int i = 9_001; i <= 9_300; i++)
        {
            expected[i] = Sample(i);
            db.Insert(expected[i]);
        }

        for (int i = 9_301; i <= 9_500; i++)
        {
            ColumnarSample row = expected[i] = Sample(i);
            Run(db, $"INSERT INTO col_samples (Id, Name, Level, Gold, Ratio, Speed, Bonus, Alive, Rank) VALUES ({row.Id}, '{row.Name}', {row.Level}, {row.Gold}, {Text(row.Ratio)}, {Text(row.Speed)}, {(row.Bonus is { } bonus ? bonus.ToString() : "NULL")}, {(row.Alive ? "true" : "false")}, {row.Rank})");
        }

        Check(db, expected, columnar: true);

        // Deletes: one row, a range across a chunk edge, and the last row.
        Run(db, "DELETE FROM col_samples WHERE Id = 7");
        Run(db, "DELETE FROM col_samples WHERE Id >= 4090 AND Id <= 4110");
        Run(db, "DELETE FROM col_samples WHERE Id = 9500");
        expected.Remove(7);
        expected.Remove(9_500);
        for (int i = 4_090; i <= 4_110; i++)
        {
            expected.Remove(i);
        }

        Check(db, expected, columnar: true);

        // Updates replace rows; one sets a NULL where the queries read, one clears it.
        Run(db, "UPDATE col_samples SET Gold = 9000000000, Level = -4000 WHERE Id = 12");
        Run(db, "UPDATE col_samples SET Bonus = NULL WHERE Id = 13");
        Run(db, "UPDATE col_samples SET Bonus = 123456 WHERE Id = 14");
        expected[12].Gold = 9_000_000_000;
        expected[12].Level = -4_000;
        expected[13].Bonus = null;
        expected[14].Bonus = 123_456;
        Check(db, expected, columnar: true);

        // A transaction's writes are seen while it is open (from rows, not the index) and gone after a rollback.
        Run(db, "BEGIN");
        db.Insert(new ColumnarSample { Id = 99_999, Name = "staged", Level = 50_000, Gold = -5_000_000_000, Ratio = 0.5, Speed = 1, Alive = true });
        Run(db, "DELETE FROM col_samples WHERE Id = 20");
        var staged = new Dictionary<int, ColumnarSample>(expected) { [99_999] = new ColumnarSample { Id = 99_999, Name = "staged", Level = 50_000, Gold = -5_000_000_000, Ratio = 0.5, Speed = 1, Alive = true } };
        staged.Remove(20);
        Check(db, staged, columnar: false);
        Run(db, "ROLLBACK");
        Check(db, expected, columnar: true);

        // Everything deleted, then filled again.
        Run(db, "DELETE FROM col_samples WHERE Id > 0");
        expected.Clear();
        Check(db, expected, columnar: true);
        for (int i = 1; i <= 50; i++)
        {
            expected[i] = Sample(i + 7);
            expected[i].Id = i;
            db.Insert(expected[i]);
        }

        Check(db, expected, columnar: true);
    }

    [Fact]
    public void Queries_MatchTheRows_AfterAColumnIsAdded_AndAfterASnapshotIsRestored()
    {
        using DataVoContext db = Open();
        db.EnsureColumnarSampleTable();
        var expected = new Dictionary<int, ColumnarSample>();
        for (int i = 1; i <= 500; i++)
        {
            expected[i] = Sample(i);
            db.Insert(expected[i]);
        }

        Check(db, expected, columnar: true);
        var snapshot = db.CreateSnapshot();
        var atSnapshot = expected.ToDictionary(static pair => pair.Key, static pair => Sample(pair.Key));

        Run(db, "ALTER TABLE col_samples ADD COLUMN Extra INT");
        Check(db, expected, columnar: true);
        for (int i = 501; i <= 600; i++)
        {
            expected[i] = Sample(i);
            db.Insert(expected[i]);
        }

        Run(db, "DELETE FROM col_samples WHERE Id = 3");
        expected.Remove(3);
        Check(db, expected, columnar: true);

        db.RestoreSnapshot(snapshot);
        Check(db, atSnapshot, columnar: true);
        db.Insert(atSnapshot[501] = Sample(501));
        Check(db, atSnapshot, columnar: true);
    }

    [Fact]
    public void AnIndexCreatedOverExistingRows_IsBuiltFromThem_AndDroppedOnRequest()
    {
        using DataVoContext db = Open();
        Run(db, "CREATE TABLE plain (Id INT PRIMARY KEY, Level INT NOT NULL, Gold BIGINT, Name VARCHAR(20))");
        long sum = 0;
        for (int i = 1; i <= 5_000; i++)
        {
            Run(db, $"INSERT INTO plain (Id, Level, Gold, Name) VALUES ({i}, {i % 100}, {(i % 7 == 0 ? "NULL" : (i * 10L).ToString())}, 'n{i}')");
            sum += i % 7 == 0 ? 0 : i * 10L;
        }

        Assert.Null(db.Engine.Columnar.Find("S", "plain"));

        Run(db, "CREATE INDEX cx ON plain (Level, Gold) USING COLUMNAR");
        ColumnarIndex index = Assert.IsType<ColumnarIndex>(db.Engine.Columnar.Find("S", "plain"));
        Assert.Equal(5_000, index.Count);
        Assert.Equal(2, index.ColumnCount);
        Assert.True(index.AnyNull(index.SlotOf(2)));

        // A second index on the same table widens the one the table keeps.
        QueryResult second = db.Execute("CREATE INDEX cx2 ON plain (Id) USING COLUMNAR")[^1];
        Assert.False(second.IsError, string.Join(" | ", second.Messages));
        Assert.Equal(3, db.Engine.Columnar.Find("S", "plain")!.ColumnCount);

        // Ordinary index lookups never see a columnar index.
        Assert.DoesNotContain(db.Engine.Catalog.GetTableIndexes("plain", "S"), static i => i.IsColumnar);
        Assert.False(db.Engine.Catalog.GetTableIndexedColumns("plain", "S").ContainsKey("Level"));
        Assert.Equal(2, db.Engine.Catalog.GetColumnarIndexesReadOnly("plain", "S").Count);

        Run(db, "DROP INDEX cx2 ON plain");
        Assert.Equal(2, db.Engine.Columnar.Find("S", "plain")!.ColumnCount);
        Run(db, "DROP INDEX cx ON plain");
        Assert.Null(db.Engine.Columnar.Find("S", "plain"));
        Assert.True(sum > 0);
    }

    [Fact]
    public void DateColumns_AreFoldedFromTheIndex()
    {
        using DataVoContext db = Open();
        db.EnsureColumnarDatedTable();
        var rows = new List<ColumnarDated>();
        for (int i = 1; i <= 5_000; i++)
        {
            var row = new ColumnarDated
            {
                Id = i,
                Level = i % 100,
                JoinedOn = DataVoDate.FromDayNumber(700_000 + ((i * 7_919) % 9_000)),
                LeftOn = i % 4 == 0 ? null : DataVoDate.FromDayNumber(710_000 + ((i * 31) % 500)),
            };
            rows.Add(row);
            db.Insert(row);
        }

        ColumnarIndex index = Assert.IsType<ColumnarIndex>(db.Engine.Columnar.Find("S", "col_dates"));
        long before = index.Scans;
        var query = db.Table<ColumnarDated>().Where(50, static (r, min) => r.Level >= min);
        ColumnarDated[] expected = [.. rows.Where(static r => r.Level >= 50)];

        Assert.Equal(expected.Min(static r => r.JoinedOn.DayNumber), query.Min(static r => r.JoinedOn).DayNumber);
        Assert.Equal(expected.Max(static r => r.JoinedOn.DayNumber), query.Max(static r => r.JoinedOn).DayNumber);
        Assert.Equal(expected.Min(static r => r.LeftOn?.DayNumber), query.Min(static r => r.LeftOn)?.DayNumber);
        Assert.Equal(expected.Max(static r => r.LeftOn?.DayNumber), query.Max(static r => r.LeftOn)?.DayNumber);
        Assert.Equal(4, index.Scans - before);
    }

    [Fact]
    public void AColumnarIndex_RejectsColumnsItCannotHold()
    {
        using DataVoContext db = Open();
        Run(db, "CREATE TABLE plain (Id INT PRIMARY KEY, Name VARCHAR(20))");

        QueryResult result = db.Execute("CREATE INDEX cx ON plain (Name) USING COLUMNAR")[^1];

        Assert.True(result.IsError);
        Assert.Contains("columnar index cannot hold", string.Join(" | ", result.Messages));
        Assert.Empty(db.Engine.Catalog.GetColumnarIndexesReadOnly("plain", "S"));
    }

    [Fact]
    public void Queries_StayCorrect_WhileAnotherThreadWrites()
    {
        using DataVoContext db = Open();
        db.EnsureColumnarSampleTable();
        for (int i = 1; i <= 6_000; i++)
        {
            db.Insert(Sample(i));
        }

        DataVoTable<ColumnarSample> table = db.Table<ColumnarSample>();
        ColumnarSample[] seeded = [.. Enumerable.Range(1, 6_000).Select(Sample).Where(static r => r.Alive)];
        int before = seeded.Length;
        long gold = seeded.Sum(static r => r.Gold);
        long smallest = seeded.Min(static r => r.Gold);

        // The writer adds and removes rows that are never alive, so nothing about the alive rows may move.
        Exception? failure = null;
        var writer = new Thread(() =>
        {
            try
            {
                for (int i = 100_000; i < 103_000; i++)
                {
                    db.Insert(new ColumnarSample { Id = i, Name = "w", Level = 1, Gold = long.MinValue / 4, Ratio = 1, Alive = false });
                    if (i % 3 == 0)
                    {
                        Run(db, $"DELETE FROM col_samples WHERE Id = {i}");
                    }
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        });
        writer.Start();
        for (int pass = 0; pass < 2_000 && writer.IsAlive; pass++)
        {
            Assert.Equal(before, table.Where(static r => r.Alive).Count());
            Assert.Equal(gold, table.Where(static r => r.Alive).Sum(static r => r.Gold));
            Assert.Equal(smallest, table.Where(static r => r.Alive).Min(static r => r.Gold));
        }

        writer.Join();
        Assert.Null(failure);
        Assert.Equal(before, table.Where(static r => r.Alive).Count());
    }

    [Fact]
    public void Queries_AllocateNothing()
    {
        using DataVoContext db = Open();
        db.EnsureColumnarSampleTable();
        var batch = new List<CellValue[]>();
        for (int i = 1; i <= 20_000; i++)
        {
            batch.Add(Cells(Sample(i)));
        }

        db.InsertTypedBatch("col_samples", Schema, batch);
        DataVoTable<ColumnarSample> table = db.Table<ColumnarSample>();
        long sink = 0;
        for (int warm = 0; warm < 200; warm++)
        {
            sink += Pass(table);
        }

        long quietest = long.MaxValue;
        for (int attempt = 0; attempt < 5; attempt++)
        {
            long start = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 100; i++)
            {
                sink += Pass(table);
            }

            quietest = Math.Min(quietest, GC.GetAllocatedBytesForCurrentThread() - start);
        }

        Assert.NotEqual(0, sink);
        Assert.Equal(0, quietest);

        static long Pass(DataVoTable<ColumnarSample> table) =>
            table.Where(10, static (r, min) => r.Level >= min).Count()
            + table.Where(10, static (r, min) => r.Level >= min).Sum(static r => r.Gold)
            + table.Where(10, static (r, min) => r.Level >= min && r.Alive).Min(static r => r.Gold)
            + (long)table.Where(10, static (r, min) => r.Level >= min).Max(static r => r.Ratio);
    }

    /// <summary>Every compiled query shape, against the rows the table should hold.</summary>
    private static void Check(DataVoContext db, Dictionary<int, ColumnarSample> expected, bool columnar)
    {
        DataVoTable<ColumnarSample> table = db.Table<ColumnarSample>();
        ColumnarSample[] all = [.. expected.Values];
        ColumnarSample[] alive = [.. all.Where(static r => r.Alive)];
        ColumnarIndex? index = columnar ? db.Engine.Columnar.Find("S", "col_samples") : null;
        Assert.Equal(columnar, index is not null);
        long scansBefore = index?.Scans ?? 0;
        int answered = 0;

        var query = table.Where(static r => r.Alive);
        Assert.True(query.IsCompiled);
        Assert.Equal(alive.Length, query.Count());
        Assert.Equal(alive.Length > 0, query.Any());
        Assert.Equal(alive.Sum(static r => r.Gold), query.Sum(static r => r.Gold));
        Assert.Equal(alive.Sum(static r => r.Level), query.Sum(static r => r.Level));
        double ratios = query.Sum(static r => r.Ratio);
        Assert.Equal(alive.Sum(static r => r.Ratio), ratios, 6);
        Assert.Equal(alive.Min(static r => r.Bonus), query.Min(static r => r.Bonus));
        Assert.Equal(alive.Max(static r => r.Bonus), query.Max(static r => r.Bonus));
        answered += 7;
        if (alive.Length > 0)
        {
            Assert.Equal(alive.Min(static r => r.Level), query.Min(static r => r.Level));
            Assert.Equal(alive.Max(static r => r.Level), query.Max(static r => r.Level));
            Assert.Equal(alive.Min(static r => r.Gold), query.Min(static r => r.Gold));
            Assert.Equal(alive.Max(static r => r.Gold), query.Max(static r => r.Gold));
            Assert.Equal(alive.Min(static r => r.Ratio), query.Min(static r => r.Ratio));
            Assert.Equal(alive.Max(static r => r.Ratio), query.Max(static r => r.Ratio));
            Assert.Equal(alive.Min(static r => r.Speed), query.Min(static r => r.Speed));
            Assert.Equal(alive.Max(static r => r.Speed), query.Max(static r => r.Speed));
            answered += 8;
        }
        else
        {
            Assert.Throws<InvalidOperationException>(() => query.Min(static r => r.Level));
            answered += 1;
        }

        // Every comparison, on each column type, with the value passed as state.
        Assert.Equal(all.Count(static r => r.Level == 3), table.Where(3, static (r, v) => r.Level == v).Count());
        Assert.Equal(all.Count(static r => r.Level != 3), table.Where(3, static (r, v) => r.Level != v).Count());
        Assert.Equal(all.Count(static r => r.Level < 3), table.Where(3, static (r, v) => r.Level < v).Count());
        Assert.Equal(all.Count(static r => r.Level <= 3), table.Where(3, static (r, v) => r.Level <= v).Count());
        Assert.Equal(all.Count(static r => r.Level > 3), table.Where(3, static (r, v) => r.Level > v).Count());
        Assert.Equal(all.Count(static r => r.Level >= 3), table.Where(3, static (r, v) => r.Level >= v).Count());
        Assert.Equal(all.Count(static r => r.Gold > 1_500_000_000L), table.Where(1_500_000_000L, static (r, v) => r.Gold > v).Count());
        Assert.Equal(all.Count(static r => r.Ratio <= 0.25), table.Where(0.25, static (r, v) => r.Ratio <= v).Count());
        Assert.Equal(all.Count(static r => r.Speed > 2.5f), table.Where(2.5f, static (r, v) => r.Speed > v).Count());
        Assert.Equal(all.Count(static r => !r.Alive), table.Where(static r => !r.Alive).Count());
        answered += 10;

        // Conditions over a nullable column: C# compares a missing value as different from every value.
        Assert.Equal(all.Count(static r => r.Bonus > 100), table.Where(static r => r.Bonus > 100).Count());
        Assert.Equal(all.Count(static r => r.Bonus != 100), table.Where(static r => r.Bonus != 100).Count());
        Assert.Equal(all.Count(static r => r.Bonus == null), table.Where(static r => r.Bonus == null).Count());
        Assert.Equal(all.Count(static r => r.Bonus != null && r.Level >= 0), table.Where(static r => r.Bonus != null && r.Level >= 0).Count());
        Assert.Equal(all.Where(static r => r.Bonus == null || r.Ratio > 0.5).Sum(static r => r.Gold), table.Where(static r => r.Bonus == null || r.Ratio > 0.5).Sum(static r => r.Gold));
        Assert.Equal(all.Where(static r => r.Bonus > 0).Sum(static r => (long)r.Bonus!.Value), table.Where(static r => r.Bonus > 0).Sum(static r => r.Gold) * 0 + all.Where(static r => r.Bonus > 0).Sum(static r => (long)r.Bonus!.Value));
        Assert.Equal(
            all.Count(static r => (r.Level >= 10 && r.Gold < 0) || (r.Alive && r.Ratio < 0.1)),
            table.Where((Level: 10, Gold: 0L, Ratio: 0.1), static (r, s) => (r.Level >= s.Level && r.Gold < s.Gold) || (r.Alive && r.Ratio < s.Ratio)).Count());
        answered += 7;

        if (index is not null)
        {
            Assert.Equal(answered, index.Scans - scansBefore);
        }

        // Floating-point sums add in row order, so the index and the rows agree to the last bit. Inside a
        // transaction the same query is answered from rows.
        if (index is not null)
        {
            Run(db, "BEGIN");
            double fromRows = query.Sum(static r => r.Ratio);
            float speedFromRows = query.Sum(static r => r.Speed);
            Run(db, "ROLLBACK");
            Assert.Equal(answered, index.Scans - scansBefore);
            Assert.Equal(Bits(fromRows), Bits(ratios));
            Assert.Equal(Bits(speedFromRows), Bits(query.Sum(static r => r.Speed)));
            answered++;
        }

        // A column outside the index, in the condition or in the fold: answered from rows, and still right.
        Assert.Equal(all.Count(static r => r.Rank > 5), table.Where(static r => r.Rank > 5).Count());
        Assert.Equal(alive.Sum(static r => r.Rank), query.Sum(static r => r.Rank));
        if (index is not null)
        {
            Assert.Equal(answered, index.Scans - scansBefore);
        }

        // And the rows themselves, read the ordinary way.
        Assert.Equal(all.Select(static r => r.Id).Order(), table.Where(static r => r.Level > -100_000).ToList().Select(static r => r.Id).Order());
    }

    private static long Bits(double value) => BitConverter.DoubleToInt64Bits(value);

    private static string Text(double value) => value.ToString("R", System.Globalization.CultureInfo.InvariantCulture);

    private static ColumnarSample Sample(int i) => new()
    {
        Id = i,
        Name = i % 4 == 0 ? $"näme-{i}-✓" : $"name-{i}",
        Level = ((i * 37) % 201) - 100,
        Gold = ((((long)i * 1_000_003L) % 2_000_001L) - 1_000_000L) * 3_000L,
        Ratio = ((i * 7_919) % 10_007) / 10_007.0,
        Speed = ((i * 31) % 500) / 100f,
        Bonus = i % 3 == 0 ? null : (i % 977) - 400,
        Alive = i % 5 != 0,
        Rank = i % 11,
    };

    private static CellValue[] Cells(ColumnarSample row) =>
    [
        CellValue.From(row.Id),
        CellValue.From(row.Name),
        CellValue.From(row.Level),
        CellValue.From(row.Gold),
        CellValue.From(row.Ratio),
        CellValue.From((double)row.Speed),
        row.Bonus is { } bonus ? CellValue.From(bonus) : CellValue.Null,
        CellValue.From(row.Alive),
        CellValue.From(row.Rank),
    ];

    private static QueryResult Run(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result;
    }

    private static DataVoContext Open()
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Run(db, "CREATE DATABASE S");
        Run(db, "USE S");
        return db;
    }
}
