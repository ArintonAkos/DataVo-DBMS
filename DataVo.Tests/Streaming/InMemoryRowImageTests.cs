using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Streaming;

[DataVoTable("ref_samples")]
public sealed class RefSample
{
    [PrimaryKey] public int Id { get; set; }

    // Text ahead of the columns the scans read: stored bytes have no fixed offsets here, row images do.
    public string Name { get; set; } = "";

    public int Level { get; set; }

    public long Gold { get; set; }

    public double Ratio { get; set; }

    public int? Bonus { get; set; }

    public bool Alive { get; set; }
}

/// <summary>
/// Compiled scans of an in-memory table read its row images. The images are built by the first scan and caught
/// up by later ones, so every answer is checked against the same rows held in a dictionary, after each kind of
/// change a table can see.
/// </summary>
public sealed class InMemoryRowImageTests
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Level", "Gold", "Ratio", "Bonus", "Alive");

    [Fact]
    public void Scans_MatchTheRows_AfterInsertsDeletesUpdatesAndARollback()
    {
        using DataVoContext db = Open();
        db.EnsureRefSampleTable();
        var expected = new Dictionary<int, RefSample>();

        Check(db, expected); // an empty table, before any image exists

        // Rows of all three kinds: typed batch, model insert, SQL text.
        var batch = new List<CellValue[]>();
        for (int i = 1; i <= 3_000; i++)
        {
            RefSample row = Sample(i);
            expected[i] = row;
            batch.Add(Cells(row));
        }

        db.InsertTypedBatch("ref_samples", Schema, batch);
        for (int i = 3_001; i <= 3_300; i++)
        {
            expected[i] = Sample(i);
            db.Insert(expected[i]);
        }

        for (int i = 3_301; i <= 3_600; i++)
        {
            RefSample row = expected[i] = Sample(i);
            Run(db, $"INSERT INTO ref_samples (Id, Name, Level, Gold, Ratio, Bonus, Alive) VALUES ({row.Id}, '{row.Name}', {row.Level}, {row.Gold}, {row.Ratio.ToString("R", System.Globalization.CultureInfo.InvariantCulture)}, {(row.Bonus is { } bonus ? bonus.ToString() : "NULL")}, {(row.Alive ? "true" : "false")})");
        }

        Check(db, expected); // builds the image
        Check(db, expected); // and reads it again

        // Rows added after the image was built.
        for (int i = 3_601; i <= 3_700; i++)
        {
            expected[i] = Sample(i);
            db.Insert(expected[i]);
        }

        Check(db, expected);

        // Deletes: one row, a range across a block edge, and the last row.
        Run(db, "DELETE FROM ref_samples WHERE Id = 7");
        Run(db, "DELETE FROM ref_samples WHERE Id >= 2040 AND Id <= 2060");
        Run(db, "DELETE FROM ref_samples WHERE Id = 3700");
        expected.Remove(7);
        expected.Remove(3_700);
        for (int i = 2_040; i <= 2_060; i++)
        {
            expected.Remove(i);
        }

        Check(db, expected);

        // Updates replace rows; one sets a NULL where the scans read, one clears it.
        Run(db, "UPDATE ref_samples SET Gold = 9000000000, Level = -4000 WHERE Id = 12");
        Run(db, "UPDATE ref_samples SET Bonus = NULL WHERE Id = 13");
        Run(db, "UPDATE ref_samples SET Bonus = 123456 WHERE Id = 14");
        expected[12].Gold = 9_000_000_000;
        expected[12].Level = -4_000;
        expected[13].Bonus = null;
        expected[14].Bonus = 123_456;
        Check(db, expected);

        // A transaction's writes are seen while it is open and gone after a rollback.
        Run(db, "BEGIN");
        db.Insert(new RefSample { Id = 9_999, Name = "staged", Level = 50_000, Gold = -5_000_000_000, Ratio = 0.5, Alive = true });
        Run(db, "DELETE FROM ref_samples WHERE Id = 20");
        var staged = new Dictionary<int, RefSample>(expected) { [9_999] = new RefSample { Id = 9_999, Name = "staged", Level = 50_000, Gold = -5_000_000_000, Ratio = 0.5, Alive = true } };
        staged.Remove(20);
        Check(db, staged);
        Run(db, "ROLLBACK");
        Check(db, expected);

        // Everything deleted, then filled again.
        Run(db, "DELETE FROM ref_samples WHERE Id > 0");
        expected.Clear();
        Check(db, expected);
        for (int i = 1; i <= 50; i++)
        {
            expected[i] = Sample(i + 7);
            expected[i].Id = i;
            db.Insert(expected[i]);
        }

        Check(db, expected);
    }

    [Fact]
    public void Scans_MatchTheRows_AfterAColumnIsAdded_AndAfterASnapshotIsRestored()
    {
        using DataVoContext db = Open();
        db.EnsureRefSampleTable();
        var expected = new Dictionary<int, RefSample>();
        for (int i = 1; i <= 500; i++)
        {
            expected[i] = Sample(i);
            db.Insert(expected[i]);
        }

        Check(db, expected);
        var snapshot = db.CreateSnapshot();
        var atSnapshot = expected.ToDictionary(static pair => pair.Key, static pair => Sample(pair.Key));

        // A new column changes every record's length: images encoded before it are discarded.
        Run(db, "ALTER TABLE ref_samples ADD COLUMN Extra INT");
        Check(db, expected);
        for (int i = 501; i <= 600; i++)
        {
            expected[i] = Sample(i);
            db.Insert(expected[i]);
        }

        Run(db, "DELETE FROM ref_samples WHERE Id = 3");
        expected.Remove(3);
        Check(db, expected);

        db.RestoreSnapshot(snapshot);
        Check(db, atSnapshot);
        db.Insert(atSnapshot[501] = Sample(501));
        Check(db, atSnapshot);
    }

    [Fact]
    public void Scans_StayCorrect_WhileAnotherThreadWrites()
    {
        using DataVoContext db = Open();
        db.EnsureRefSampleTable();
        for (int i = 1; i <= 2_000; i++)
        {
            db.Insert(Sample(i));
        }

        DataVoTable<RefSample> table = db.Table<RefSample>();
        int before = table.Where(static r => r.Alive).Count();
        Assert.Equal(Enumerable.Range(1, 2_000).Count(static i => Sample(i).Alive), before);

        // The writer adds rows that are never alive, so the count of alive rows must not move while it runs.
        using var stop = new CancellationTokenSource();
        Exception? failure = null;
        var writer = new Thread(() =>
        {
            try
            {
                for (int i = 100_000; !stop.IsCancellationRequested && i < 103_000; i++)
                {
                    db.Insert(new RefSample { Id = i, Name = "w", Level = 1, Gold = 1, Ratio = 1, Alive = false });
                    if (i % 3 == 0)
                    {
                        Run(db, $"DELETE FROM ref_samples WHERE Id = {i}");
                    }
                }
            }
            catch (Exception exception)
            {
                failure = exception;
            }
        });
        writer.Start();
        try
        {
            for (int pass = 0; pass < 400 && writer.IsAlive; pass++)
            {
                Assert.Equal(before, table.Where(static r => r.Alive).Count());
                Assert.Equal(1L, table.Where(static r => r.Alive).Min(static r => r.Gold) - Enumerable.Range(1, 2_000).Select(Sample).Where(static r => r.Alive).Min(static r => r.Gold) + 1);
            }
        }
        finally
        {
            stop.Cancel();
            writer.Join();
        }

        Assert.Null(failure);
        Assert.Equal(before, table.Where(static r => r.Alive).Count());
    }

    [Fact]
    public void TypedRows_WithTextAheadOfTheColumnsRead_AreScannedWithoutAllocating()
    {
        using DataVoContext db = Open();
        db.EnsureRefSampleTable();
        var batch = new List<CellValue[]>();
        var rows = new List<RefSample>();
        for (int i = 1; i <= 6_000; i++)
        {
            rows.Add(Sample(i));
            batch.Add(Cells(rows[^1]));
        }

        db.InsertTypedBatch("ref_samples", Schema, batch);
        DataVoTable<RefSample> table = db.Table<RefSample>();
        RefSample[] alive = [.. rows.Where(static r => r.Alive)];
        int count = alive.Length;
        long gold = alive.Sum(static r => r.Gold);
        double ratio = alive.Max(static r => r.Ratio);
        int? bonus = alive.Min(static r => r.Bonus);

        long quietest = long.MaxValue;
        for (int pass = 0; pass < 6; pass++)
        {
            long start = GC.GetAllocatedBytesForCurrentThread();
            var query = table.Where(static r => r.Alive);
            bool same = query.Count() == count
                && query.Sum(static r => r.Gold) == gold
                && query.Max(static r => r.Ratio) == ratio
                && query.Min(static r => r.Bonus) == bonus
                && query.Any();
            long allocated = GC.GetAllocatedBytesForCurrentThread() - start;

            Assert.True(same);
            if (pass > 0)
            {
                quietest = Math.Min(quietest, allocated);
            }
        }

        Assert.Equal(0, quietest);
    }

    /// <summary>Every compiled scan shape, against the rows the table should hold.</summary>
    private static void Check(DataVoContext db, Dictionary<int, RefSample> expected)
    {
        DataVoTable<RefSample> table = db.Table<RefSample>();
        RefSample[] all = [.. expected.Values];
        RefSample[] alive = [.. all.Where(static r => r.Alive)];
        var query = table.Where(static r => r.Alive);
        Assert.True(query.IsCompiled);

        Assert.Equal(alive.Length, query.Count());
        Assert.Equal(alive.Length > 0, query.Any());
        Assert.Equal(alive.Sum(static r => r.Gold), query.Sum(static r => r.Gold));
        Assert.Equal(alive.Sum(static r => r.Ratio), query.Sum(static r => r.Ratio), 6);
        Assert.Equal(alive.Min(static r => r.Bonus), query.Min(static r => r.Bonus));
        Assert.Equal(alive.Max(static r => r.Bonus), query.Max(static r => r.Bonus));
        if (alive.Length > 0)
        {
            Assert.Equal(alive.Min(static r => r.Level), query.Min(static r => r.Level));
            Assert.Equal(alive.Max(static r => r.Gold), query.Max(static r => r.Gold));
            Assert.Equal(alive.Max(static r => r.Ratio), query.Max(static r => r.Ratio));
        }
        else
        {
            Assert.Throws<InvalidOperationException>(() => query.Min(static r => r.Level));
        }

        // Conditions over a nullable column: a comparison, a null test, and both joined.
        Assert.Equal(all.Count(static r => r.Bonus > 100), table.Where(static r => r.Bonus > 100).Count());
        Assert.Equal(all.Count(static r => r.Bonus == null), table.Where(static r => r.Bonus == null).Count());
        Assert.Equal(all.Count(static r => r.Bonus != null && r.Level >= 0), table.Where(static r => r.Bonus != null && r.Level >= 0).Count());
        Assert.Equal(all.Where(static r => r.Bonus == null || r.Ratio > 0.5).Sum(static r => r.Gold), table.Where(static r => r.Bonus == null || r.Ratio > 0.5).Sum(static r => r.Gold));
        Assert.Equal(all.Count(static r => r.Level >= 10 && r.Gold < 0), table.Where((Level: 10, Gold: 0L), static (r, s) => r.Level >= s.Level && r.Gold < s.Gold).Count());

        // And the rows themselves, read the ordinary way.
        Assert.Equal(all.Select(static r => r.Id).Order(), table.Where(static r => r.Level > -100_000).ToList().Select(static r => r.Id).Order());
        Assert.Equal(all.Where(static r => r.Alive).Select(static r => r.Name).Order(StringComparer.Ordinal), query.ToList().Select(static r => r.Name).Order(StringComparer.Ordinal));
    }

    private static RefSample Sample(int i) => new()
    {
        Id = i,
        Name = i % 4 == 0 ? $"näme-{i}-✓" : $"name-{i}",
        Level = ((i * 37) % 201) - 100,
        Gold = ((((long)i * 1_000_003L) % 2_000_001L) - 1_000_000L) * 3_000L,
        Ratio = ((i * 7_919) % 10_007) / 10_007.0,
        Bonus = i % 3 == 0 ? null : (i % 977) - 400,
        Alive = i % 5 != 0,
    };

    private static CellValue[] Cells(RefSample row) =>
    [
        CellValue.From(row.Id),
        CellValue.From(row.Name),
        CellValue.From(row.Level),
        CellValue.From(row.Gold),
        CellValue.From(row.Ratio),
        row.Bonus is { } bonus ? CellValue.From(bonus) : CellValue.Null,
        CellValue.From(row.Alive),
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
