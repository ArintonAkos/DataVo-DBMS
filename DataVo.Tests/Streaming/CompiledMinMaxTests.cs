using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Streaming;

[DataVoTable("ref_readings")]
public sealed class RefReading
{
    [PrimaryKey] public int Id { get; set; }

    public int Level { get; set; }

    public long Score { get; set; }

    public float Health { get; set; }

    public double Ratio { get; set; }

    public DataVoDate Day { get; set; }

    public DateOnly Born { get; set; }

    public bool Alive { get; set; }

    public int? Bonus { get; set; }

    public long? Stamp { get; set; }

    public float? Trim { get; set; }

    public double? Drift { get; set; }

    public DataVoDate? Due { get; set; }

    public DateOnly? Gone { get; set; }

    public string Name { get; set; } = "";
}

[DataVoTable("ref_floats")]
public sealed class RefFloat
{
    [PrimaryKey] public int Id { get; set; }

    public int Batch { get; set; }

    public float Single { get; set; }

    public double Double { get; set; }

    public double? Maybe { get; set; }
}

/// <summary>
/// <c>Min</c> and <c>Max</c> over a compiled query return what LINQ returns over the same rows in memory, in every
/// storage mode, whether the scan finds the value itself or the rows are read.
/// </summary>
public sealed class CompiledMinMaxTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_minmax_{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [InlineData(StorageMode.Lsm, false)]
    [InlineData(StorageMode.Lsm, true)]
    [InlineData(StorageMode.InMemory, false)]
    [InlineData(StorageMode.Disk, false)]
    public void MinAndMax_OfEveryColumnType_MatchTheRowsInMemory(StorageMode mode, bool flushed)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefReadingTable();
        int count = mode == StorageMode.Disk ? 4_200 : 9_500; // more than two blocks of the in-place scan
        var rows = new List<RefReading>();
        Run(db, "BEGIN");
        for (int i = 1; i <= count; i++)
        {
            rows.Add(Reading(i));
            db.Insert(rows[^1]);
        }

        Run(db, "COMMIT");

        if (flushed)
        {
            db.Engine.StorageContext.LsmEngine!.FlushTableForTesting(db.Engine.Sessions.Get(db.SessionId)!, "ref_readings");
            Run(db, "UPDATE ref_readings SET Level = -777, Score = 99000000000 WHERE Id = 10");
            Run(db, "DELETE FROM ref_readings WHERE Id = 11");
            rows[9].Level = -777;
            rows[9].Score = 99_000_000_000;
            rows.RemoveAt(10);
        }

        DataVoTable<RefReading> table = db.Table<RefReading>();
        RefReading[] alive = [.. rows.Where(static r => r.Alive)];
        var query = table.Where(static r => r.Alive);
        Assert.True(query.IsCompiled);

        Assert.Equal(alive.Min(static r => r.Level), query.Min(static r => r.Level));
        Assert.Equal(alive.Max(static r => r.Level), query.Max(static r => r.Level));
        Assert.Equal(alive.Min(static r => r.Id), query.Min(static r => r.Id));
        Assert.Equal(alive.Max(static r => r.Id), query.Max(static r => r.Id));
        Assert.Equal(alive.Min(static r => r.Score), query.Min(static r => r.Score));
        Assert.Equal(alive.Max(static r => r.Score), query.Max(static r => r.Score));
        Assert.Equal(alive.Min(static r => r.Health), query.Min(static r => r.Health));
        Assert.Equal(alive.Max(static r => r.Health), query.Max(static r => r.Health));
        Assert.Equal(alive.Min(static r => r.Ratio), query.Min(static r => r.Ratio));
        Assert.Equal(alive.Max(static r => r.Ratio), query.Max(static r => r.Ratio));
        Assert.Equal(alive.Min(static r => r.Day), query.Min(static r => r.Day));
        Assert.Equal(alive.Max(static r => r.Day), query.Max(static r => r.Day));
        Assert.Equal(alive.Min(static r => r.Born), query.Min(static r => r.Born));
        Assert.Equal(alive.Max(static r => r.Born), query.Max(static r => r.Born));

        // Nullable columns: NULL cells are skipped.
        Assert.Equal(alive.Min(static r => r.Bonus), query.Min(static r => r.Bonus));
        Assert.Equal(alive.Max(static r => r.Bonus), query.Max(static r => r.Bonus));
        Assert.Equal(alive.Min(static r => r.Stamp), query.Min(static r => r.Stamp));
        Assert.Equal(alive.Max(static r => r.Stamp), query.Max(static r => r.Stamp));
        Assert.Equal(alive.Min(static r => r.Trim), query.Min(static r => r.Trim));
        Assert.Equal(alive.Max(static r => r.Trim), query.Max(static r => r.Trim));
        Assert.Equal(alive.Min(static r => r.Drift), query.Min(static r => r.Drift));
        Assert.Equal(alive.Max(static r => r.Drift), query.Max(static r => r.Drift));
        Assert.Equal(alive.Min(static r => r.Due), query.Min(static r => r.Due));
        Assert.Equal(alive.Max(static r => r.Due), query.Max(static r => r.Due));
        Assert.Equal(alive.Min(static r => r.Gone), query.Min(static r => r.Gone));
        Assert.Equal(alive.Max(static r => r.Gone), query.Max(static r => r.Gone));

        // A condition with a value, over a nullable column, and one that reads a column after the folded one.
        var some = table.Where(0.25, static (r, ratio) => r.Ratio > ratio && r.Bonus != null);
        RefReading[] expected = [.. rows.Where(static r => r.Ratio > 0.25 && r.Bonus != null)];
        Assert.Equal(expected.Min(static r => r.Score), some.Min(static r => r.Score));
        Assert.Equal(expected.Max(static r => r.Bonus), some.Max(static r => r.Bonus));
        Assert.Equal(expected.Max(static r => r.Day), some.Max(static r => r.Day));

        // No matching row: an exception for a value that cannot be null, null for one that can.
        var none = table.Where(static r => r.Level > 100_000);
        Assert.Throws<InvalidOperationException>(() => none.Min(static r => r.Level));
        Assert.Throws<InvalidOperationException>(() => none.Max(static r => r.Ratio));
        Assert.Throws<InvalidOperationException>(() => none.Max(static r => r.Day));
        Assert.Null(none.Min(static r => r.Bonus));
        Assert.Null(none.Max(static r => r.Due));
        Assert.Null(none.Max(static r => r.Gone));

        // Rows match but every value is NULL.
        var blank = table.Where(static r => r.Bonus == null);
        Assert.Null(blank.Min(static r => r.Bonus));
        Assert.Null(blank.Max(static r => r.Bonus));
        Assert.Equal(rows.Where(static r => r.Bonus == null).Min(static r => r.Level), blank.Min(static r => r.Level));

        // A computed selector, a widened one, a text one, a condition the scan cannot compile, and a pinned key
        // all go through the rows.
        Assert.Equal(alive.Min(static r => r.Level * 2), query.Min(static r => r.Level * 2));
        Assert.Equal(alive.Max(static r => (long)r.Level), query.Max(static r => (long)r.Level));
        Assert.Equal(alive.Max(static r => r.Bonus ?? 0), query.Max(static r => r.Bonus ?? 0));
        Assert.Equal(alive.Max(static r => r.Name), query.Max(static r => r.Name));
        Assert.Equal(rows.Where(static r => r.Name == "r-7").Max(static r => r.Score), table.Where(static r => r.Name == "r-7").Max(static r => r.Score));
        Assert.Equal(rows[4].Score, table.Where(static r => r.Id == 5).Min(static r => r.Score));
        Assert.Throws<InvalidOperationException>(() => table.Where(static r => r.Id == -5).Min(static r => r.Score));
        Assert.Null(table.Where(static r => r.Id == -5).Min(static r => r.Stamp));

        // Inside a transaction the result includes the transaction's own writes, and a rollback undoes them.
        int lowest = alive.Min(static r => r.Level);
        long highest = alive.Max(static r => r.Score);
        Run(db, "BEGIN");
        Run(db, "UPDATE ref_readings SET Level = -5000 WHERE Id = 1");
        db.Insert(new RefReading { Id = count + 1, Alive = true, Score = long.MaxValue, Day = DataVoDate.FromDayNumber(1), Born = new DateOnly(1900, 1, 1), Name = "new" });
        Assert.Equal(-5000, query.Min(static r => r.Level));
        Assert.Equal(long.MaxValue, query.Max(static r => r.Score));
        Assert.Equal(DataVoDate.FromDayNumber(1), query.Min(static r => r.Day));
        Assert.Equal(new DateOnly(1900, 1, 1), query.Min(static r => r.Born));
        Run(db, "ROLLBACK");
        Assert.Equal(lowest, query.Min(static r => r.Level));
        Assert.Equal(highest, query.Max(static r => r.Score));
        Assert.Equal(alive.Min(static r => r.Day), query.Min(static r => r.Day));

        Run(db, "BEGIN");
        Run(db, "DELETE FROM ref_readings WHERE Id > 0");
        Assert.Throws<InvalidOperationException>(() => query.Min(static r => r.Level));
        Assert.Null(query.Max(static r => r.Bonus));
        Run(db, "ROLLBACK");
        Assert.Equal(lowest, query.Min(static r => r.Level));
    }

    [Theory]
    [InlineData(StorageMode.Lsm)]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Disk)]
    public void MinAndMax_OverAnEmptyTable_ThrowOrReturnNull(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefReadingTable();
        var query = db.Table<RefReading>().Where(static r => r.Alive);

        for (int pass = 0; pass < 2; pass++)
        {
            Assert.Throws<InvalidOperationException>(() => query.Min(static r => r.Level));
            Assert.Throws<InvalidOperationException>(() => query.Max(static r => r.Score));
            Assert.Throws<InvalidOperationException>(() => query.Min(static r => r.Health));
            Assert.Throws<InvalidOperationException>(() => query.Max(static r => r.Ratio));
            Assert.Throws<InvalidOperationException>(() => query.Min(static r => r.Day));
            Assert.Throws<InvalidOperationException>(() => query.Max(static r => r.Born));
            Assert.Throws<InvalidOperationException>(() => query.Max(static r => r.Level + 1));
            Assert.Null(query.Min(static r => r.Bonus));
            Assert.Null(query.Max(static r => r.Stamp));
            Assert.Null(query.Min(static r => r.Trim));
            Assert.Null(query.Max(static r => r.Drift));
            Assert.Null(query.Min(static r => r.Due));
            Assert.Null(query.Max(static r => r.Gone));
            Assert.Null(query.Max(static r => r.Name));
            Assert.Throws<ArgumentNullException>(() => query.Min<int>(null!));

            if (pass == 0)
            {
                Run(db, "BEGIN"); // and the same inside a transaction, where rows are streamed
            }
            else
            {
                Run(db, "ROLLBACK");
            }
        }
    }

    [Theory]
    [InlineData(StorageMode.Lsm, false)]
    [InlineData(StorageMode.Lsm, true)]
    [InlineData(StorageMode.InMemory, false)]
    [InlineData(StorageMode.Disk, false)]
    public void NaN_IsBelowEveryNumber_AsItIsForLinq(StorageMode mode, bool flushed)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefFloatTable();
        RefFloat[] rows =
        [
            new() { Id = 1, Batch = 1, Single = 2f, Double = 2, Maybe = 5 },
            new() { Id = 2, Batch = 1, Single = float.NaN, Double = double.NaN, Maybe = double.NaN },
            new() { Id = 3, Batch = 1, Single = -3f, Double = -3, Maybe = null },
            new() { Id = 4, Batch = 1, Single = float.PositiveInfinity, Double = double.NegativeInfinity, Maybe = -1 },
            new() { Id = 5, Batch = 2, Single = float.NaN, Double = double.NaN, Maybe = double.NaN },
            new() { Id = 6, Batch = 2, Single = float.NaN, Double = double.NaN, Maybe = null },
            new() { Id = 7, Batch = 3, Single = float.NaN, Double = double.NaN, Maybe = double.NaN }, // NaN first, numbers after
            new() { Id = 8, Batch = 3, Single = -0f, Double = 1e300, Maybe = 4 },
            new() { Id = 9, Batch = 3, Single = float.MinValue, Double = double.MaxValue, Maybe = 9 },
        ];
        foreach (RefFloat row in rows)
        {
            db.Insert(row);
        }

        if (flushed)
        {
            db.Engine.StorageContext.LsmEngine!.FlushTableForTesting(db.Engine.Sessions.Get(db.SessionId)!, "ref_floats");
        }

        DataVoTable<RefFloat> table = db.Table<RefFloat>();
        for (int batch = 1; batch <= 3; batch++)
        {
            RefFloat[] expected = [.. rows.Where(r => r.Batch == batch)];
            var query = table.Where(batch, static (r, b) => r.Batch == b);

            Assert.Equal(expected.Min(static r => r.Single), query.Min(static r => r.Single));
            Assert.Equal(expected.Max(static r => r.Single), query.Max(static r => r.Single));
            Assert.Equal(expected.Min(static r => r.Double), query.Min(static r => r.Double));
            Assert.Equal(expected.Max(static r => r.Double), query.Max(static r => r.Double));
            Assert.Equal(expected.Min(static r => r.Maybe), query.Min(static r => r.Maybe));
            Assert.Equal(expected.Max(static r => r.Maybe), query.Max(static r => r.Maybe));

            // The same through the rows.
            Assert.Equal(expected.Min(static r => r.Double + 0), query.Min(static r => r.Double + 0));
            Assert.Equal(expected.Max(static r => r.Single + 0), query.Max(static r => r.Single + 0));
        }

        Assert.True(float.IsNaN(table.Where(static r => r.Batch == 1).Min(static r => r.Single)));
        Assert.Equal(float.PositiveInfinity, table.Where(static r => r.Batch == 1).Max(static r => r.Single));
        Assert.True(double.IsNaN(table.Where(static r => r.Batch == 2).Max(static r => r.Double)));
        Assert.Equal(double.MaxValue, table.Where(static r => r.Batch == 3).Max(static r => r.Double));
    }

    [Theory]
    [InlineData(StorageMode.Lsm, false)]
    [InlineData(StorageMode.Lsm, true)]
    [InlineData(StorageMode.InMemory, false)]
    public void CompiledMinAndMax_AllocateNothingOnceWarm(StorageMode mode, bool flushed)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefReadingTable();
        var rows = new List<RefReading>();
        Run(db, "BEGIN");
        for (int i = 1; i <= 6_000; i++)
        {
            rows.Add(Reading(i));
            db.Insert(rows[^1]);
        }

        Run(db, "COMMIT");
        if (flushed)
        {
            db.Engine.StorageContext.LsmEngine!.FlushTableForTesting(db.Engine.Sessions.Get(db.SessionId)!, "ref_readings");
        }

        DataVoTable<RefReading> table = db.Table<RefReading>();
        RefReading[] alive = [.. rows.Where(static r => r.Alive)];
        int level = alive.Min(static r => r.Level);
        long score = alive.Max(static r => r.Score);
        float health = alive.Min(static r => r.Health);
        double ratio = alive.Max(static r => r.Ratio);
        DataVoDate day = alive.Min(static r => r.Day);
        DateOnly born = alive.Max(static r => r.Born);
        int? bonus = alive.Min(static r => r.Bonus);
        DataVoDate? due = alive.Max(static r => r.Due);

        long quietest = long.MaxValue;
        for (int pass = 0; pass < 6; pass++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            var query = table.Where(static r => r.Alive);
            bool same = query.Min(static r => r.Level) == level
                && query.Max(static r => r.Score) == score
                && query.Min(static r => r.Health) == health
                && query.Max(static r => r.Ratio) == ratio
                && query.Min(static r => r.Day) == day
                && query.Max(static r => r.Born) == born
                && query.Min(static r => r.Bonus) == bonus
                && query.Max(static r => r.Due) == due;
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.True(same);
            if (pass > 0)
            {
                quietest = Math.Min(quietest, allocated);
            }
        }

        Assert.Equal(0, quietest);
    }

    private static RefReading Reading(int i) => new()
    {
        Id = i,
        Level = ((i * 37) % 201) - 100,
        Score = ((((long)i * 1_000_003L) % 2_000_001L) - 1_000_000L) * 5_000L,
        Health = ((i * 0.37f) % 91f) - 45f,
        Ratio = Math.Sin(i) * 1e6,
        Day = DataVoDate.FromDayNumber(730_000 + ((i * 7919) % 5_000)),
        Born = DateOnly.FromDayNumber(700_000 + ((i * 104_729) % 30_000)),
        Alive = i % 3 != 0,
        Bonus = i % 2 == 0 ? null : -(i % 977),
        Stamp = i % 5 == 0 ? null : (((long)i * 7_777_777L) % 1_000_003L) - 500_000L,
        Trim = i % 7 == 0 ? null : ((i * 1.3f) % 17f) - 8f,
        Drift = i % 4 == 0 ? null : Math.Cos(i) * 1e-3,
        Due = i % 3 == 1 ? null : DataVoDate.FromDayNumber(735_000 + ((i * 31) % 900)),
        Gone = i % 6 == 0 ? null : DateOnly.FromDayNumber(736_000 + ((i * 17) % 1_200)),
        Name = $"r-{i}",
    };

    private static QueryResult Run(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result;
    }

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = _directory });
        Run(db, "CREATE DATABASE S");
        Run(db, "USE S");
        return db;
    }
}
