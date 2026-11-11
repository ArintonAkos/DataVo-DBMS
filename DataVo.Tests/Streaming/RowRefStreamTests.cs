using System.Numerics;
using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Streaming;

[DataVoTable("ref_units")]
public sealed class RefUnit
{
    [PrimaryKey] public int Id { get; set; }

    public float Health { get; set; }

    public long Score { get; set; }

    public double Ratio { get; set; }

    public bool Alive { get; set; }

    public Guid Tag { get; set; }

    public string Name { get; set; } = "";

    public Vector3 Position { get; set; }

    public string? Nickname { get; set; }

    public int? Bonus { get; set; }
}

[DataVoTable("ref_tokens")]
public sealed class RefToken
{
    [PrimaryKey] public Guid Id { get; set; }

    public int Value { get; set; }
}

/// <summary>
/// <see cref="RowRef"/> streams read every column type exactly as the model path reads it, in every storage mode,
/// page after page, and a warmed-up pass over 50,000 rows allocates nothing.
/// </summary>
public sealed class RowRefStreamTests : IDisposable
{
    private const int Id = 0, Health = 1, Score = 2, Ratio = 3, Alive = 4, Tag = 5, Name = 6, Position = 7, Nickname = 8, Bonus = 9;

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_rowref_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Disk, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void EveryAccessor_MatchesTheModelRead_AcrossSeveralPages(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefUnitTable();
        const int Count = 2_600; // three pages, the last one partial
        for (int i = 1; i <= Count; i++)
        {
            db.Insert(Unit(i));
        }

        Dictionary<int, RefUnit> expected = db.Table<RefUnit>().ToList().ToDictionary(static unit => unit.Id);
        Assert.Equal(Count, expected.Count);

        int seen = 0;
        int previous = int.MinValue;
        foreach (RowRef row in db.Table<RefUnit>().StreamRef())
        {
            Assert.Equal(10, row.ColumnCount);
            int id = row.GetInt32(Id);
            Assert.True(id > previous, "rows arrive in key order, each once");
            previous = id;
            RefUnit unit = expected[id];

            Assert.False(row.IsNull(Id));
            Assert.Equal(BitConverter.SingleToInt32Bits(unit.Health), BitConverter.SingleToInt32Bits(row.GetFloat(Health)));
            Assert.Equal((double)unit.Health, row.GetDouble(Health));
            Assert.Equal(unit.Score, row.GetInt64(Score));
            Assert.Equal(unit.Ratio, row.GetDouble(Ratio));
            Assert.Equal(unit.Alive, row.GetBoolean(Alive));
            Assert.Equal(unit.Tag, row.GetGuid(Tag));
            Assert.Equal(unit.Name, row.GetStringSpan(Name).ToString());
            Assert.Equal(unit.Name, System.Text.Encoding.UTF8.GetString(row.GetStringUtf8(Name)));
            Assert.Equal(unit.Position, row.GetVector3(Position));
            Assert.Equal(12, row.GetVectorBytes(Position).Length);
            Assert.Equal(unit.Nickname is null, row.IsNull(Nickname));
            if (unit.Nickname is not null)
            {
                Assert.Equal(unit.Nickname, row.GetStringSpan(Nickname).ToString());

                // Two text cells of one row stay readable side by side.
                Assert.Equal(unit.Name, row.GetStringSpan(Name).ToString());
            }

            Assert.Equal(unit.Bonus is null, row.IsNull(Bonus));
            if (unit.Bonus is { } bonus)
            {
                Assert.Equal(bonus, row.GetInt32(Bonus));
                Assert.Equal(bonus, row.GetInt64(Bonus));
            }

            // The single-call readers agree with IsNull and the checked accessors, on the key column too.
            Assert.True(row.TryGetInt32(Id, out int keyValue));
            Assert.Equal(id, keyValue);
            Assert.True(row.TryGetInt64(Id, out long wideKey));
            Assert.Equal(id, wideKey);
            Assert.True(row.TryGetFloat(Health, out float health));
            Assert.Equal(row.GetFloat(Health), health);
            Assert.True(row.TryGetDouble(Health, out double wideHealth));
            Assert.Equal(row.GetDouble(Health), wideHealth);
            Assert.True(row.TryGetInt64(Score, out long score));
            Assert.Equal(unit.Score, score);
            Assert.True(row.TryGetDouble(Ratio, out double ratio));
            Assert.Equal(unit.Ratio, ratio);
            Assert.True(row.TryGetBoolean(Alive, out bool alive));
            Assert.Equal(unit.Alive, alive);
            Assert.True(row.TryGetGuid(Tag, out Guid tag));
            Assert.Equal(unit.Tag, tag);
            Assert.Equal(unit.Bonus is not null, row.TryGetInt32(Bonus, out int bonusValue));
            Assert.Equal(unit.Bonus ?? 0, bonusValue);
            Assert.Equal(unit.Bonus is not null, row.TryGetInt64(Bonus, out long wideBonus));
            Assert.Equal(unit.Bonus ?? 0, wideBonus);

            seen++;
        }

        Assert.Equal(Count, seen);
    }

    [Theory]
    [InlineData(StorageMode.Lsm, false)]
    [InlineData(StorageMode.Lsm, true)]
    [InlineData(StorageMode.InMemory, false)]
    [InlineData(StorageMode.Disk, false)]
    public void CompiledCount_TestsRowsWhereTheyAreStored_AcrossSeveralBatches(StorageMode mode, bool flushed)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefUnitTable();
        const int Count = 9_500; // more than two batches of the in-place scan
        var units = new List<RefUnit>();
        for (int i = 1; i <= Count; i++)
        {
            units.Add(Unit(i));
            db.Insert(units[^1]);
        }

        if (flushed)
        {
            db.Engine.StorageContext.LsmEngine!.FlushTableForTesting(db.Engine.Sessions.Get(db.SessionId)!, "ref_units");

            // Rows written after the flush are merged with the flushed file during the scan.
            Run(db, "UPDATE ref_units SET Score = -1 WHERE Id = 10");
            Run(db, "DELETE FROM ref_units WHERE Id = 11");
            units[9].Score = -1;
            units.RemoveAt(10);
        }

        DataVoTable<RefUnit> table = db.Table<RefUnit>();
        long median = units[units.Count / 2].Score;

        Assert.Equal(units.Count(u => u.Score > median), table.Where(median, static (u, score) => u.Score > score).Count());
        Assert.Equal(units.Count(static u => u.Alive && u.Bonus != null), table.Where(static u => u.Alive && u.Bonus != null).Count());
        Assert.Equal(units.Count(static u => u.Nickname == null || u.Ratio < 0.5), table.Where(static u => u.Nickname == null || u.Ratio < 0.5).Count());
        Assert.Equal(units.Count(static u => u.Bonus != 3), table.Where(static u => u.Bonus != 3).Count());
        Assert.Equal(units.Count(static u => u.Health >= 50f), table.Where(static u => u.Health >= 50f).Count());
        Assert.True(table.Where(static u => u.Score == -1 || u.Health >= 0f).Any());
        Assert.False(table.Where(long.MaxValue, static (u, score) => u.Score == score).Any());
        Assert.True(table.Where(median, static (u, score) => u.Score > score).IsCompiled);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TextColumns_ReadTheSameWhetherWrittenBySqlOrByModel(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "CREATE TABLE notes (Id INT PRIMARY KEY, Body VARCHAR, Weight FLOAT, Big BIGINT, Ok BIT)");
        Run(db, "INSERT INTO notes (Id, Body, Weight, Big, Ok) VALUES (1, 'plain', 1.5, 9000000000, true), (2, 'it''s quoted', -0.25, -1, false), (3, '', 0.0, 0, true), (4, NULL, NULL, NULL, NULL)");

        var expected = new Dictionary<int, Dictionary<string, object?>>();
        foreach (Dictionary<string, object?> row in Run(db, "SELECT Id, Body, Weight, Big, Ok FROM notes").Data)
        {
            expected[Convert.ToInt32(row["Id"])] = row;
        }

        RowRefStream notes = db.StreamRef("notes");
        int body = notes.GetOrdinal("body");
        int weight = notes.GetOrdinal("Weight");
        int big = notes.GetOrdinal("Big");
        int ok = notes.GetOrdinal("Ok");
        int seen = 0;
        foreach (RowRef row in notes)
        {
            Dictionary<string, object?> want = expected[row.GetInt32(0)];
            Assert.Equal(want["Body"] is null, row.IsNull(body));
            Assert.Equal(want["Weight"] is null, row.IsNull(weight));
            Assert.Equal(want["Big"] is null, row.IsNull(big));
            Assert.Equal(want["Ok"] is null, row.IsNull(ok));
            if (want["Body"] is string text)
            {
                Assert.Equal(text, row.GetStringSpan(body).ToString());
                Assert.Equal(Convert.ToSingle(want["Weight"]), row.GetFloat(weight));
                Assert.Equal(Convert.ToInt64(want["Big"]), row.GetInt64(big));
                Assert.Equal(Convert.ToBoolean(want["Ok"]), row.GetBoolean(ok));
            }

            seen++;
        }

        Assert.Equal(4, seen);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void GuidPrimaryKey_ReadsFromTheRowKey(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefTokenTable();
        var tokens = Enumerable.Range(0, 1_500).Select(static i => new RefToken { Id = Guid.NewGuid(), Value = i }).ToList();
        tokens.ForEach(db.Insert);

        var byId = tokens.ToDictionary(static token => token.Id);
        int seen = 0;
        foreach (RowRef row in db.Table<RefToken>().StreamRef())
        {
            Assert.False(row.IsNull(0));
            Guid id = row.GetGuid(0);
            Assert.Equal(byId[id].Value, row.GetInt32(1));
            Assert.Throws<InvalidCastException>(() => GetInt32Of(db, 0));
            seen++;
            break;
        }

        foreach (RowRef row in db.Table<RefToken>().StreamRef())
        {
            Assert.True(byId.Remove(row.GetGuid(0)));
            seen++;
        }

        Assert.Empty(byId);
        Assert.Equal(1 + tokens.Count, seen);
    }

    [Fact]
    public void WrongTypeNullAndOutOfRange_ThrowClearly()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        db.EnsureRefUnitTable();
        db.Insert(Unit(2)); // even ids have no nickname and no bonus

        foreach (RowRef row in db.Table<RefUnit>().StreamRef())
        {
            InvalidCastException cast = Throws<InvalidCastException>(row, static r => r.GetInt32(Health));
            Assert.Contains("FLOAT", cast.Message);
            Throws<InvalidCastException>(row, static r => r.GetFloat(Ratio));
            Throws<InvalidCastException>(row, static r => r.GetStringSpan(Id));
            Throws<InvalidCastException>(row, static r => r.GetVector3(Name));
            Throws<InvalidCastException>(row, static r => r.GetInt64(Ratio));
            InvalidOperationException isNull = Throws<InvalidOperationException>(row, static r => r.GetStringSpan(Nickname));
            Assert.Contains("NULL", isNull.Message);
            Throws<InvalidOperationException>(row, static r => r.GetInt32(Bonus));
            Throws<ArgumentOutOfRangeException>(row, static r => r.IsNull(10));
            Throws<ArgumentOutOfRangeException>(row, static r => r.GetInt32(-1));
        }

        Assert.Throws<ArgumentException>(() => db.Table<RefUnit>().StreamRef().GetOrdinal("Missing"));
        Assert.Equal(Position, db.Table<RefUnit>().StreamRef().GetOrdinal("position"));
        Assert.Throws<ArgumentException>(() => db.StreamRef(""));
        Assert.Throws<InvalidOperationException>(() => Drain(db.StreamRef("no_such_table")));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WritersRunBetweenRows_BecauseNoLockIsHeldOutsideAPageRead(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefUnitTable();
        for (int i = 1; i <= 2_100; i++)
        {
            db.Insert(Unit(i));
        }

        int seen = 0;
        foreach (RowRef row in db.Table<RefUnit>().StreamRef())
        {
            if (seen == 5)
            {
                // A write lock taken while a read lock is held on this thread would throw (no recursion is allowed).
                Run(db, "UPDATE ref_units SET Score = 1 WHERE Id = 2000");
                db.Insert(Unit(5_000));
            }

            Assert.True(row.GetInt32(Id) > 0);
            seen++;
        }

        Assert.InRange(seen, 2_100, 2_101); // the row written mid-pass may or may not be seen
        Assert.Equal(1, db.Table<RefUnit>().FirstOrDefault(static u => u.Id == 2000)!.Score);
    }

    [Fact]
    public void SchemaChangeDuringAPass_StopsIt()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        Run(db, "CREATE TABLE shapes (Id INT PRIMARY KEY, Size INT)");
        Run(db, $"INSERT INTO shapes (Id, Size) VALUES {string.Join(", ", Enumerable.Range(1, 1_500).Select(static i => $"({i}, {i})"))}");

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() =>
        {
            int seen = 0;
            foreach (RowRef _ in db.StreamRef("shapes"))
            {
                if (++seen == 10)
                {
                    Run(db, "ALTER TABLE shapes ADD COLUMN Extra INT");
                }
            }
        });
        Assert.Contains("changed shape", error.Message);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InsideATransaction_ThePassSeesItsOwnUncommittedWrites(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRefUnitTable();
        for (int i = 1; i <= 3; i++)
        {
            db.Insert(Unit(i));
        }

        Run(db, "BEGIN TRANSACTION");
        Run(db, "INSERT INTO ref_units (Id, Health, Score, Ratio, Alive, Tag, Name, Position, Nickname, Bonus) VALUES (99, 2.5, 7, 0.5, true, '00000000-0000-0000-0000-000000000063', 'pending', '[1,2,3]', NULL, 4)");
        Run(db, "DELETE FROM ref_units WHERE Id = 1");

        var ids = new List<int>();
        foreach (RowRef row in db.Table<RefUnit>().StreamRef())
        {
            ids.Add(row.GetInt32(Id));
            if (row.GetInt32(Id) == 99)
            {
                Assert.Equal("pending", row.GetStringSpan(Name).ToString());
                Assert.Equal(2.5f, row.GetFloat(Health));
                Assert.Equal(new Vector3(1, 2, 3), row.GetVector3(Position));
                Assert.Equal(new Guid("00000000-0000-0000-0000-000000000063"), row.GetGuid(Tag));
                Assert.True(row.IsNull(Nickname));
                Assert.Equal(4, row.GetInt32(Bonus));
            }
        }

        Assert.Equal([2, 3, 99], ids.Order());
        Run(db, "ROLLBACK");

        ids.Clear();
        foreach (RowRef row in db.Table<RefUnit>().StreamRef())
        {
            ids.Add(row.GetInt32(Id));
        }

        Assert.Equal([1, 2, 3], ids);
    }

    [Fact]
    public void NestedPasses_OverTwoTables_DoNotDisturbEachOther()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsureRefUnitTable();
        db.EnsureRefTokenTable();
        for (int i = 1; i <= 1_100; i++)
        {
            db.Insert(Unit(i));
        }

        db.Insert(new RefToken { Id = Guid.NewGuid(), Value = 7 });
        db.Insert(new RefToken { Id = Guid.NewGuid(), Value = 8 });

        long sum = 0;
        int outer = 0;
        foreach (RowRef unit in db.Table<RefUnit>().StreamRef())
        {
            int id = unit.GetInt32(Id);
            if (id % 500 == 0)
            {
                foreach (RowRef token in db.Table<RefToken>().StreamRef())
                {
                    sum += token.GetInt32(1);
                }
            }

            Assert.Equal(id, unit.GetInt32(Id));
            Assert.Equal(Unit(id).Name, unit.GetStringSpan(Name).ToString());
            outer++;
        }

        Assert.Equal(1_100, outer);
        Assert.Equal(2 * 15, sum);
    }

    [Theory]
    [InlineData(StorageMode.Lsm, false)]
    [InlineData(StorageMode.Lsm, true)]
    [InlineData(StorageMode.InMemory, false)]
    [InlineData(StorageMode.Disk, false)]
    public void FullPassOver50000Rows_AllocatesZeroBytes(StorageMode mode, bool flushed)
    {
        const int Rows = 50_000;
        using DataVoContext db = Open(mode);
        db.EnsureRefUnitTable();
        for (int start = 1; start <= Rows; start += 1_000)
        {
            IEnumerable<string> values = Enumerable.Range(start, 1_000).Select(static id =>
                $"({id}, {id % 100}.5, {id * 3L}, 0.25, {(id % 2 == 1 ? "true" : "false")}, '{new Guid(id, 0, 0, new byte[8])}', 'unit {id}', '[{id}, 1, 2]', NULL, {id % 7})");
            Run(db, $"INSERT INTO ref_units (Id, Health, Score, Ratio, Alive, Tag, Name, Position, Nickname, Bonus) VALUES {string.Join(", ", values)}");
        }

        if (flushed)
        {
            db.Engine.StorageContext.LsmEngine!.FlushTableForTesting(db.Engine.Sessions.Get(db.SessionId)!, "ref_units");
        }

        DataVoTable<RefUnit> table = db.Table<RefUnit>();
        Assert.Equal(Checksum(Rows), Pass(table, out int warmCount)); // warm the page buffers, the JIT and the plan
        Assert.Equal(Rows, warmCount);

        // Anything the stream itself allocates, per row or per page, shows up in every pass. The runtime can also
        // charge a one-off allocation of its own to this thread while the rest of the suite runs beside it (seen
        // once on a CI runner: 4,184 bytes in one pass), so the quietest of several passes is the one asserted.
        long allocated = long.MaxValue;
        for (int pass = 0; pass < 5; pass++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            long checksum = Pass(table, out int count);
            allocated = Math.Min(allocated, GC.GetAllocatedBytesForCurrentThread() - before);

            Assert.Equal(Rows, count);
            Assert.Equal(Checksum(Rows), checksum);
        }

        if (mode == StorageMode.Disk)
        {
            // The legacy disk engine opens a FileStream for each page (~160 bytes; ~8 KB over 50,000 rows). Its
            // writers open the file with FileShare.None, so a handle kept open between pages would block them on
            // Windows. Rows themselves cost nothing: the bound is per page, not per row.
            Assert.True(allocated < 12_000, $"Disk engine allocated {allocated} bytes across {Rows} rows.");
        }
        else
        {
            Assert.Equal(0, allocated);
        }
    }

    private static long Pass(DataVoTable<RefUnit> table, out int count)
    {
        long checksum = 0;
        count = 0;
        foreach (RowRef row in table.StreamRef())
        {
            checksum += row.GetInt32(Id) + row.GetInt64(Score) + (long)row.GetFloat(Health) + (row.GetBoolean(Alive) ? 1 : 0);
            checksum += row.GetStringSpan(Name).Length + row.GetGuid(Tag).GetHashCode() * 0 + (long)row.GetVector3(Position).X;
            checksum += row.IsNull(Nickname) ? 1 : 0;
            count++;
        }

        return checksum;
    }

    private static long Checksum(int rows)
    {
        long checksum = 0;
        for (int id = 1; id <= rows; id++)
        {
            checksum += id + (id * 3L) + (id % 100) + (id % 2) + $"unit {id}".Length + id + 1;
        }

        return checksum;
    }

    private static RefUnit Unit(int i) => new()
    {
        Id = i,
        Health = (i * 0.37f) - 50f,
        Score = (long)i * 1_000_003L - 7,
        Ratio = i / 3.0,
        Alive = i % 3 != 0,
        Tag = new Guid(i, (short)(i >> 3), 7, [1, 2, 3, 4, 5, 6, 7, (byte)i]),
        Name = i % 5 == 0 ? $"ünït-{i}-🚀" : $"unit-{i}",
        Position = new Vector3(i, -i * 0.5f, 1f / i),
        Nickname = i % 2 == 0 ? null : $"nick ✓ {i}",
        Bonus = i % 2 == 0 ? null : i * 2,
    };

    private static int GetInt32Of(DataVoContext db, int column)
    {
        foreach (RowRef row in db.Table<RefToken>().StreamRef())
        {
            return row.GetInt32(column);
        }

        return 0;
    }

    private delegate void RowAction(RowRef row);

    private static TException Throws<TException>(RowRef row, RowAction action)
        where TException : Exception
    {
        try
        {
            action(row);
        }
        catch (TException expected)
        {
            return expected;
        }

        throw new Xunit.Sdk.XunitException($"Expected {typeof(TException).Name}.");
    }

    private static void Drain(RowRefStream stream)
    {
        foreach (RowRef _ in stream)
        {
        }
    }

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
