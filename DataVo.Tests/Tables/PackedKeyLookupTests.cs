using System.Numerics;
using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Tables;

[DataVoTable("packed_heroes")]
public sealed class PackedHero
{
    [PrimaryKey] public int Id { get; set; }

    public float Health { get; set; }

    public long Score { get; set; }

    public double Ratio { get; set; }

    public bool Alive { get; set; }

    public Guid Tag { get; set; }

    public DateOnly Born { get; set; }

    public string Name { get; set; } = "";

    public Vector3 Position { get; set; }

    public string? Nickname { get; set; }

    public int? Bonus { get; set; }

    public DataVoDate? Retired { get; set; }
}

/// <summary>
/// A lookup by integer primary key on an in-memory table builds the model straight from the packed row, and
/// gives the statement's answer whenever it cannot.
/// </summary>
public sealed class PackedKeyLookupTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_packed_{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private DataVoContext Open(StorageMode mode = StorageMode.InMemory)
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = _directory });
        db.Execute("CREATE DATABASE S");
        db.Execute("USE S");
        db.EnsurePackedHeroTable();
        return db;
    }

    private static PackedHero Hero(int i) => new()
    {
        Id = i,
        Health = (i * 0.37f) - 50f,
        Score = (long)i * 1_000_003L - 7,
        Ratio = i / 3.0,
        Alive = i % 3 != 0,
        Tag = new Guid(i, (short)(i >> 3), 7, [1, 2, 3, 4, 5, 6, 7, (byte)i]),
        Born = new DateOnly(1990 + (i % 30), 1 + (i % 12), 1 + (i % 28)),
        Name = i % 5 == 0 ? $"hérö-{i}-🚀" : $"hero-{i}",
        Position = new Vector3(i, -i * 0.5f, 1f / i),
        Nickname = i % 2 == 0 ? null : $"nick {i}",
        Bonus = i % 2 == 0 ? null : i * 2,
        Retired = i % 4 == 0 ? DataVoDate.FromDayNumber(730_000 + i) : null,
    };

    private static void AssertSame(PackedHero expected, PackedHero? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(BitConverter.SingleToInt32Bits(expected.Health), BitConverter.SingleToInt32Bits(actual.Health));
        Assert.Equal(expected.Score, actual.Score);
        Assert.Equal(expected.Ratio, actual.Ratio);
        Assert.Equal(expected.Alive, actual.Alive);
        Assert.Equal(expected.Tag, actual.Tag);
        Assert.Equal(expected.Born, actual.Born);
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.Position, actual.Position);
        Assert.Equal(expected.Nickname, actual.Nickname);
        Assert.Equal(expected.Bonus, actual.Bonus);
        Assert.Equal(expected.Retired?.DayNumber, actual.Retired?.DayNumber);
    }

    private static readonly ReactiveRowSchema Schema = new(
        "Id", "Health", "Score", "Ratio", "Alive", "Tag", "Born", "Name", "Position", "Nickname", "Bonus", "Retired");

    /// <summary>A hero as the typed cells of one row, in the order of <see cref="Schema"/>.</summary>
    private static CellValue[] Cells(PackedHero hero) =>
    [
        CellValue.From(hero.Id), CellValue.From((double)hero.Health), CellValue.From(hero.Score), CellValue.From(hero.Ratio),
        CellValue.From(hero.Alive), CellValue.From(hero.Tag), CellValue.From(DataVoDate.FromDateOnly(hero.Born)),
        CellValue.From(hero.Name), CellValue.From(new[] { hero.Position.X, hero.Position.Y, hero.Position.Z }),
        CellValue.From(hero.Nickname), hero.Bonus is { } bonus ? CellValue.From(bonus) : CellValue.Null,
        hero.Retired is { } retired ? CellValue.From(retired) : CellValue.Null,
    ];

    /// <summary>Inserts heroes 1 to <paramref name="count"/> as typed rows, which an in-memory table keeps packed.</summary>
    private static void Load(DataVoContext db, int count, ReactiveRowSchema? schema = null) =>
        db.InsertTypedBatch("packed_heroes", schema ?? Schema, [.. Enumerable.Range(1, count).Select(static i => Cells(Hero(i)))]);

    [Fact]
    public void Lookup_ReadsEveryColumnTypeStraightFromThePackedRow()
    {
        using DataVoContext db = Open();
        Load(db, 300);
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();

        for (int i = 1; i <= 300; i++)
        {
            Assert.True(heroes.TryReadByIntegerKey(i, out PackedHero direct), "the row is read without the statement");
            AssertSame(Hero(i), direct);
            AssertSame(Hero(i), heroes.FirstOrDefault(i, static (hero, id) => hero.Id == id));
            AssertSame(Hero(i), heroes.SingleOrDefault(i, static (hero, id) => hero.Id == id));
        }
    }

    [Fact]
    public void Lookup_OfAMissingOrDeletedKey_IsLeftToTheStatement_WhichFindsNothing()
    {
        using DataVoContext db = Open();
        Load(db, 10);
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();
        db.Execute("DELETE FROM packed_heroes WHERE Id = 4");

        Assert.False(heroes.TryReadByIntegerKey(4, out _));
        Assert.False(heroes.TryReadByIntegerKey(11, out _));
        Assert.Null(heroes.FirstOrDefault(4, static (hero, id) => hero.Id == id));
        Assert.Null(heroes.FirstOrDefault(11, static (hero, id) => hero.Id == id));
        AssertSame(Hero(5), heroes.FirstOrDefault(5, static (hero, id) => hero.Id == id));
    }

    [Fact]
    public void Lookup_SeesAnUpdatedRow()
    {
        using DataVoContext db = Open();
        Load(db, 10);
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();
        PackedHero changed = Hero(7);
        changed.Name = "renamed";
        changed.Score = -1;
        changed.Nickname = null;
        db.Update(changed);

        AssertSame(changed, heroes.FirstOrDefault(7, static (hero, id) => hero.Id == id));
        if (heroes.TryReadByIntegerKey(7, out PackedHero direct))
        {
            AssertSame(changed, direct);
        }
    }

    [Fact]
    public void Lookup_InsideATransaction_GoesThroughTheStatement_AndSeesTheSessionsOwnWrites()
    {
        using DataVoContext db = Open();
        Load(db, 5);
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();

        db.Execute("BEGIN TRANSACTION");
        db.Execute("UPDATE packed_heroes SET Score = 42 WHERE Id = 3");
        Assert.False(heroes.TryReadByIntegerKey(3, out _));
        Assert.Equal(42, heroes.FirstOrDefault(3, static (hero, id) => hero.Id == id)!.Score);
        db.Execute("ROLLBACK");

        Assert.Equal(Hero(3).Score, heroes.FirstOrDefault(3, static (hero, id) => hero.Id == id)!.Score);
    }

    [Theory]
    [InlineData(StorageMode.Lsm)]
    [InlineData(StorageMode.Disk)]
    public void Lookup_OnATableThatIsNotInMemory_GoesThroughTheStatement(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Load(db, 20);
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();

        Assert.False(heroes.TryReadByIntegerKey(9, out _));
        AssertSame(Hero(9), heroes.FirstOrDefault(9, static (hero, id) => hero.Id == id));
    }

    [Fact]
    public void Lookup_OfARowStoredAsBytes_GoesThroughTheStatement_InATableThatAlsoHasPackedRows()
    {
        using DataVoContext db = Open();
        Load(db, 3);
        db.Insert(Hero(50)); // an INSERT statement: an in-memory table keeps such a row as serialized bytes
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();

        Assert.False(heroes.TryReadByIntegerKey(50, out _));
        AssertSame(Hero(50), heroes.FirstOrDefault(50, static (h, id) => h.Id == id));
        Assert.True(heroes.TryReadByIntegerKey(3, out PackedHero packed));
        AssertSame(Hero(3), packed);
    }

    [Fact]
    public void Lookup_AfterTheTableIsRecreatedWithItsColumnsInAnotherOrder_StillReadsByName()
    {
        using DataVoContext db = Open();
        Load(db, 5);
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();
        Assert.True(heroes.TryReadByIntegerKey(2, out _));

        db.Execute("DROP TABLE packed_heroes");
        db.Execute("CREATE TABLE packed_heroes (Name VARCHAR NOT NULL, Id INT PRIMARY KEY, Retired DATE, Bonus INT, Nickname VARCHAR, "
            + "Position VECTOR(3) NOT NULL, Born DATE NOT NULL, Tag GUID NOT NULL, Alive BIT NOT NULL, Ratio DOUBLE NOT NULL, "
            + "Score BIGINT NOT NULL, Health FLOAT NOT NULL)");
        var reordered = new ReactiveRowSchema(
            "Name", "Id", "Retired", "Bonus", "Nickname", "Position", "Born", "Tag", "Alive", "Ratio", "Score", "Health");
        int[] from = [7, 0, 11, 10, 9, 8, 6, 5, 4, 3, 2, 1];
        db.InsertTypedBatch("packed_heroes", reordered,
            [.. Enumerable.Range(1, 6).Select(i => from.Select(ordinal => Cells(Hero(i))[ordinal]).ToArray())]);

        for (int i = 1; i <= 6; i++)
        {
            AssertSame(Hero(i), heroes.FirstOrDefault(i, static (hero, id) => hero.Id == id));
            Assert.True(heroes.TryReadByIntegerKey(i, out PackedHero direct));
            AssertSame(Hero(i), direct);
        }
    }

    [Fact]
    public void Lookup_AfterSnapshotRestore_ReadsTheRestoredRows()
    {
        using DataVoContext db = Open();
        Load(db, 5);
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();
        var snapshot = db.CreateSnapshot();
        PackedHero changed = Hero(2);
        changed.Name = "after the snapshot";
        db.Update(changed);
        db.InsertTypedBatch("packed_heroes", Schema, [Cells(Hero(6))]);

        Assert.True(heroes.TryReadByIntegerKey(1, out _)); // the reader now holds the table and its key map

        db.RestoreSnapshot(snapshot);

        // The same reader: what it held was replaced by the restore, and it must notice.
        AssertSame(Hero(2), heroes.FirstOrDefault(2, static (hero, id) => hero.Id == id));
        Assert.Null(heroes.FirstOrDefault(6, static (hero, id) => hero.Id == id));
        Assert.True(heroes.TryReadByIntegerKey(2, out PackedHero restored));
        AssertSame(Hero(2), restored);
        Assert.False(heroes.TryReadByIntegerKey(6, out _));
    }

    /// <summary>
    /// Readers look rows up by key while a writer deletes and reinserts them. A lookup returns the whole row or
    /// nothing: a row caught mid-removal must not surface with a string already let go, nor as an error.
    /// </summary>
    [Fact]
    public void Lookup_WhileRowsAreDeletedAndReinserted_ReturnsWholeRowsOrNothing()
    {
        using DataVoContext db = Open();
        const int Count = 200;
        Load(db, Count);
        DataVoTable<PackedHero> heroes = db.Table<PackedHero>();
        Exception? failure = null;
        bool done = false;

        var readers = new Thread[2];
        for (int r = 0; r < readers.Length; r++)
        {
            int seed = r + 1;
            readers[r] = new Thread(() =>
            {
                var random = new Random(seed);
                try
                {
                    while (!Volatile.Read(ref done))
                    {
                        int id = 1 + random.Next(Count);
                        if (heroes.FirstOrDefault(id, static (hero, key) => hero.Id == key) is { } hero)
                        {
                            AssertSame(Hero(id), hero);
                        }
                    }
                }
                catch (Exception e)
                {
                    Volatile.Write(ref failure, e);
                }
            });
            readers[r].Start();
        }

        var writes = new Random(99);
        for (int round = 0; round < 3_000 && Volatile.Read(ref failure) is null; round++)
        {
            int id = 1 + writes.Next(Count);
            db.Execute($"DELETE FROM packed_heroes WHERE Id = {id}");
            db.InsertTypedBatch("packed_heroes", Schema, [Cells(Hero(id))]);
        }

        Volatile.Write(ref done, true);
        foreach (Thread reader in readers)
        {
            reader.Join();
        }

        Assert.Null(failure);
    }
}
