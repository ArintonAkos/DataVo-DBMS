using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.GeneratedTables;

[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }

    [Indexed] public int Level { get; set; }

    public string Name { get; set; } = "";

    public int DoubleLevel => Level * 2;
}

[DataVoTable("items")]
public sealed class Item
{
    [PrimaryKey] public int Id { get; set; }

    public int Qty { get; set; }
}

[DataVoTable("scores")]
public sealed record Score([property: PrimaryKey] Guid Id, int Points, string? Note);

[DataVoTable("settings")]
public sealed class Setting
{
    [PrimaryKey] public string Key_ { get; init; } = "";

    public string Value { get; init; } = "";
}

[DataVoTable("positions")]
public struct Position
{
    [PrimaryKey] public int Id { get; set; }

    public float X { get; set; }
}

[DataVoTable("everything")]
public sealed class AllTypes
{
    [PrimaryKey] public int Id { get; set; }

    public float Speed { get; set; }

    public bool Alive { get; set; }

    public Guid Tag { get; set; }

    public DataVoDate Born { get; set; }

    public DateOnly Joined { get; set; }

    public int? MaybeInt { get; set; }

    public float? MaybeFloat { get; set; }

    public bool? MaybeBool { get; set; }

    public Guid? MaybeTag { get; set; }

    public DataVoDate? MaybeBorn { get; set; }

    public DateOnly? MaybeJoined { get; set; }

    public string? MaybeText { get; set; }
}

public class BaseEntity
{
    [PrimaryKey] public int Id { get; set; }
}

[DataVoTable("derived_rows")]
public sealed class DerivedRow : BaseEntity
{
    public int Value { get; set; }
}

[DataVoTable("mismatched")]
public sealed class MismatchedColumn
{
    [PrimaryKey] public int Id { get; set; }

    public string Amount { get; set; } = "";
}

/// <summary>Models marked <c>[DataVoTable]</c> get working, typed data access from the source generator.</summary>
public sealed class GeneratedTableTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_gen_{Guid.NewGuid():N}");

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
    public void InsertGetAndUpdate_RoundTrip(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsurePlayerTable();

        db.Insert(new Player { Id = 1, Level = 5, Name = "O'Brien" });
        Player loaded = db.GetPlayerById(1)!;
        Assert.Equal(5, loaded.Level);
        Assert.Equal("O'Brien", loaded.Name);

        Assert.True(db.Update(new Player { Id = 1, Level = 9, Name = "Ann" }));
        Assert.Equal(9, db.GetPlayerById(1)!.Level);
        Assert.Equal("Ann", db.GetPlayerById(1)!.Name);
    }

    [Fact]
    public void MissingRow_ReturnsNull_AndUpdateReportsNothingChanged()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsurePlayerTable();

        Assert.Null(db.GetPlayerById(404));
        Assert.False(db.Update(new Player { Id = 404, Level = 1, Name = "x" }));
    }

    [Fact]
    public void DuplicateKey_Throws()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        db.EnsurePlayerTable();
        db.Insert(new Player { Id = 1, Name = "a" });

        Assert.Throws<InvalidOperationException>(() => db.Insert(new Player { Id = 1, Name = "b" }));
    }

    [Fact]
    public void NullEntity_Throws()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        db.EnsurePlayerTable();

        Assert.Throws<ArgumentNullException>(() => db.Insert((Player)null!));
    }

    [Fact]
    public void EnsureTable_CreatesIndexesAndCanRunTwice()
    {
        using DataVoContext db = Open(StorageMode.Lsm);

        db.EnsurePlayerTable();
        db.EnsurePlayerTable();

        Assert.Single(db.Engine.Catalog.GetTableIndexes("players", "G"), index => index.AttributeNames.Contains("Level"));
    }

    [Fact]
    public void EnsureTable_AddsAMissingIndexToAnExistingTable()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        Ok(db, "CREATE TABLE players (Id INT PRIMARY KEY, Level INT, Name VARCHAR)");

        db.EnsurePlayerTable();

        Assert.Single(db.Engine.Catalog.GetTableIndexes("players", "G"), index => index.AttributeNames.Contains("Level"));
    }

    [Fact]
    public void EnsureTable_RejectsATableThatDoesNotMatch()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        Ok(db, "CREATE TABLE mismatched (Id INT PRIMARY KEY, Amount INT)");

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => db.EnsureMismatchedColumnTable());

        Assert.Contains("Amount", error.Message);
        Assert.Contains("VARCHAR", error.Message);
    }

    [Fact]
    public void EnsureTable_WithoutADatabase_ExplainsWhy()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

        Assert.Contains("Select a database", Assert.Throws<InvalidOperationException>(() => db.EnsurePlayerTable()).Message);
    }

    [Fact]
    public void Record_WithConstructorParameters_RoundTrips()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsureScoreTable();
        Guid id = Guid.NewGuid();

        db.Insert(new Score(id, 7, null));

        Assert.Equal(new Score(id, 7, null), db.GetScoreById(id));
        Assert.True(db.Update(new Score(id, 8, "better")));
        Assert.Equal("better", db.GetScoreById(id)!.Note);
    }

    [Fact]
    public void StringKeyAndInitOnlyProperties_RoundTrip()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsureSettingTable();

        db.Insert(new Setting { Key_ = "theme", Value = "dark" });

        Assert.Equal("dark", db.GetSettingByKey_("theme")!.Value);
    }

    [Fact]
    public void Struct_ReturnsNullWhenMissing()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        db.EnsurePositionTable();
        db.Insert(new Position { Id = 1, X = 2.5f });

        Assert.Equal(2.5f, db.GetPositionById(1)!.Value.X);
        Assert.Null(db.GetPositionById(2));
    }

    [Fact]
    public void InheritedPrimaryKey_IsAColumn()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        db.EnsureDerivedRowTable();
        db.Insert(new DerivedRow { Id = 3, Value = 4 });

        Assert.Equal(4, db.GetDerivedRowById(3)!.Value);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void EverySupportedType_RoundTrips(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureAllTypesTable();
        var date = new DateOnly(2024, 2, 29);
        Guid tag = Guid.NewGuid();
        var full = new AllTypes
        {
            Id = 1, Speed = 1.5f, Alive = true, Tag = tag, Born = DataVoDate.FromDateOnly(date), Joined = date,
            MaybeInt = 3, MaybeFloat = 2.5f, MaybeBool = false, MaybeTag = tag, MaybeBorn = DataVoDate.FromDateOnly(date), MaybeJoined = date, MaybeText = "x",
        };
        db.Insert(full);
        db.Insert(new AllTypes { Id = 2, Born = DataVoDate.FromDateOnly(date), Joined = date, Tag = tag });

        AllTypes loaded = db.GetAllTypesById(1)!;
        Assert.Equal((1.5f, true, tag, date), (loaded.Speed, loaded.Alive, loaded.Tag, loaded.Joined));
        Assert.Equal(DataVoDate.FromDateOnly(date), loaded.Born);
        Assert.Equal(3, loaded.MaybeInt);
        Assert.Equal(2.5f, loaded.MaybeFloat);
        Assert.False(loaded.MaybeBool);
        Assert.Equal(tag, loaded.MaybeTag);
        Assert.Equal(DataVoDate.FromDateOnly(date), loaded.MaybeBorn);
        Assert.Equal(date, loaded.MaybeJoined);
        Assert.Equal("x", loaded.MaybeText);

        AllTypes empty = db.GetAllTypesById(2)!;
        Assert.Null(empty.MaybeInt);
        Assert.Null(empty.MaybeFloat);
        Assert.Null(empty.MaybeBool);
        Assert.Null(empty.MaybeTag);
        Assert.Null(empty.MaybeBorn);
        Assert.Null(empty.MaybeJoined);
        Assert.Null(empty.MaybeText);
    }

    [Theory]
    [InlineData(StorageMode.Disk)]
    [InlineData(StorageMode.Lsm)]
    public void Data_SurvivesReopening(StorageMode mode)
    {
        using (DataVoContext first = Open(mode))
        {
            first.EnsurePlayerTable();
            first.Insert(new Player { Id = 1, Level = 5, Name = "a" });
        }

        using var second = new DataVoContext(Config(mode));
        Ok(second, "USE G");
        second.EnsurePlayerTable();

        Assert.Equal(5, second.GetPlayerById(1)!.Level);
    }

    [Fact]
    public void Models_WithTheSameKeyType_DoNotClash()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        db.EnsurePlayerTable();
        db.EnsureItemTable();
        db.Insert(new Player { Id = 1, Level = 2, Name = "a" });
        db.Insert(new Item { Id = 1, Qty = 9 });

        Assert.Equal(2, db.GetPlayerById(1)!.Level);
        Assert.Equal(9, db.GetItemById(1)!.Qty);
    }

    [Fact]
    public void ChangingSession_FollowsTheNewSessionsDatabase()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        db.EnsureItemTable();
        db.Insert(new Item { Id = 1, Qty = 9 });

        Guid other = Guid.NewGuid();
        Ok(db, "CREATE DATABASE H");
        db.Execute("USE H", other);
        db.SessionId = other;
        db.EnsureItemTable();

        Assert.Null(db.GetItemById(1));
    }

    [Fact]
    public void PointLookup_AllocatesOnlyTheModel()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsureItemTable();
        db.Insert(new Item { Id = 1, Qty = 9 });
        for (int i = 0; i < 1_000; i++)
        {
            db.GetItemById(1);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
        {
            db.GetItemById(1);
        }

        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / 1_000;
        Assert.True(perCall <= 64, $"{perCall} bytes per lookup.");
    }

    [Fact]
    public void Transaction_SeesItsOwnInsert_AndRollsBack()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsurePlayerTable();
        Ok(db, "BEGIN");

        db.Insert(new Player { Id = 1, Level = 5, Name = "a" });
        Assert.Equal(5, db.GetPlayerById(1)!.Level);

        Ok(db, "ROLLBACK");
        Assert.Null(db.GetPlayerById(1));
    }

    private DataVoConfig Config(StorageMode mode) => new()
    {
        StorageMode = mode,
        DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
    };

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(Config(mode));
        Ok(db, "CREATE DATABASE G");
        Ok(db, "USE G");
        return db;
    }

    private static void Ok(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
    }
}
