using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.Linq;

[DataVoTable("heroes")]
public sealed class Hero
{
    [PrimaryKey] public int Id { get; set; }

    [Indexed] public string Name { get; set; } = "";

    [Indexed] public int Level { get; set; }

    public bool IsActive { get; set; }

    public int? Score { get; set; }

    public string? Guild { get; set; }

    public long Gold { get; set; }

    public float Speed { get; set; }

    public Guid Tag { get; set; }

    public DateOnly Joined { get; set; }
}

[DataVoTable("waypoints")]
public struct Waypoint
{
    [PrimaryKey] public int Id { get; set; }

    public int Zone { get; set; }
}

[DataVoTable("counters")]
public sealed class Counter
{
    [PrimaryKey] public int Id { get; set; }

    public int Value { get; set; }
}

public static class HeroLimits
{
    public const int Veteran = 30;

    public static readonly int Elite = 40;

    public static int Legend => 50;
}

/// <summary>
/// Table queries written as lambdas return what the same lambda returns over the rows in memory, whether the
/// generator compiled them into a prepared statement or left them to scan.
/// </summary>
public sealed class LinqInterceptorTests : IDisposable
{
    private static readonly Guid KnownTag = new("11111111-2222-3333-4444-555555555555");

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_linq_{Guid.NewGuid():N}");
    private readonly List<Hero> _heroes = [];
    private readonly int _field = 7;

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
    public void PrimaryKeyLookup_WithState_ReturnsTheExactRow(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        int targetId = 17;

        Hero? hero = db.Table<Hero>().FirstOrDefault(targetId, static (h, id) => h.Id == id);

        Assert.NotNull(hero);
        AssertSame(_heroes[16], hero);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void PrimaryKeyLookup_Miss_ReturnsNull(StorageMode mode)
    {
        using DataVoContext db = Open(mode);

        Assert.Null(db.Table<Hero>().FirstOrDefault(9_999, static (h, id) => h.Id == id));
        Assert.Null(db.Table<Hero>().SingleOrDefault(9_999, static (h, id) => h.Id == id));
        Assert.Null(db.Table<Hero>().FirstOrDefault(h => h.Id == -1));
        Assert.Null(db.Table<Hero>().Where(h => h.Id == 9_999).FirstOrDefault());
    }

    [Fact]
    public void PrimaryKeyLookup_WithLiteralOrConstant_ReturnsTheRow()
    {
        using DataVoContext db = Open(StorageMode.Lsm);

        Assert.Equal(3, db.Table<Hero>().FirstOrDefault(h => h.Id == 3)!.Id);
        Assert.Equal(3, db.Table<Hero>().SingleOrDefault(h => 3 == h.Id)!.Id);
        Assert.Equal(HeroLimits.Veteran, db.Table<Hero>().FirstOrDefault(h => h.Id == HeroLimits.Veteran)!.Id);
        Assert.Equal(3, db.Table<Hero>().Where(h => h.Id == 3).FirstOrDefault()!.Id);
        Assert.Equal(3, db.Table<Hero>().Where(h => h.Id == 3).SingleOrDefault()!.Id);
    }

    [Fact]
    public void StateCanBeAVariableAParameterOrAnExpression()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        int local = 5;

        Assert.Equal(5, db.Table<Hero>().FirstOrDefault(local, static (h, id) => h.Id == id)!.Id);
        Assert.Equal(6, db.Table<Hero>().FirstOrDefault(local + 1, static (h, id) => h.Id == id)!.Id);
        Assert.Equal(7, db.Table<Hero>().FirstOrDefault(_field, static (h, id) => h.Id == id)!.Id);
        Assert.Equal(8, Lookup(db, 8)!.Id);
        for (int id = 1; id <= 20; id++)
        {
            Assert.Equal(id, db.Table<Hero>().FirstOrDefault(id, static (h, key) => h.Id == key)!.Id);
        }

        static Hero? Lookup(DataVoContext db, int id) => db.Table<Hero>().FirstOrDefault(id, static (h, key) => h.Id == key);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void IndexedEquality_FindsTheRow(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        string name = "hero-23";

        Assert.Equal(23, db.Table<Hero>().FirstOrDefault(name, static (h, n) => h.Name == n)!.Id);
        Assert.Equal(23, db.Table<Hero>().SingleOrDefault(name, static (h, n) => h.Name == n)!.Id);
        Assert.Equal(24, db.Table<Hero>().FirstOrDefault(h => h.Name == "hero-24")!.Id);
        Assert.Null(db.Table<Hero>().FirstOrDefault("HERO-23", static (h, n) => h.Name == n));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Comparisons_ReturnTheSameRowsAsTheLambda(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        int min = 35;

        Check(db.Table<Hero>().Where(min, static (h, m) => h.Level >= m), h => h.Level >= min);
        Check(db.Table<Hero>().Where(min, static (h, m) => h.Level > m), h => h.Level > min);
        Check(db.Table<Hero>().Where(min, static (h, m) => h.Level < m), h => h.Level < min);
        Check(db.Table<Hero>().Where(min, static (h, m) => h.Level <= m), h => h.Level <= min);
        Check(db.Table<Hero>().Where(min, static (h, m) => h.Level != m), h => h.Level != min);
        Check(db.Table<Hero>().Where(min, static (h, m) => m <= h.Level), h => min <= h.Level);
        Check(db.Table<Hero>().Where(min, static (h, m) => m > h.Level), h => min > h.Level);
        Check(db.Table<Hero>().Where(h => h.Level >= 45), h => h.Level >= 45);
        Check(db.Table<Hero>().Where(h => h.Level >= HeroLimits.Veteran), h => h.Level >= HeroLimits.Veteran);
        Check(db.Table<Hero>().Where(h => h.Level >= HeroLimits.Elite), h => h.Level >= HeroLimits.Elite);
        Check(db.Table<Hero>().Where(h => h.Level >= HeroLimits.Legend), h => h.Level >= HeroLimits.Legend);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ConjunctionsAndDisjunctions_ReturnTheSameRowsAsTheLambda(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        (int Min, int Max) range = (10, 20);

        Check(db.Table<Hero>().Where(h => h.Level > 10 && h.IsActive), h => h.Level > 10 && h.IsActive);
        Check(db.Table<Hero>().Where(h => h.Level > 10 && !h.IsActive), h => h.Level > 10 && !h.IsActive);
        Check(db.Table<Hero>().Where(range, static (h, r) => h.Level >= r.Min && h.Level <= r.Max), h => h.Level >= 10 && h.Level <= 20);
        Check(db.Table<Hero>().Where(range, static (h, r) => h.Level < r.Min || h.Level > r.Max), h => h.Level < 10 || h.Level > 20);
        Check(
            db.Table<Hero>().Where(range, static (h, r) => (h.Level < r.Min || h.Level > r.Max) && h.IsActive && h.Gold >= 1_000L),
            h => (h.Level < 10 || h.Level > 20) && h.IsActive && h.Gold >= 1_000L);
        Check(db.Table<Hero>().Where(12, static (h, id) => h.Id == id && h.IsActive), h => h.Id == 12 && h.IsActive);
        Check(db.Table<Hero>().Where(12, static (h, id) => h.Id == id || h.Level == 50), h => h.Id == 12 || h.Level == 50);
        Check(
            db.Table<Hero>().Where(h => h.Id > 1 && h.Id < 40 && h.Level > 2 && h.Level < 45 && h.Gold > 100L && h.IsActive),
            h => h.Id > 1 && h.Id < 40 && h.Level > 2 && h.Level < 45 && h.Gold > 100L && h.IsActive);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NullHandling_MatchesCSharp(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        int? noScore = null;
        int? someScore = 30;
        string? noGuild = null;

        Check(db.Table<Hero>().Where(h => h.Score == null), h => h.Score == null);
        Check(db.Table<Hero>().Where(h => h.Score != null), h => h.Score != null);
        Check(db.Table<Hero>().Where(h => h.Guild is null), h => h.Guild is null);
        Check(db.Table<Hero>().Where(h => h.Guild is not null), h => h.Guild is not null);
        Check(db.Table<Hero>().Where(h => h.Score != 30), h => h.Score != 30);
        Check(db.Table<Hero>().Where(h => h.Score > 30), h => h.Score > 30);
        Check(db.Table<Hero>().Where(h => h.Guild != "red"), h => h.Guild != "red");
        Check(db.Table<Hero>().Where(someScore, static (h, s) => h.Score == s), h => h.Score == someScore);
        Check(db.Table<Hero>().Where(someScore, static (h, s) => h.Score != s), h => h.Score != someScore);
        Check(db.Table<Hero>().Where(noScore, static (h, s) => h.Score == s), h => h.Score == noScore);
        Check(db.Table<Hero>().Where(noScore, static (h, s) => h.Score != s), h => h.Score != noScore);
        Check(db.Table<Hero>().Where(noScore, static (h, s) => h.Score > s), h => h.Score > noScore);
        Check(db.Table<Hero>().Where(noGuild, static (h, g) => h.Guild == g), h => h.Guild == noGuild);
        Check(db.Table<Hero>().Where(noGuild, static (h, g) => h.Guild != g), h => h.Guild != noGuild);
        Check(db.Table<Hero>().Where("red", static (h, g) => h.Guild == g), h => h.Guild == "red");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void EveryColumnType_CanBeCompared(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        var joined = new DateOnly(2024, 1, 20);

        Check(db.Table<Hero>().Where(5_000L, static (h, g) => h.Gold >= g), h => h.Gold >= 5_000L);
        Check(db.Table<Hero>().Where(h => h.Gold == 700), h => h.Gold == 700);
        Check(db.Table<Hero>().Where(2.5f, static (h, s) => h.Speed > s), h => h.Speed > 2.5f);
        Check(db.Table<Hero>().Where(h => h.Speed <= 1.5f), h => h.Speed <= 1.5f);
        Check(db.Table<Hero>().Where(KnownTag, static (h, t) => h.Tag == t), h => h.Tag == KnownTag);
        Check(db.Table<Hero>().Where(true, static (h, a) => h.IsActive == a), h => h.IsActive);
        Check(db.Table<Hero>().Where(joined, static (h, d) => h.Joined == d), h => h.Joined == joined);
        Check(db.Table<Hero>().Where(joined, static (h, d) => h.Joined > d), h => h.Joined > joined);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CapturedAndUntranslatableLambdas_FallBackToACorrectScan(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        int targetId = 21;
        int min = 44;
        Func<Hero, bool> built = h => h.Level == 3;

#pragma warning disable DATAVOL001, DATAVOL002 // these calls are meant to scan
        Assert.Equal(21, db.Table<Hero>().FirstOrDefault(h => h.Id == targetId)!.Id);
        Assert.Equal(21, db.Table<Hero>().Where(h => h.Id == targetId).SingleOrDefault()!.Id);
        Assert.Null(db.Table<Hero>().FirstOrDefault(h => h.Id == targetId + 10_000));
        Check(db.Table<Hero>().Where(h => h.Level >= min), h => h.Level >= min, compiled: false);
        Check(db.Table<Hero>().Where(h => h.Level >= _field), h => h.Level >= _field, compiled: false);
        Check(db.Table<Hero>().Where(h => h.Name.EndsWith("7", StringComparison.Ordinal)), h => h.Name.EndsWith("7", StringComparison.Ordinal), compiled: false);
        Check(db.Table<Hero>().Where(min, static (h, m) => h.Level + 1 > m), h => h.Level + 1 > min, compiled: false);
        Check(db.Table<Hero>().Where(min, static (h, m) => h.Level > m / 2), h => h.Level > min / 2, compiled: false);
        Check(db.Table<Hero>().Where(h => h.Speed > 2.5), h => h.Speed > 2.5, compiled: false);
        Check(db.Table<Hero>().Where(h => h.Level > h.Id), h => h.Level > h.Id, compiled: false);
#pragma warning restore DATAVOL001, DATAVOL002
        Check(db.Table<Hero>().Where(built), built, compiled: false);
        Assert.Equal(3, db.Table<Hero>().FirstOrDefault(built)!.Level);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SingleOrDefault_ThrowsWhenSeveralRowsMatch(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        int min = 10;

        Assert.Throws<InvalidOperationException>(() => db.Table<Hero>().SingleOrDefault(10, static (h, m) => h.Level >= m));
        Assert.Throws<InvalidOperationException>(() => db.Table<Hero>().Where(h => h.IsActive).SingleOrDefault());
#pragma warning disable DATAVOL001
        Assert.Throws<InvalidOperationException>(() => db.Table<Hero>().SingleOrDefault(h => h.Level >= min));
#pragma warning restore DATAVOL001
        Assert.Equal(50, db.Table<Hero>().SingleOrDefault(50, static (h, m) => h.Level >= m)!.Level);
    }

    [Fact]
    public void Query_CountsChecksAndListsItsRows()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        var query = db.Table<Hero>().Where(40, static (h, m) => h.Level >= m);
        Assert.True(query.IsCompiled);

        Assert.Equal(_heroes.Count(h => h.Level >= 40), query.Count());
        Assert.True(query.Any());
        Assert.False(db.Table<Hero>().Where(400, static (h, m) => h.Level >= m).Any());
        Assert.Equal(query.Count(), query.ToList().Count);
        Assert.Equal(query.Count(), query.Select(static h => h.Id).Distinct().Count());
        Assert.Equal(_heroes.Count, db.Table<Hero>().ToList().Count);
        Assert.Equal(_heroes.Count, db.Table<Hero>().Count());
    }

    [Fact]
    public void Query_SeesRowsWrittenAfterItWasCreated()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        var query = db.Table<Hero>().Where(1_000, static (h, m) => h.Level >= m);
        Assert.False(query.Any());

        db.Insert(new Hero { Id = 500, Name = "late", Level = 1_000 });

        Assert.Equal(500, query.FirstOrDefault()!.Id);
        Assert.Equal(500, db.Table<Hero>().FirstOrDefault(500, static (h, id) => h.Id == id)!.Id);
    }

    [Fact]
    public void WholeTable_CanBeEnumerated()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        var ids = new List<int>();
        foreach (Hero hero in db.Table<Hero>())
        {
            ids.Add(hero.Id);
        }

        Assert.Equal(_heroes.Select(static h => h.Id), ids.Order());
    }

    [Fact]
    public void StructModel_ReturnsDefaultOnAMiss()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsureWaypointTable();
        db.Insert(new Waypoint { Id = 1, Zone = 4 });

        Assert.Equal(4, db.Table<Waypoint>().FirstOrDefault(1, static (w, id) => w.Id == id).Zone);
        Assert.Equal(default, db.Table<Waypoint>().FirstOrDefault(2, static (w, id) => w.Id == id));
        Assert.Equal(1, db.Table<Waypoint>().Where(4, static (w, zone) => w.Zone == zone).SingleOrDefault().Id);
    }

    [Fact]
    public void Table_IsSharedPerContext_AndSeparateBetweenContexts()
    {
        using DataVoContext first = Open(StorageMode.InMemory);
        using var second = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        second.Execute("CREATE DATABASE G");
        second.Execute("USE G");
        second.EnsureHeroTable();
        second.Insert(new Hero { Id = 1, Name = "other", Level = 99 });

        Assert.Same(first.Table<Hero>(), first.Table<Hero>());
        Assert.NotSame(first.Table<Hero>(), second.Table<Hero>());
        Assert.Equal("hero-1", first.Table<Hero>().FirstOrDefault(1, static (h, id) => h.Id == id)!.Name);
        Assert.Equal("other", second.Table<Hero>().FirstOrDefault(1, static (h, id) => h.Id == id)!.Name);
    }

    [Fact]
    public void InterceptedPointLookup_AllocatesOnlyTheRow()
    {
        // The LSM engine answers a prepared point lookup without allocating, so any byte here is the query's own.
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsureCounterTable();
        for (int id = 1; id <= 500; id++)
        {
            db.Insert(new Counter { Id = id, Value = id * 2 });
        }

        DataVoTable<Counter> counters = db.Table<Counter>();
        const int Iterations = 1_000;
        long sum = 0;

        void Hits()
        {
            for (int i = 0; i < Iterations; i++)
            {
                sum += counters.FirstOrDefault(1 + (i % 500), static (c, id) => c.Id == id)!.Value;
            }
        }

        void Literals()
        {
            for (int i = 0; i < Iterations; i++)
            {
                sum += counters.FirstOrDefault(c => c.Id == 7)!.Value;
            }
        }

        void Chained()
        {
            for (int i = 0; i < Iterations; i++)
            {
                sum += counters.Where(1 + (i % 500), static (c, id) => c.Id == id).SingleOrDefault()!.Value;
            }
        }

        void Misses()
        {
            for (int i = 0; i < Iterations; i++)
            {
                sum += counters.FirstOrDefault(-1 - i, static (c, id) => c.Id == id) is null ? 0 : 1;
            }
        }

        // Each call site prepares its statement the first time it runs.
        Hits();
        Literals();
        Chained();
        Misses();

        // A Counter is 24 bytes; anything beyond one per hit means the call was not routed to the prepared lookup.
        Assert.Equal(Iterations * 24, Measure(Hits));
        Assert.Equal(Iterations * 24, Measure(Literals));
        Assert.Equal(Iterations * 24, Measure(Chained));
        Assert.Equal(0, Measure(Misses));
        Assert.True(sum > 0);
    }

    [Fact]
    public void CapturedLambda_CostsFarMoreThanTheStateForm()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        DataVoTable<Hero> heroes = db.Table<Hero>();
        int targetId = 17;

        void WithState() => Assert.NotNull(heroes.FirstOrDefault(targetId, static (h, id) => h.Id == id));
        // The scan is what is being measured.
#pragma warning disable DATAVOL001
        void Captured() => Assert.NotNull(heroes.FirstOrDefault(h => h.Id == targetId));
#pragma warning restore DATAVOL001

        WithState();
        Captured();

        Assert.True(Measure(Captured) > Measure(WithState) * 10);
    }

    private static long Measure(Action action)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        action();
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private void Check<TState>(DataVoQuery<Hero, TState> query, Func<Hero, bool> expected, bool compiled = true)
    {
        Assert.Equal(compiled, query.IsCompiled);
        int[] want = [.. _heroes.Where(expected).Select(static h => h.Id).Order()];
        var got = new List<int>();
        foreach (Hero hero in query)
        {
            AssertSame(_heroes[hero.Id - 1], hero);
            got.Add(hero.Id);
        }

        got.Sort();
        Assert.Equal(want, got);
        Assert.Equal(want, query.ToList().Select(static h => h.Id).Order());
        Assert.Equal(want.Length, query.Count());
        Assert.Equal(want.Length > 0, query.Any());
        Hero? first = query.FirstOrDefault();
        Assert.Equal(want.Length > 0, first is not null);
        if (first is not null)
        {
            Assert.Contains(first.Id, want);
        }
    }

    private static void AssertSame(Hero expected, Hero actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.Level, actual.Level);
        Assert.Equal(expected.IsActive, actual.IsActive);
        Assert.Equal(expected.Score, actual.Score);
        Assert.Equal(expected.Guild, actual.Guild);
        Assert.Equal(expected.Gold, actual.Gold);
        Assert.Equal(expected.Speed, actual.Speed);
        Assert.Equal(expected.Tag, actual.Tag);
        Assert.Equal(expected.Joined, actual.Joined);
    }

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
        });
        db.Execute("CREATE DATABASE G");
        db.Execute("USE G");
        db.EnsureHeroTable();

        string?[] guilds = ["red", "blue", null];
        for (int id = 1; id <= 50; id++)
        {
            var hero = new Hero
            {
                Id = id,
                Name = $"hero-{id}",
                Level = id,
                IsActive = id % 2 == 0,
                Score = id % 5 == 0 ? null : id % 7 * 10,
                Guild = guilds[id % 3],
                Gold = id * 100L,
                Speed = id % 4 + 0.5f,
                Tag = id == 9 ? KnownTag : Guid.NewGuid(),
                Joined = new DateOnly(2024, 1, 1).AddDays(id),
            };
            _heroes.Add(hero);
            db.Insert(hero);
        }

        return db;
    }
}
