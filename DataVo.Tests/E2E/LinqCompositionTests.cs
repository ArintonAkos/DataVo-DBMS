using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.Linq;

/// <summary>An init-only model, for checking that composed queries never need setters.</summary>
[DataVoTable("relics")]
public sealed class Relic
{
    [PrimaryKey] public int Id { get; init; }

    public string Name { get; init; } = "";

    public int Power { get; init; }

    public double? Weight { get; init; }
}

/// <summary>A positional record model.</summary>
[DataVoTable("quests")]
public sealed record Quest([PrimaryKey] int Id, string Title, int Reward, bool Done);

/// <summary>A model that counts how many times it is built, so a test can prove a projection builds none.</summary>
[DataVoTable("tallies")]
public sealed class Tally
{
    private static int s_built;

    public Tally() => Interlocked.Increment(ref s_built);

    public static int Built => Volatile.Read(ref s_built);

    [PrimaryKey] public int Id { get; set; }

    public int Team { get; set; }

    public string Label { get; set; } = "";
}

/// <summary>A record a projection constructs.</summary>
public sealed record HeroCard(int Id, string Name);

/// <summary>A class a projection fills with an object initializer.</summary>
public sealed class HeroLine
{
    public int Id { get; set; }

    public string Text { get; set; } = "";

    public long Wealth { get; init; }
}

/// <summary>
/// <c>OrderBy</c>, <c>Skip</c>, <c>Take</c>, <c>Select</c>, <c>ExecuteDelete</c> and <c>ExecuteUpdate</c> on typed tables
/// return what the same LINQ operators return over the rows in memory, on every storage engine, whether the
/// generator compiled them or not.
/// </summary>
public sealed class LinqCompositionTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_linq_compose_{Guid.NewGuid():N}");
    private readonly List<Hero> _heroes = [];

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
    public void OrderBy_OnEveryColumnType_MatchesLinqOrder(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();

        Same(heroes.OrderBy(static h => h.Level), _heroes.OrderBy(static h => h.Level));
        Same(heroes.OrderByDescending(static h => h.Gold), _heroes.OrderByDescending(static h => h.Gold));
        Same(heroes.OrderBy(static h => h.Score), _heroes.OrderBy(static h => h.Score));
        Same(heroes.OrderByDescending(static h => h.Score), _heroes.OrderByDescending(static h => h.Score));
        Same(heroes.OrderBy(static h => h.Speed), _heroes.OrderBy(static h => h.Speed));
        Same(heroes.OrderBy(static h => h.IsActive), _heroes.OrderBy(static h => h.IsActive));
        Same(heroes.OrderBy(static h => h.Tag), _heroes.OrderBy(static h => h.Tag));
        Same(heroes.OrderByDescending(static h => h.Joined), _heroes.OrderByDescending(static h => h.Joined));
        Same(heroes.OrderBy(static h => h.Name), _heroes.OrderBy(static h => h.Name));
        Same(heroes.OrderBy(static h => h.Guild), _heroes.OrderBy(static h => h.Guild));
        Same(heroes.OrderByDescending(static h => h.Guild), _heroes.OrderByDescending(static h => h.Guild));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ThenBy_BreaksTiesAsLinqDoes(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();

        Same(
            heroes.OrderBy(static h => h.IsActive).ThenByDescending(static h => h.Speed).ThenBy(static h => h.Name),
            _heroes.OrderBy(static h => h.IsActive).ThenByDescending(static h => h.Speed).ThenBy(static h => h.Name));
        Same(
            heroes.OrderByDescending(static h => h.Guild).ThenBy(static h => h.Score).ThenByDescending(static h => h.Id),
            _heroes.OrderByDescending(static h => h.Guild).ThenBy(static h => h.Score).ThenByDescending(static h => h.Id));
        Same(
            heroes.Where(static h => h.IsActive).OrderBy(static h => h.Speed).ThenBy(static h => h.Gold),
            _heroes.Where(static h => h.IsActive).OrderBy(static h => h.Speed).ThenBy(static h => h.Gold));
        Same(
            heroes.Where(30, static (h, min) => h.Level >= min).OrderByDescending(static h => h.Score).ThenBy(static h => h.Level),
            _heroes.Where(static h => h.Level >= 30).OrderByDescending(static h => h.Score).ThenBy(static h => h.Level));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OrderedQuery_RunsInTheEngine_WhenEveryKeyIsCompiled(StorageMode mode)
    {
        using DataVoContext db = Open(mode);

        var ordered = db.Table<Hero>().Where(1_000L, static (h, gold) => h.Gold > gold).OrderByDescending(static h => h.Gold).ThenBy(static h => h.Name);

        Assert.True(ordered.IsCompiled);
        Assert.Equal("SELECT Id, Name, Level, IsActive, Score, Guild, Gold, Speed, Tag, Joined FROM heroes WHERE Gold > @p0 ORDER BY Gold DESC, Name", ordered.Sql);
        Assert.Equal(
            "SELECT Id, Name, Level, IsActive, Score, Guild, Gold, Speed, Tag, Joined FROM heroes WHERE Gold > @p0 ORDER BY Gold DESC, Name LIMIT @limit OFFSET @offset",
            ordered.Skip(2).Take(3).Sql);
        Assert.Equal(_heroes.Where(static h => h.Gold > 1_000L).Max(static h => h.Gold), ordered.FirstOrDefault()!.Gold);
        Assert.Equal(_heroes.Count(static h => h.Gold > 1_000L), ordered.Count());
        Assert.True(ordered.Any());
        Assert.Throws<InvalidOperationException>(() => ordered.SingleOrDefault());
        Assert.Equal(50, db.Table<Hero>().Where(50, static (h, id) => h.Id == id).OrderBy(static h => h.Gold).SingleOrDefault()!.Id);
        Assert.Null(db.Table<Hero>().Where(static h => h.Gold > 1_000_000L).OrderBy(static h => h.Gold).FirstOrDefault());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UncompiledOrdering_SortsInMemory_WithTheSameResult(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();
        int minimum = 20;

#pragma warning disable DATAVOL001, DATAVOL004 // these run in memory on purpose
        var computed = heroes.OrderBy(static h => h.Level % 7).ThenBy(static h => h.Name.Length);
        var captured = heroes.Where(h => h.Level >= minimum).OrderByDescending(static h => h.Gold);
#pragma warning restore DATAVOL001, DATAVOL004
        var compared = heroes.OrderBy(static h => h.Name, StringComparer.Ordinal);
        var throughLinq = Enumerable.ThenBy(heroes.OrderBy(static h => h.IsActive), static h => -h.Id);

        Assert.False(computed.IsCompiled);
        Assert.Null(computed.Sql);
        Same(computed, _heroes.OrderBy(static h => h.Level % 7).ThenBy(static h => h.Name.Length));
        Assert.False(captured.IsCompiled);
        Same(captured, _heroes.Where(h => h.Level >= minimum).OrderByDescending(static h => h.Gold));
        Same(compared, _heroes.OrderBy(static h => h.Name, StringComparer.Ordinal));
        Same(throughLinq, _heroes.OrderBy(static h => h.IsActive).ThenBy(static h => -h.Id));
        Same(computed.Take(4), _heroes.OrderBy(static h => h.Level % 7).ThenBy(static h => h.Name.Length).Take(4));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SkipAndTake_MatchEnumerableEdgeCases(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();
        int[] counts = [int.MinValue, -3, 0, 1, 7, 49, 50, 51, int.MaxValue];

        foreach (int count in counts)
        {
            Same(heroes.Take(count), _heroes.Take(count));
            Same(heroes.Skip(count), _heroes.Skip(count));
            Same(heroes.OrderByDescending(static h => h.Gold).Take(count), _heroes.OrderByDescending(static h => h.Gold).Take(count));
            Same(heroes.OrderBy(static h => h.Score).Skip(count), _heroes.OrderBy(static h => h.Score).Skip(count));
            Same(heroes.Where(static h => h.IsActive).Skip(count), _heroes.Where(static h => h.IsActive).Skip(count));
            Assert.Equal(_heroes.Skip(count).Count(), heroes.Skip(count).Count());
            Assert.Equal(_heroes.Take(count).Any(), heroes.Take(count).Any());
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ChainedSkipAndTake_NarrowOneWindow(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        var byGold = db.Table<Hero>().Where(static h => !h.IsActive).OrderByDescending(static h => h.Gold);
        IOrderedEnumerable<Hero> expected = _heroes.Where(static h => !h.IsActive).OrderByDescending(static h => h.Gold);

        Same(byGold.Skip(2).Take(10).Skip(3).Take(4), expected.Skip(2).Take(10).Skip(3).Take(4));
        Same(byGold.Take(5).Skip(2), expected.Take(5).Skip(2));
        Same(byGold.Take(5).Take(9), expected.Take(5).Take(9));
        Same(byGold.Skip(3).Skip(4).Take(2), expected.Skip(3).Skip(4).Take(2));
        Same(byGold.Take(3).Skip(-2).Take(-1), expected.Take(3).Skip(-2).Take(-1));
        Same(byGold.Skip(int.MaxValue).Skip(int.MaxValue), expected.Skip(int.MaxValue).Skip(int.MaxValue));
        Assert.Equal(expected.Skip(20).Count(), byGold.Skip(20).Count());
        Assert.Equal(expected.Skip(24).Any(), byGold.Skip(24).Any());
        Assert.Equal(expected.Skip(25).Any(), byGold.Skip(25).Any());
        Assert.Equal(expected.Skip(6).First().Id, byGold.Skip(6).FirstOrDefault()!.Id);
        Assert.Equal(expected.Skip(6).Take(1).Single().Id, byGold.Skip(6).Take(1).SingleOrDefault()!.Id);
        Assert.Throws<InvalidOperationException>(() => byGold.Skip(6).Take(2).SingleOrDefault());
        Assert.Null(byGold.Skip(25).SingleOrDefault());
        Assert.True(byGold.Skip(2).Take(3).IsCompiled);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UnorderedPaging_TakesTheRowsTheQueryEnumerates(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();

        Window(heroes.Where(static h => h.IsActive));
        Window(heroes.Where(2_000L, static (h, g) => h.Gold >= g));
        Window(heroes.Where("hero-12", static (h, n) => h.Name == n));
        Window(heroes.Where(12, static (h, id) => h.Id > id));
        Window(heroes.Where(12, static (h, id) => h.Id == id));

        static void Window<TState>(DataVoQuery<Hero, TState> query)
        {
            Assert.True(query.IsCompiled);
            List<int> all = [.. query.ToList().Select(static h => h.Id)];
            foreach ((int skip, int take) in new[] { (0, 3), (2, 4), (5, 100), (100, 1), (0, 0), (1, 1) })
            {
                Assert.Equal(all.Skip(skip).Take(take), query.Skip(skip).Take(take).Select(static h => h.Id));
                Assert.Equal(all.Skip(skip).Take(take), query.Skip(skip).Take(take).ToList().Select(static h => h.Id));
                Assert.Equal(all.Skip(skip).Take(take).Count(), query.Skip(skip).Take(take).Count());
            }
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Select_BuildsEveryProjectionShape_FromItsColumnsOnly(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();

        Projected(heroes.Select(static h => h.Name), _heroes.Select(static h => h.Name), "SELECT Name FROM heroes");
        Projected(heroes.Select(static h => new HeroCard(h.Id, h.Name)), _heroes.Select(static h => new HeroCard(h.Id, h.Name)), "SELECT Id, Name FROM heroes");
        Projected(
            heroes.Select(static h => (h.Id, Total: h.Gold + h.Level, Missing: h.Score == null)),
            _heroes.Select(static h => (h.Id, Total: h.Gold + h.Level, Missing: h.Score == null)),
            "SELECT Id, Gold, Level, Score FROM heroes");
        Projected(
            heroes.Select(static h => new { h.Name, Short = h.Guild == null ? -1 : h.Guild.Length, Rank = h.Level switch { > 40 => "high", > 20 => "mid", _ => "low" } }),
            _heroes.Select(static h => new { h.Name, Short = h.Guild == null ? -1 : h.Guild.Length, Rank = h.Level switch { > 40 => "high", > 20 => "mid", _ => "low" } }),
            "SELECT Name, Guild, Level FROM heroes");
        Projected(
            heroes.Select(static h => $"{h.Name}:{h.Gold:D6}/{h.Guild?.ToUpperInvariant() ?? "none"}"),
            _heroes.Select(static h => $"{h.Name}:{h.Gold:D6}/{h.Guild?.ToUpperInvariant() ?? "none"}"),
            "SELECT Name, Gold, Guild FROM heroes");
        Projected(heroes.Select(static h => 7), _heroes.Select(static h => 7), "SELECT Id FROM heroes");
        Projected(heroes.Select(static h => h.Joined.AddDays(h.Level)), _heroes.Select(static h => h.Joined.AddDays(h.Level)), "SELECT Joined, Level FROM heroes");
        Projected(heroes.Select(static h => h.Tag), _heroes.Select(static h => h.Tag), "SELECT Tag FROM heroes");
        Projected(heroes.Select(static h => h.Speed * 2), _heroes.Select(static h => h.Speed * 2), "SELECT Speed FROM heroes");
        Projected(heroes.Select(static h => new[] { h.Level, h.Score ?? 0 }.Max()), _heroes.Select(static h => new[] { h.Level, h.Score ?? 0 }.Max()), "SELECT Level, Score FROM heroes");
        Projected(
            heroes.Select(static h => new HeroLine { Id = h.Id, Text = h.Name.ToUpperInvariant(), Wealth = h.Gold * 2 }),
            _heroes.Select(static h => new HeroLine { Id = h.Id, Text = h.Name.ToUpperInvariant(), Wealth = h.Gold * 2 }),
            "SELECT Id, Name, Gold FROM heroes",
            static line => (line.Id, line.Text, line.Wealth));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Select_ComposesWithFiltersOrderingAndPaging(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();

        var board = heroes.Where(static h => h.IsActive).OrderByDescending(static h => h.Gold).ThenBy(static h => h.Id).Skip(1).Take(5).Select(static h => new { h.Id, h.Name });
        Assert.True(board.IsCompiled);
        Assert.Equal("SELECT Id, Name FROM heroes WHERE IsActive = @p0 ORDER BY Gold DESC, Id LIMIT @limit OFFSET @offset", board.Sql);
        Assert.Equal(
            _heroes.Where(static h => h.IsActive).OrderByDescending(static h => h.Gold).ThenBy(static h => h.Id).Skip(1).Take(5).Select(static h => new { h.Id, h.Name }),
            board.ToList());

        var names = heroes.Where(25, static (h, min) => h.Level > min).Select(static h => h.Name).Skip(3).Take(4);
        Assert.Equal(_heroes.Where(static h => h.Level > 25).Select(static h => h.Name).Skip(3).Take(4), names);
        Assert.Equal(4, names.Count());
        Assert.True(names.Any());
        Assert.Equal("hero-29", names.FirstOrDefault());
        Assert.Null(names.Skip(10).FirstOrDefault());
        Assert.Throws<InvalidOperationException>(() => names.SingleOrDefault());
        Assert.Equal("hero-29", names.Take(1).SingleOrDefault());

        var single = heroes.Where(17, static (h, id) => h.Id == id).Select(static h => h.Gold);
        Assert.Equal(_heroes[16].Gold, single.SingleOrDefault());
        Assert.Equal(0L, heroes.Where(-1, static (h, id) => h.Id == id).Select(static h => h.Gold).FirstOrDefault());
        Assert.Equal([_heroes[16].Gold], single.ToList());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UncompiledProjection_BuildsModelsAndProjectsThem(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();
        int bonus = 5;
        Func<Hero, int> elsewhere = static h => h.Level * 3;

#pragma warning disable DATAVOL004 // these build models on purpose
        var captured = heroes.Where(static h => h.IsActive).Select(h => h.Level + bonus);
        var whole = heroes.OrderBy(static h => h.Level).Select(static h => h);
#pragma warning restore DATAVOL004
        var delegated = heroes.Take(10).Select(elsewhere);

        Assert.False(captured.IsCompiled);
        Assert.Equal("SELECT Id, Name, Level, IsActive, Score, Guild, Gold, Speed, Tag, Joined FROM heroes WHERE IsActive = @p0", captured.Sql);
        Assert.Equal(_heroes.Where(static h => h.IsActive).Select(h => h.Level + bonus), captured);
        Assert.Equal(_heroes.Select(static h => h.Id), whole.Select(static h => h.Id));
        Assert.Equal(_heroes.Take(10).Select(elsewhere), delegated);
        Assert.Equal(10, delegated.Count());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void CompiledProjection_NeverBuildsTheModel(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureTallyTable();
        for (int id = 1; id <= 40; id++)
        {
            db.Insert(new Tally { Id = id, Team = id % 4, Label = $"t{id}" });
        }

        DataVoTable<Tally> tallies = db.Table<Tally>();
        int before = Tally.Built;

        List<string> labels = tallies.Where(static t => t.Team == 1).OrderByDescending(static t => t.Id).Select(static t => t.Label).ToList();
        var groups = tallies.Select(static t => new { t.Id, t.Team }).Skip(5).Take(10).ToList();
        int? firstGroup = tallies.OrderBy(static t => t.Label).Select(static t => (int?)t.Team).FirstOrDefault();
        int count = tallies.Select(static t => t.Label).Count();

        Assert.Equal(before, Tally.Built);
        Assert.Equal(["t37", "t33", "t29", "t25", "t21", "t17", "t13", "t9", "t5", "t1"], labels);
        Assert.Equal(Enumerable.Range(6, 10), groups.Select(static g => g.Id));
        Assert.Equal(1, firstGroup);
        Assert.Equal(40, count);

        Assert.Equal(40, tallies.ToList().Count);
        Assert.True(Tally.Built >= before + 40);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void EveryModelShape_SortsPagesProjectsAndChanges(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureRelicTable();
        db.EnsureQuestTable();
        db.EnsureWaypointTable();
        for (int id = 1; id <= 12; id++)
        {
            db.Insert(new Relic { Id = id, Name = $"relic-{id}", Power = id % 5, Weight = id % 3 == 0 ? null : id * 0.5 });
            db.Insert(new Quest(id, $"quest-{id}", id * 10, id % 2 == 0));
            db.Insert(new Waypoint { Id = id, Zone = id % 4 });
        }

        Assert.Equal([9, 4, 8, 3], db.Table<Relic>().OrderByDescending(static r => r.Power).ThenByDescending(static r => r.Id).Take(4).Select(static r => r.Id).ToList());
        Assert.Equal([3, 6, 9, 12], db.Table<Relic>().OrderBy(static r => r.Weight).ThenBy(static r => r.Id).Take(4).Select(static r => r.Id).ToList());
        Assert.Equal(["quest-12", "quest-10"], db.Table<Quest>().Where(static q => q.Done).OrderByDescending(static q => q.Reward).Take(2).Select(static q => q.Title).ToList());
        Assert.Equal(new Quest(3, "quest-3", 30, false), db.Table<Quest>().OrderBy(static q => q.Id).Skip(2).FirstOrDefault());
        Assert.Equal([4, 8, 12], db.Table<Waypoint>().Where(0, static (w, z) => w.Zone == z).OrderBy(static w => w.Id).Select(static w => w.Id).ToList());
        Assert.Equal(default, db.Table<Waypoint>().OrderBy(static w => w.Zone).Skip(100).FirstOrDefault());

        Assert.Equal(6, db.Table<Quest>().Where(static q => q.Done).ExecuteUpdate(5, static (set, extra) => set.Set(q => q.Reward, q => q.Reward + extra)));
        Assert.Equal(125, db.Table<Quest>().Where(12, static (q, id) => q.Id == id).Select(static q => q.Reward).SingleOrDefault());
        Assert.Equal(3, db.Table<Waypoint>().Where(0, static (w, z) => w.Zone == z).ExecuteDelete());
        Assert.Equal(9, db.Table<Waypoint>().Count());
        Assert.Equal(4, db.Table<Relic>().Where(static r => r.Weight == null).ExecuteUpdate(static set => set.Set(r => r.Weight, 9.5)));
        Assert.Equal(12, db.Table<Relic>().Where(static r => r.Weight != null).Count());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ExecuteDelete_RemovesExactlyTheMatchingRows(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();
        List<Hero> remaining = [.. _heroes];
        int captured = 45;

        Assert.Equal(remaining.RemoveAll(static h => h.Gold < 500L), heroes.Where(500L, static (h, g) => h.Gold < g).ExecuteDelete());
        Assert.Equal(remaining.RemoveAll(static h => h.Id == 10), heroes.Where(10, static (h, id) => h.Id == id).ExecuteDelete());
        Assert.Equal(0, heroes.Where(10, static (h, id) => h.Id == id).ExecuteDelete());
        Assert.Equal(remaining.RemoveAll(static h => h.Name == "hero-20"), heroes.Where("hero-20", static (h, n) => h.Name == n).ExecuteDelete());
#pragma warning disable DATAVOL001 // the delete runs per key on purpose
        Assert.Equal(remaining.RemoveAll(h => h.Level > captured), heroes.Where(h => h.Level > captured).ExecuteDelete());
#pragma warning restore DATAVOL001
        Assert.Equal(0, heroes.Where((int?)null, static (h, s) => h.Score == s && h.Id < 0).ExecuteDelete());

        Assert.Equal(remaining.Select(static h => h.Id), heroes.OrderBy(static h => h.Id).Select(static h => h.Id));
        Assert.Equal(remaining.Count, heroes.ExecuteDelete());
        Assert.False(heroes.Any());
        Assert.Equal(0, heroes.ExecuteDelete());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ExecuteUpdate_SetsValuesNullsAndArithmetic(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();
        const long Reward = 250L;
        string? noGuild = null;
        int active = _heroes.Count(static h => h.IsActive);
        int reds = _heroes.Count(static h => h.Guild == "red");

        int paid = heroes.Where(static h => h.IsActive).ExecuteUpdate(Reward, static (set, reward) => set.Set(h => h.Gold, h => h.Gold + reward));
        int scaled = heroes.Where(10, static (h, max) => h.Level <= max).ExecuteUpdate((Factor: 2, Speed: 0.5f), static (set, s) => set
            .Set(h => h.Score, h => (h.Score - 1) * s.Factor)
            .Set(h => h.Speed, h => h.Speed * s.Speed));
        int renamed = heroes.Where(48, static (h, min) => h.Level >= min).ExecuteUpdate(static set => set.Set(h => h.Name, "veteran").SetNull(h => h.Score));
        int cleared = heroes.Where(static h => h.Guild == "red").ExecuteUpdate(noGuild, static (set, g) => set.Set(h => h.Guild, g).Set(h => h.IsActive, true));
        int pinned = heroes.Where(5, static (h, id) => h.Id == id).ExecuteUpdate(HeroLimits.Elite, static (set, level) => set.Set(h => h.Level, level).Set(h => h.Tag, Guid.Empty));

        foreach (Hero hero in _heroes)
        {
            if (hero.IsActive)
            {
                hero.Gold += Reward;
            }

            if (hero.Level <= 10)
            {
                hero.Score = (hero.Score - 1) * 2;
                hero.Speed *= 0.5f;
            }

            if (hero.Level >= 48)
            {
                hero.Name = "veteran";
                hero.Score = null;
            }

            if (hero.Guild == "red")
            {
                hero.Guild = null;
                hero.IsActive = true;
            }

            if (hero.Id == 5)
            {
                hero.Level = HeroLimits.Elite;
                hero.Tag = Guid.Empty;
            }
        }

        Assert.Equal(active, paid);
        Assert.Equal(10, scaled);
        Assert.Equal(3, renamed);
        Assert.Equal(reds, cleared);
        Assert.Equal(1, pinned);
        Assert.Equal(0, heroes.Where(-1, static (h, id) => h.Id == id).ExecuteUpdate(static set => set.Set(h => h.Gold, 0L)));
        List<Hero> stored = heroes.OrderBy(static h => h.Id).ToList();
        Assert.Equal(_heroes.Count, stored.Count);
        for (int i = 0; i < stored.Count; i++)
        {
            AssertSame(_heroes[i], stored[i]);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ExecuteUpdate_WithAnUncompiledFilter_UpdatesEachMatchingKey(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        int threshold = 40;

#pragma warning disable DATAVOL001 // the filter runs in C# on purpose
        int changed = db.Table<Hero>().Where(h => h.Level > threshold).ExecuteUpdate(1L, static (set, one) => set.Set(h => h.Gold, h => h.Gold * 2 + one));
#pragma warning restore DATAVOL001
        int none = db.Table<Hero>().Where((int?)null, static (h, s) => h.Score == s && h.Level > 100).ExecuteUpdate(static set => set.Set(h => h.Gold, 0L));

        Assert.Equal(10, changed);
        Assert.Equal(0, none);
        Assert.Equal(_heroes.Select(h => h.Level > threshold ? h.Gold * 2 + 1 : h.Gold), db.Table<Hero>().OrderBy(static h => h.Id).Select(static h => h.Gold));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BulkChanges_JoinTheOpenTransaction_AndRollBackWithIt(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();
        int threshold = 25;
        long total = _heroes.Sum(static h => h.Gold);

        db.Execute("BEGIN");
        Assert.Equal(25, heroes.Where(static h => !h.IsActive).ExecuteDelete());
        Assert.Equal(25, heroes.ExecuteUpdate(static set => set.Set(h => h.Gold, h => h.Gold + 1L)));
#pragma warning disable DATAVOL001 // per-key path inside the caller's transaction
        Assert.Equal(13, heroes.Where(h => h.Level > threshold).ExecuteDelete());
#pragma warning restore DATAVOL001
        Assert.Equal(12, heroes.Count());
        db.Execute("ROLLBACK");

        Assert.Equal(50, heroes.Count());
        Assert.Equal(total, heroes.Select(static h => h.Gold).ToList().Sum());

        db.Execute("BEGIN");
        Assert.Equal(50, heroes.ExecuteUpdate(7L, static (set, add) => set.Set(h => h.Gold, h => h.Gold + add)));
        db.Execute("COMMIT");
        Assert.Equal(total + (50 * 7), heroes.Select(static h => h.Gold).ToList().Sum());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void FailedBulkChange_LeavesEveryRowAsItWas(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Hero> heroes = db.Table<Hero>();
        string? missing = null;
        int threshold = 0;

        Assert.ThrowsAny<Exception>(() => heroes.Where(static h => h.IsActive).ExecuteUpdate(missing, static (set, name) => set.Set(h => h.Name, name!)));
#pragma warning disable DATAVOL001 // per-key path: the failure must roll back every key already written
        Assert.ThrowsAny<Exception>(() => heroes.Where(h => h.Level > threshold).ExecuteUpdate(missing, static (set, name) => set.Set(h => h.Name, name!)));
#pragma warning restore DATAVOL001

        Assert.Equal(_heroes.Select(static h => h.Name), heroes.OrderBy(static h => h.Id).Select(static h => h.Name));
        Assert.Equal(50, heroes.Where(static h => h.Name != "").Count());
    }

    [Fact]
    public void UncompiledExecuteUpdate_ThrowsAndSaysWhy()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        Func<Func<DataVoSetters<Hero>, DataVoSetters<Hero>>, int> notIntercepted = db.Table<Hero>().ExecuteUpdate;

        var error = Assert.Throws<NotSupportedException>(() => notIntercepted(static set => set.Set(h => h.Gold, 0L)));
        Assert.Contains("InterceptorsNamespaces", error.Message);
        Assert.Equal(_heroes.Select(static h => h.Gold), db.Table<Hero>().OrderBy(static h => h.Id).Select(static h => h.Gold));
    }

    private static void Same(IEnumerable<Hero> actual, IEnumerable<Hero> expected)
    {
        int[] want = [.. expected.Select(static h => h.Id)];
        Assert.Equal(want, actual.Select(static h => h.Id));
        Assert.Equal(want.Length, actual.Count());
    }

    private static void Projected<TResult>(DataVoProjectedQuery<Hero, Func<Hero, bool>, TResult> query, IEnumerable<TResult> expected, string sql)
    {
        Assert.True(query.IsCompiled);
        Assert.Equal(sql, query.Sql);
        Assert.Equal(expected, query.ToList());
        Assert.Equal(expected, query);
        Assert.Equal(expected.First(), query.FirstOrDefault());
        Assert.Equal(expected.Count(), query.Count());
        Assert.Equal(expected.Skip(48), query.Skip(48).ToList());
    }

    private static void Projected<TResult, TKey>(DataVoProjectedQuery<Hero, Func<Hero, bool>, TResult> query, IEnumerable<TResult> expected, string sql, Func<TResult, TKey> key)
    {
        Assert.True(query.IsCompiled);
        Assert.Equal(sql, query.Sql);
        Assert.Equal(expected.Select(key), query.ToList().Select(key));
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

        string?[] guilds = ["red", "blue", null, "Amber", "amber"];
        for (int id = 1; id <= 50; id++)
        {
            var hero = new Hero
            {
                Id = id,
                Name = $"hero-{id}",
                Level = id,
                IsActive = id % 2 == 0,
                Score = id % 5 == 0 ? null : id % 7 * 10,
                Guild = guilds[id % 3 == 0 ? 2 : id % 5],
                Gold = (id * 37 % 50) * 100L,
                Speed = id % 4 + 0.5f,
                Tag = new Guid(id, (short)(id * 7), (short)(id % 3), [(byte)(51 - id), 0, 0, 0, 0, 0, 0, 0]),
                Joined = new DateOnly(2024, 1, 1).AddDays(id * 13 % 40),
            };
            _heroes.Add(hero);
            db.Insert(hero);
        }

        return db;
    }
}
