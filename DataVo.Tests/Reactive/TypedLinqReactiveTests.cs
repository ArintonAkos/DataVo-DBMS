using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>Model used to exercise strongly-typed LINQ reactive subscriptions.</summary>
[DataVoTable("reactive_players")]
public sealed class ReactivePlayer
{
    [PrimaryKey] public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    [Indexed] public string Team { get; set; } = string.Empty;

    [Indexed] public int Score { get; set; }
}

/// <summary>Value-type HUD projection for zero-allocation <c>SubscribeInto</c> tests.</summary>
public readonly record struct ReactiveHudSlot(int Id, string Name, int Score);

/// <summary>Pure primitive value-type HUD slot for strict 0-byte allocation verification.</summary>
public readonly record struct NumericHudSlot(int Id, int Score);

/// <summary>
/// End-to-end and zero-allocation verification of typed LINQ reactive subscriptions
/// (<c>Subscribe</c> and <c>SubscribeInto</c>) on <c>db.Table&lt;T&gt;()</c>.
/// </summary>
[Collection(AllocationSensitiveCollection.Name)]
public sealed class TypedLinqReactiveTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_linq_reactive_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private DataVoContext Open(StorageMode mode)
    {
        string dbDir = Path.Combine(_directory, $"{mode}_{Guid.NewGuid():N}");
        var config = new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = dbDir,
        };

        var ctx = new DataVoContext(config);
        ctx.Execute("CREATE DATABASE Game");
        ctx.Execute("USE Game");
        ctx.EnsureReactivePlayerTable();
        return ctx;
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TypedLinq_WhereOrderByTake_Subscribe_DeliversTypedTopKDeltas(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        var live = new Dictionary<int, ReactivePlayer>();
        int updatedCount = 0;

        using IDisposable sub = db.Table<ReactivePlayer>()
            .Where(100, static (p, minScore) => p.Score >= minScore)
            .OrderByDescending(static p => p.Score)
            .Take(3)
            .Subscribe(change =>
            {
                foreach (ReactivePlayer p in change.Added)
                {
                    live[p.Id] = p;
                }

                foreach (ReactivePlayer p in change.Updated)
                {
                    live[p.Id] = p;
                    updatedCount++;
                }

                foreach (ReactivePlayer p in change.Removed)
                {
                    live.Remove(p.Id);
                }
            });

        db.Insert(new ReactivePlayer { Id = 1, Name = "Ada", Team = "Red", Score = 110 });
        db.Insert(new ReactivePlayer { Id = 2, Name = "Bob", Team = "Red", Score = 80 });  // below threshold
        db.Insert(new ReactivePlayer { Id = 3, Name = "Lin", Team = "Blue", Score = 250 });
        db.Insert(new ReactivePlayer { Id = 4, Name = "Zed", Team = "Blue", Score = 180 });
        db.DispatchPendingNotifications();

        Assert.Equal([1, 3, 4], live.Keys.OrderBy(static id => id).ToArray());

        // Inserting a higher-scoring player displaces Id=1 (Score 110).
        db.Insert(new ReactivePlayer { Id = 5, Name = "Kai", Team = "Red", Score = 300 });
        db.DispatchPendingNotifications();

        Assert.Equal([3, 4, 5], live.Keys.OrderBy(static id => id).ToArray());
        Assert.Equal(300, live[5].Score);

        // Updating an in-window player delivers Updated with the new score.
        db.Update(new ReactivePlayer { Id = 4, Name = "Zed", Team = "Blue", Score = 275 });
        db.DispatchPendingNotifications();

        Assert.Equal(275, live[4].Score);
        Assert.True(updatedCount >= 1);

        // Deleting top player promotes Id=1 back into the top 3.
        db.Table<ReactivePlayer>().Where(5, static (p, id) => p.Id == id).ExecuteDelete();
        db.DispatchPendingNotifications();

        Assert.Equal([1, 3, 4], live.Keys.OrderBy(static id => id).ToArray());
    }

    [Fact]
    public void TypedLinq_WhereOnly_Subscribe_TracksInsertUpdateAndDeleteTransitions()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        var added = new List<int>();
        var updated = new List<(int Id, int BeforeScore, int AfterScore)>();
        var removed = new List<int>();

        using IDisposable sub = db.Table<ReactivePlayer>()
            .Where(100, static (p, min) => p.Score >= min)
            .Subscribe(change =>
            {
                foreach (ReactivePlayer p in change.Added)
                {
                    added.Add(p.Id);
                }

                for (int i = 0; i < change.Updated.Count; i++)
                {
                    updated.Add((change.Updated[i].Id, change.UpdatedBefore[i].Score, change.Updated[i].Score));
                }

                foreach (ReactivePlayer p in change.Removed)
                {
                    removed.Add(p.Id);
                }
            });

        // Insert qualifying + non-qualifying rows.
        db.Insert(new ReactivePlayer { Id = 1, Name = "Ada", Team = "Red", Score = 120 });
        db.Insert(new ReactivePlayer { Id = 2, Name = "Bob", Team = "Red", Score = 50 });
        db.DispatchPendingNotifications();
        Assert.Equal([1], added);

        // Stay transition (120 -> 180) delivers Updated + UpdatedBefore.
        db.Update(new ReactivePlayer { Id = 1, Name = "Ada", Team = "Red", Score = 180 });
        // Enter transition (50 -> 110) delivers Added.
        db.Update(new ReactivePlayer { Id = 2, Name = "Bob", Team = "Red", Score = 110 });
        db.DispatchPendingNotifications();
        Assert.Equal([1, 2], added);
        Assert.Equal([(1, 120, 180)], updated);

        // Leave transition (110 -> 40) + physical Delete of Id=1 both deliver Removed.
        db.Update(new ReactivePlayer { Id = 2, Name = "Bob", Team = "Red", Score = 40 });
        db.Table<ReactivePlayer>().Where(1, static (p, id) => p.Id == id).ExecuteDelete();
        db.DispatchPendingNotifications();
        Assert.Equal([1, 2], removed.OrderBy(static id => id).ToArray());
    }

    [Fact]
    public void SubscribeInto_PrimitiveHudSlot_TracksPartialWindowRankSwapsDeletesAndClamping()
    {
        using DataVoContext db = Open(StorageMode.InMemory);

        // 1. Full Top-3 window into NumericHudSlot[3] (no string fields): verify count < 3, rank swaps, and deletes.
        var top3 = new NumericHudSlot[3];
        int top3Count = 0;
        using IDisposable subTop3 = db.Table<ReactivePlayer>()
            .Where(100, static (p, min) => p.Score >= min)
            .OrderByDescending(static p => p.Score)
            .Take(3)
            .Select(static p => new NumericHudSlot(p.Id, p.Score))
            .SubscribeInto(top3.AsMemory(), count => top3Count = count);

        // 2. Clamping verification: Take(5) into destination.Length == 2.
        var clamped = new NumericHudSlot[2];
        int clampedCount = 0;
        using IDisposable subClamped = db.Table<ReactivePlayer>()
            .Where(100, static (p, min) => p.Score >= min)
            .OrderByDescending(static p => p.Score)
            .Take(5)
            .Select(static p => new NumericHudSlot(p.Id, p.Score))
            .SubscribeInto(clamped.AsMemory(), count => clampedCount = count);

        // count < 3: 1 matching item.
        db.Insert(new ReactivePlayer { Id = 1, Name = "A", Team = "Red", Score = 150 });
        db.DispatchPendingNotifications();
        Assert.Equal(1, top3Count);
        Assert.Equal(new NumericHudSlot(1, 150), top3[0]);
        Assert.Equal(1, clampedCount);

        // count < 3: 2 matching items in descending order.
        db.Insert(new ReactivePlayer { Id = 2, Name = "B", Team = "Red", Score = 250 });
        db.DispatchPendingNotifications();
        Assert.Equal(2, top3Count);
        Assert.Equal([new NumericHudSlot(2, 250), new NumericHudSlot(1, 150)], top3[..2]);
        Assert.Equal(2, clampedCount);

        // 4 matching items -> top3 holds 3, clamped holds destination.Length == 2.
        db.Insert(new ReactivePlayer { Id = 3, Name = "C", Team = "Red", Score = 200 });
        db.Insert(new ReactivePlayer { Id = 4, Name = "D", Team = "Red", Score = 120 });
        db.DispatchPendingNotifications();
        Assert.Equal(3, top3Count);
        Assert.Equal([new NumericHudSlot(2, 250), new NumericHudSlot(3, 200), new NumericHudSlot(1, 150)], top3);
        Assert.Equal(2, clampedCount);
        Assert.Equal([new NumericHudSlot(2, 250), new NumericHudSlot(3, 200)], clamped);

        // Rank swap: promote Id=1 above Id=2 (150 -> 300).
        db.Update(new ReactivePlayer { Id = 1, Name = "A", Team = "Red", Score = 300 });
        db.DispatchPendingNotifications();
        Assert.Equal(3, top3Count);
        Assert.Equal([new NumericHudSlot(1, 300), new NumericHudSlot(2, 250), new NumericHudSlot(3, 200)], top3);
        Assert.Equal([new NumericHudSlot(1, 300), new NumericHudSlot(2, 250)], clamped);

        // Delete top row (Id=1) -> Id=4 (120) promoted into slot 2.
        db.Table<ReactivePlayer>().Where(1, static (p, id) => p.Id == id).ExecuteDelete();
        db.DispatchPendingNotifications();
        Assert.Equal(3, top3Count);
        Assert.Equal([new NumericHudSlot(2, 250), new NumericHudSlot(3, 200), new NumericHudSlot(4, 120)], top3);

        // Delete two more -> count drops below 3 to 1.
        db.Table<ReactivePlayer>().Where(200, static (p, min) => p.Score >= min).ExecuteDelete();
        db.DispatchPendingNotifications();
        Assert.Equal(1, top3Count);
        Assert.Equal(new NumericHudSlot(4, 120), top3[0]);
        Assert.Equal(1, clampedCount);
    }

    [Fact]
    public void TypedLinq_ConcurrentSubscriptions_WithDifferentStates_AreIsolated()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        var redIds = new List<int>();
        var blueIds = new List<int>();

        IDisposable WatchTeam(string team, List<int> sink) =>
            db.Table<ReactivePlayer>()
                .Where(team, static (p, t) => p.Team == t)
                .Subscribe(change =>
                {
                    foreach (ReactivePlayer p in change.Added)
                    {
                        sink.Add(p.Id);
                    }
                });

        using IDisposable subRed = WatchTeam("Red", redIds);
        using IDisposable subBlue = WatchTeam("Blue", blueIds);

        db.Insert(new ReactivePlayer { Id = 1, Name = "Ada", Team = "Red", Score = 100 });
        db.Insert(new ReactivePlayer { Id = 2, Name = "Lin", Team = "Blue", Score = 200 });
        db.DispatchPendingNotifications();

        Assert.Equal([1], redIds);
        Assert.Equal([2], blueIds);
    }

    [Fact]
    public void TypedLinq_Select_Subscribe_DeliversTypedProjections()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        var added = new List<ReactiveHudSlot>();

        using IDisposable sub = db.Table<ReactivePlayer>()
            .Where(50, static (p, min) => p.Score >= min)
            .OrderByDescending(static p => p.Score)
            .Take(2)
            .Select(static p => new ReactiveHudSlot(p.Id, p.Name, p.Score))
            .Subscribe(change =>
            {
                foreach (ReactiveHudSlot slot in change.Added)
                {
                    added.Add(slot);
                }
            });

        db.Insert(new ReactivePlayer { Id = 1, Name = "Ada", Team = "Red", Score = 90 });
        db.Insert(new ReactivePlayer { Id = 2, Name = "Lin", Team = "Blue", Score = 40 });
        db.DispatchPendingNotifications();

        ReactiveHudSlot single = Assert.Single(added);
        Assert.Equal(new ReactiveHudSlot(1, "Ada", 90), single);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SubscribeInto_OrderByTakeSelect_WritesSortedLiveWindowInPlaceAndClamps(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        // Seed one initial row before subscribing to confirm seed is baseline and not delivered early.
        db.Insert(new ReactivePlayer { Id = 10, Name = "Seed", Team = "Red", Score = 120 });

        var buffer = new ReactiveHudSlot[2]; // intentionally smaller than Take(3) to verify clamping
        int lastCount = -1;
        int invocations = 0;

        using IDisposable sub = db.Table<ReactivePlayer>()
            .Where(100, static (p, min) => p.Score >= min)
            .OrderByDescending(static p => p.Score)
            .Take(3)
            .Select(static p => new ReactiveHudSlot(p.Id, p.Name, p.Score))
            .SubscribeInto(buffer.AsMemory(), count =>
            {
                lastCount = count;
                invocations++;
            });

        // Nothing delivered until DispatchPendingNotifications after a post-subscribe change.
        Assert.Equal(-1, lastCount);
        Assert.Equal(0, invocations);

        // Insert below threshold -> window unchanged, callback must NOT fire.
        db.Insert(new ReactivePlayer { Id = 1, Name = "Low", Team = "Red", Score = 50 });
        db.DispatchPendingNotifications();
        Assert.Equal(0, invocations);

        // Insert 2 qualifying players -> top 2 clamped to buffer.Length (2) in descending Score order.
        db.Insert(new ReactivePlayer { Id = 2, Name = "Mid", Team = "Red", Score = 200 });
        db.Insert(new ReactivePlayer { Id = 3, Name = "Top", Team = "Blue", Score = 350 });
        db.DispatchPendingNotifications();

        Assert.Equal(2, lastCount);
        Assert.Equal(new ReactiveHudSlot(3, "Top", 350), buffer[0]);
        Assert.Equal(new ReactiveHudSlot(2, "Mid", 200), buffer[1]);

        // Update an out-of-window qualifying row (Id=10, Score 120 -> 130) -> visible top-2 unchanged!
        int beforeUnrelated = invocations;
        db.Update(new ReactivePlayer { Id = 10, Name = "Seed", Team = "Red", Score = 130 });
        db.DispatchPendingNotifications();
        Assert.Equal(beforeUnrelated, invocations);

        // Promote Id=10 above Id=2 -> window updates in place to [Top(350), Seed(250)].
        db.Update(new ReactivePlayer { Id = 10, Name = "Seed", Team = "Red", Score = 250 });
        db.DispatchPendingNotifications();

        Assert.Equal(2, lastCount);
        Assert.Equal(new ReactiveHudSlot(3, "Top", 350), buffer[0]);
        Assert.Equal(new ReactiveHudSlot(10, "Seed", 250), buffer[1]);

        // Delete all qualifying rows -> callback fires with count == 0.
        db.Table<ReactivePlayer>().Where(100, static (p, min) => p.Score >= min).ExecuteDelete();
        db.DispatchPendingNotifications();
        Assert.Equal(0, lastCount);
    }

    [Fact]
    public void SubscribeInto_BoundedWhereWithoutOrderBy_ClampsToDestinationLength()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        var buffer = new NumericHudSlot[2];
        int lastCount = 0;

        using IDisposable sub = db.Table<ReactivePlayer>()
            .Where("Red", static (p, team) => p.Team == team)
            .Select(static p => new NumericHudSlot(p.Id, p.Score))
            .SubscribeInto(buffer.AsMemory(), count => lastCount = count);

        db.Insert(new ReactivePlayer { Id = 3, Name = "C", Team = "Red", Score = 30 });
        db.Insert(new ReactivePlayer { Id = 1, Name = "A", Team = "Red", Score = 10 });
        db.Insert(new ReactivePlayer { Id = 2, Name = "B", Team = "Red", Score = 20 });
        db.DispatchPendingNotifications();

        // Ordered deterministically by primary key and clamped to destination length (2).
        Assert.Equal(2, lastCount);
        Assert.Equal(new NumericHudSlot(1, 10), buffer[0]);
        Assert.Equal(new NumericHudSlot(2, 20), buffer[1]);
    }

    [Fact]
    public void SubscribeInto_DirectModelWindow_WorksWithoutSelect()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        var buffer = new ReactivePlayer[2];
        int lastCount = 0;

        using IDisposable sub = db.Table<ReactivePlayer>()
            .OrderByDescending(static p => p.Score)
            .Take(2)
            .SubscribeInto(buffer.AsMemory(), count => lastCount = count);

        db.Insert(new ReactivePlayer { Id = 1, Name = "A", Team = "Red", Score = 10 });
        db.Insert(new ReactivePlayer { Id = 2, Name = "B", Team = "Red", Score = 50 });
        db.Insert(new ReactivePlayer { Id = 3, Name = "C", Team = "Red", Score = 30 });
        db.DispatchPendingNotifications();

        Assert.Equal(2, lastCount);
        Assert.Equal(2, buffer[0].Id);
        Assert.Equal(3, buffer[1].Id);
    }

    [Fact]
    public void SubscribeInto_DispatchPendingNotifications_AllocatesZeroBytesInSteadyState()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        var buffer = new NumericHudSlot[3];
        int lastCount = 0;
        long checksum = 0;

        var query = db.Table<ReactivePlayer>()
            .Where(100, static (p, minScore) => p.Score >= minScore)
            .OrderByDescending(static p => p.Score)
            .Take(3)
            .Select(static p => new NumericHudSlot(p.Id, p.Score));

        Assert.True(query.IsCompiled);

        using IDisposable sub = query.SubscribeInto(buffer.AsMemory(), count =>
        {
            lastCount = count;
            for (int i = 0; i < count; i++)
            {
                checksum += buffer[i].Score;
            }
        });

        // Pre-seed 3 rows so the window is full and slab/dictionary capacity is warmed.
        var schema = new ReactiveRowSchema("Id", "Name", "Team", "Score");
        ChangeSet MakeInsert(int id, int score) => new(
            id,
            "Game",
            [new RowChange("reactive_players", id, TypedRow.FromOwnedCells(schema, [
                CellValue.From(id),
                CellValue.From("P"),
                CellValue.From("Red"),
                CellValue.From(score),
            ]))]);

        // Alternating updates to Id=1 (score 200 <-> 400) force a rank swap inside the Top-3 window on every drain.
        ChangeSet rankLow = MakeInsert(1, 200);
        ChangeSet rankHigh = MakeInsert(1, 400);

        db.Changes.Publish(MakeInsert(1, 200));
        db.Changes.Publish(MakeInsert(2, 300));
        db.Changes.Publish(MakeInsert(3, 250));
        db.DispatchPendingNotifications();

        // Warm up JIT and internal buffers.
        for (int i = 0; i < 500; i++)
        {
            db.Changes.Publish((i & 1) == 0 ? rankHigh : rankLow);
            db.DispatchPendingNotifications();
        }

        // Measure ONLY DispatchPendingNotifications() (plus Publish of pre-built ChangeSet) over 1,000 live rank swaps.
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
        {
            db.Changes.Publish((i & 1) == 0 ? rankHigh : rankLow);
            db.DispatchPendingNotifications();
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(3, lastCount);
        Assert.True(checksum > 0);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void TypedLinq_DisposeStopsDelivery()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        var buffer = new NumericHudSlot[2];
        int calls = 0;

        IDisposable sub = db.Table<ReactivePlayer>()
            .Select(static p => new NumericHudSlot(p.Id, p.Score))
            .SubscribeInto(buffer.AsMemory(), _ => calls++);

        db.Insert(new ReactivePlayer { Id = 1, Name = "A", Team = "Red", Score = 10 });
        db.DispatchPendingNotifications();
        Assert.Equal(1, calls);

        sub.Dispose();
        db.Insert(new ReactivePlayer { Id = 2, Name = "B", Team = "Red", Score = 20 });
        db.DispatchPendingNotifications();
        Assert.Equal(1, calls);
    }
}
