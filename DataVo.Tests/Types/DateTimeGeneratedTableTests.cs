using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Types;

[DataVoTable("timed_events")]
public sealed class TimedEvent
{
    [PrimaryKey] public int Id { get; set; }

    public DateTime At { get; set; }

    public DateTimeOffset Logged { get; set; }

    public DateTime? Closed { get; set; }

    public DateTimeOffset? Acknowledged { get; set; }

    public string Name { get; set; } = "";
}

/// <summary>
/// <see cref="DateTime"/> and <see cref="DateTimeOffset"/> members of a generated table map to DATETIME columns: rows
/// round-trip to the tick as UTC instants, and compiled predicates, Min and Max work on them in every engine.
/// </summary>
public sealed class DateTimeGeneratedTableTests : IDisposable
{
    private static readonly DateTime Base = new(2026, 11, 24, 8, 0, 0, DateTimeKind.Utc);
    private readonly string _root = TestStorageDirectories.Create("datetime_generated");

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Members_RoundTrip_AndCompiledQueriesFilterAndFold(StorageMode mode)
    {
        using DataVoContext db = new(new DataVoConfig { StorageMode = mode, DiskStoragePath = Path.Combine(_root, mode.ToString()), LsmStrictFsync = false });
        Assert.False(db.Execute("CREATE DATABASE T; USE T;").Last().IsError);
        db.EnsureTimedEventTable();

        List<TimedEvent> events = [.. Enumerable.Range(1, 500).Select(Event)];
        foreach (TimedEvent e in events)
        {
            db.Insert(e);
        }

        DataVoTable<TimedEvent> table = db.Table<TimedEvent>();

        TimedEvent stored = table.Where(7, static (e, id) => e.Id == id).Single();
        Assert.Equal(events[6].At, stored.At);
        Assert.Equal(DateTimeKind.Utc, stored.At.Kind);
        Assert.Equal(events[6].Logged.UtcTicks, stored.Logged.UtcTicks);
        Assert.Equal(TimeSpan.Zero, stored.Logged.Offset);
        Assert.Equal(events[6].Closed, stored.Closed);
        Assert.Equal(events[6].Acknowledged?.UtcTicks, stored.Acknowledged?.UtcTicks);

        DateTime cutoff = Base.AddMinutes(400);
        var late = table.Where(cutoff, static (e, after) => e.At > after);
        Assert.True(late.IsCompiled);
        Assert.Equal(events.Count(e => e.At > cutoff), late.Count());

        var all = table.Where(static e => e.Id > 0);
        Assert.Equal(events.Min(e => e.At), all.Min(static e => e.At));
        Assert.Equal(events.Max(e => e.At), all.Max(static e => e.At));
        Assert.Equal(events.Max(e => e.Logged).UtcTicks, all.Max(static e => e.Logged).UtcTicks);

        TimedEvent changed = events[9];
        changed.Closed = Base.AddDays(3).AddTicks(42);
        Assert.True(db.Update(changed));
        Assert.Equal(changed.Closed, table.Where(10, static (e, id) => e.Id == id).Single().Closed);
    }

    private static TimedEvent Event(int i) => new()
    {
        Id = i,
        At = Base.AddMinutes(i).AddTicks(i),
        Logged = new DateTimeOffset(Base.AddSeconds(i)).ToOffset(TimeSpan.FromHours(i % 5 - 2)),
        Closed = i % 3 == 0 ? null : Base.AddHours(i),
        Acknowledged = i % 4 == 0 ? null : new DateTimeOffset(Base.AddHours(i * 2)),
        Name = $"event-{i}",
    };
}
