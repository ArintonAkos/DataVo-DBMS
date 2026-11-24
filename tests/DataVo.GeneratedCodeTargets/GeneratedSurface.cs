using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.CompiledQueries;

namespace DataVo.GeneratedCodeTargets;

/// <summary>A model using every portable column type, so the generated table code covers each reader.</summary>
[DataVoTable("target_players")]
public sealed class TargetPlayer
{
    [PrimaryKey] public int Id { get; set; }

    public long Score { get; set; }

    public float Health { get; set; }

    public double Ratio { get; set; }

    public bool Alive { get; set; }

    public Guid Tag { get; set; }

    public DataVoDate Joined { get; set; }

    public DateTime LastSeen { get; set; }

    public DateTimeOffset Created { get; set; }

    public DateTime? Banned { get; set; }

    public int? Bonus { get; set; }

    public string Name { get; set; } = "";

    [Indexed] public string Region { get; set; } = "";
}

/// <summary>A projection read by a [DataVoQuery] method.</summary>
public sealed record TargetRow(int Id, string Name, DateTime LastSeen, DateTimeOffset Created, DateTime? Banned);

/// <summary>Compiled queries over the model: predicates, folds, projections and a generated SQL method.</summary>
public static partial class GeneratedSurface
{
    [DataVoQuery("SELECT Id, Name, LastSeen, Created, Banned FROM target_players WHERE Id = @id")]
    public static partial IReadOnlyList<TargetRow> ById(DataVoContext db, int id);

    /// <summary>Runs every compiled shape once.</summary>
    public static long Exercise(DataVoContext db, DateTime since)
    {
        db.EnsureTargetPlayerTable();
        db.Insert(new TargetPlayer { Id = 1, Name = "a", Region = "eu", LastSeen = since, Created = DateTimeOffset.UnixEpoch });
        DataVoTable<TargetPlayer> players = db.Table<TargetPlayer>();

        long total = players.Where(since, static (p, s) => p.LastSeen >= s).Count();
        total += players.Where(static p => p.Alive && p.Score > 10).Count();
        total += players.Where(static p => p.Region == "eu").Max(static p => p.Score);
        total += players.Where(static p => p.Id > 0).Max(static p => p.LastSeen).Ticks;
        total += players.Where(static p => p.Health < 0.5f).Select(static p => new { p.Id, p.LastSeen }).Count();
        total += ById(db, 1).Count;
        return total;
    }
}
