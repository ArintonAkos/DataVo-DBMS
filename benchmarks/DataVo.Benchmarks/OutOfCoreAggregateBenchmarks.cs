using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Benchmarks;

/// <summary>Where the rows of an LSM table are when a columnar query runs.</summary>
public enum Placement
{
    /// <summary>Not flushed yet: the columns are read from the columnar index in memory.</summary>
    Memory,

    /// <summary>Flushed: the columns are read from the columnar segments of the table's SSTables.</summary>
    Files,
}

/// <summary>
/// The aggregates of <see cref="FilteredAggregateBenchmarks"/> on DataVo's LSM engine alone, with the rows
/// either still in memory or all in SSTables, and the cost of opening a database and answering the first
/// aggregate from its files.
/// </summary>
/// <remarks>
/// For <see cref="Placement.Files"/> the database is closed, which flushes every row, and opened again, so
/// nothing of the table's columns is in the managed heap.
/// </remarks>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
[CategoriesColumn]
public class OutOfCoreAggregateBenchmarks
{
    private const int MinLevel = 50;

    private ScratchDirectory _scratch = null!;
    private DataVoContext _dataVo = null!;
    private DataVoTable<Player> _players = null!;

    [Params(1_000_000)]
    public int Rows { get; set; }

    [ParamsAllValues]
    public Placement Placement { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _scratch = new ScratchDirectory();
        _dataVo = Engines.OpenDataVo(Storage.File, _scratch.Path);
        Engines.Load(_dataVo, 1, Rows);
        long expected = Count(_dataVo);
        if (Placement == Placement.Files)
        {
            _dataVo.Dispose();
            _dataVo = Reopen();
        }

        _players = _dataVo.Table<Player>();
        if (Count() != expected || Sum() == 0)
        {
            throw new InvalidOperationException("The reopened database disagrees with the one that was loaded.");
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _dataVo.Dispose();
        _scratch.Dispose();
    }

    [Benchmark, BenchmarkCategory("Count")]
    public long Count() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Count();

    [Benchmark, BenchmarkCategory("Sum")]
    public long Sum() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Sum(static p => p.Gold);

    [Benchmark, BenchmarkCategory("Min")]
    public long Min() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Min(static p => p.Gold);

    [Benchmark, BenchmarkCategory("Max")]
    public double Max() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Max(static p => p.Ratio);

    /// <summary>
    /// Opens the database a second time, answers one aggregate and closes it: what a program pays before its
    /// first answer. Only measured with the rows in files; the operating system's cache holds them.
    /// </summary>
    [Benchmark, BenchmarkCategory("OpenAndSum")]
    public long OpenAndSum()
    {
        if (Placement != Placement.Files)
        {
            return 0;
        }

        _dataVo.Dispose();
        _dataVo = Reopen();
        _players = _dataVo.Table<Player>();
        return Sum();
    }

    private DataVoContext Reopen()
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Lsm,
            DiskStoragePath = Path.Combine(_scratch.Path, "datavo"),
        });
        var result = db.Execute("USE Bench")[^1];
        if (result.IsError)
        {
            throw new InvalidOperationException(string.Join(" | ", result.Messages));
        }

        return db;
    }

    private static long Count(DataVoContext db) => db.Table<Player>().Where(MinLevel, static (p, min) => p.Level >= min).Count();
}

/// <summary>Where the updated rows of an update-heavy table are when a columnar query runs.</summary>
public enum UpdatesIn
{
    /// <summary>Not flushed: every scan masks the files' older copies against the in-memory tail.</summary>
    Tail,

    /// <summary>Flushed into SSTables that overlap the older ones: masked file against file.</summary>
    Files,
}

/// <summary>
/// <see cref="OutOfCoreAggregateBenchmarks"/> on a table where a share of the rows has been updated, which is what
/// makes a columnar scan of SSTables work out which older copies a newer source replaces.
/// </summary>
/// <remarks>
/// The table is loaded and closed, so its 1,000,000 rows are in SSTables. Then the rows with <c>Level</c> at or
/// above 100 - <see cref="UpdatedPercent"/> (that share of rows, spread over the whole key range, all counted by
/// the aggregate) get <c>Gold + 1</c> twice.
/// With <see cref="UpdatesIn.Files"/> the database is closed after each round, which leaves two flushed files
/// whose keys overlap each other and the loaded file (three level-0 files, one fewer than starts a compaction);
/// with <see cref="UpdatesIn.Tail"/> both rounds stay unflushed.
/// <para>
/// <c>Sum</c> is a warm scan. With the updates in the tail it pays, on every scan, for masking the files'
/// rows against the tail's keys. <c>OpenAndSum</c> is the first scan after the files changed (here: after an
/// open), which works out the masks between overlapping files; a flush pays the same on the next scan.
/// </para>
/// </remarks>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
[CategoriesColumn]
public class OutOfCoreAggregateUpdateBenchmarks
{
    private const int Rows = 1_000_000;
    private const int MinLevel = 50;
    private const int Rounds = 2; // the loaded file plus two update files: one short of the four that start a compaction

    private ScratchDirectory _scratch = null!;
    private DataVoContext _dataVo = null!;
    private DataVoTable<Player> _players = null!;

    [Params(0, 1, 10)]
    public int UpdatedPercent { get; set; }

    [ParamsAllValues]
    public UpdatesIn Updates { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _scratch = new ScratchDirectory();
        _dataVo = Engines.OpenDataVo(Storage.File, _scratch.Path);
        Engines.Load(_dataVo, 1, Rows);
        _dataVo.Dispose();
        _dataVo = Reopen();
        _players = _dataVo.Table<Player>();
        long expectedSum = Sum();
        long updated = _players.Where(100 - UpdatedPercent, static (p, min) => p.Level >= min).Count();
        expectedSum += UpdatedPercent > 0 ? Rounds * updated : 0;
        for (int round = 0; round < Rounds && UpdatedPercent > 0; round++)
        {
            Check(_dataVo, $"UPDATE players SET Gold = Gold + 1 WHERE Level >= {100 - UpdatedPercent}");
            if (Updates == UpdatesIn.Files)
            {
                _dataVo.Dispose();
                _dataVo = Reopen();
            }
        }

        _players = _dataVo.Table<Player>();
        if (Sum() != expectedSum)
        {
            throw new InvalidOperationException($"SUM(Gold) is {Sum()}; expected {expectedSum} after {Rounds} rounds over {updated} rows.");
        }

        Console.WriteLine($"// SSTables: {Describe(Path.Combine(_scratch.Path, "datavo"))}");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _dataVo.Dispose();
        _scratch.Dispose();
    }

    [Benchmark, BenchmarkCategory("Sum")]
    public long Sum() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Sum(static p => p.Gold);

    /// <summary>Opens the database again and answers one aggregate. Only measured with the updates in files.</summary>
    [Benchmark, BenchmarkCategory("OpenAndSum")]
    public long OpenAndSum()
    {
        if (Updates != UpdatesIn.Files)
        {
            return 0;
        }

        _dataVo.Dispose();
        _dataVo = Reopen();
        _players = _dataVo.Table<Player>();
        return Sum();
    }

    private DataVoContext Reopen()
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Lsm,
            DiskStoragePath = Path.Combine(_scratch.Path, "datavo"),
        });
        Check(db, "USE Bench");
        return db;
    }

    private static void Check(DataVoContext db, string sql)
    {
        var result = db.Execute(sql)[^1];
        if (result.IsError)
        {
            throw new InvalidOperationException(string.Join(" | ", result.Messages));
        }
    }

    // The players table's SSTables, by size, so the log shows how the files lie.
    private static string Describe(string root) =>
        string.Join(", ", Directory.EnumerateFiles(root, "*.sst", SearchOption.AllDirectories)
            .Where(static f => f.Contains("players", StringComparison.OrdinalIgnoreCase))
            .Select(static f => $"{Path.GetFileName(f)} {new FileInfo(f).Length / 1024:N0} KB"));
}
