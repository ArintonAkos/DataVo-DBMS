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
