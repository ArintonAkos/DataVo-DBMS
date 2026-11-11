using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using DataVo.Core;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;

namespace DataVo.Benchmarks;

/// <summary>
/// One aggregate over the rows that meet a condition: <c>... FROM players WHERE Level &gt;= @min</c>, with the
/// value bound on every call. Half of the rows match. DataVo runs its compiled LINQ query; SQLite and DuckDB run a
/// statement prepared once.
/// </summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
[CategoriesColumn]
public class FilteredAggregateBenchmarks
{
    private const int MinLevel = 50;

    private ScratchDirectory _scratch = null!;
    private DataVoContext _dataVo = null!;
    private DataVoTable<Player> _players = null!;
    private SqliteConnection _sqlite = null!;
    private DuckDBConnection _duck = null!;
    private SqliteCommand _sqliteCount = null!, _sqliteSum = null!, _sqliteMin = null!, _sqliteMax = null!;
    private DuckDBCommand _duckCount = null!, _duckSum = null!, _duckMin = null!, _duckMax = null!;

    [Params(200_000)]
    public int Rows { get; set; }

    [ParamsAllValues]
    public Storage Storage { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _scratch = new ScratchDirectory();
        _dataVo = Engines.OpenDataVo(Storage, _scratch.Path);
        Engines.Load(_dataVo, 1, Rows);
        _players = _dataVo.Table<Player>();

        _sqlite = Engines.OpenSqlite(Storage, _scratch.Path);
        Engines.Load(_sqlite, 1, Rows);
        _sqliteCount = Prepare(_sqlite, "SELECT COUNT(*) FROM players WHERE Level >= $min");
        _sqliteSum = Prepare(_sqlite, "SELECT SUM(Gold) FROM players WHERE Level >= $min");
        _sqliteMin = Prepare(_sqlite, "SELECT MIN(Gold) FROM players WHERE Level >= $min");
        _sqliteMax = Prepare(_sqlite, "SELECT MAX(Ratio) FROM players WHERE Level >= $min");

        _duck = Engines.OpenDuckDb(Storage, _scratch.Path);
        Engines.Load(_duck, 1, Rows);
        _duckCount = Prepare(_duck, "SELECT COUNT(*) FROM players WHERE Level >= $min");
        _duckSum = Prepare(_duck, "SELECT CAST(SUM(Gold) AS BIGINT) FROM players WHERE Level >= $min");
        _duckMin = Prepare(_duck, "SELECT MIN(Gold) FROM players WHERE Level >= $min");
        _duckMax = Prepare(_duck, "SELECT MAX(Ratio) FROM players WHERE Level >= $min");

        // Every engine must agree before anything is timed.
        long count = DataVo_Count();
        long sum = DataVo_Sum();
        long min = DataVo_Min();
        double max = DataVo_Max();
        if (count != Sqlite_Count() || count != DuckDb_Count()
            || sum != Sqlite_Sum() || sum != DuckDb_Sum()
            || min != Sqlite_Min() || min != DuckDb_Min()
            || max != Sqlite_Max() || max != DuckDb_Max())
        {
            throw new InvalidOperationException("The engines disagree on the answer.");
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sqliteCount.Dispose();
        _sqliteSum.Dispose();
        _sqliteMin.Dispose();
        _sqliteMax.Dispose();
        _duckCount.Dispose();
        _duckSum.Dispose();
        _duckMin.Dispose();
        _duckMax.Dispose();
        _dataVo.Dispose();
        _sqlite.Dispose();
        _duck.Dispose();
        _scratch.Dispose();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Count")]
    public long DataVo_Count() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Count();

    [Benchmark, BenchmarkCategory("Count")]
    public long Sqlite_Count() => (long)_sqliteCount.ExecuteScalar()!;

    [Benchmark, BenchmarkCategory("Count")]
    public long DuckDb_Count() => (long)_duckCount.ExecuteScalar()!;

    [Benchmark(Baseline = true), BenchmarkCategory("Sum")]
    public long DataVo_Sum() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Sum(static p => p.Gold);

    [Benchmark, BenchmarkCategory("Sum")]
    public long Sqlite_Sum() => (long)_sqliteSum.ExecuteScalar()!;

    [Benchmark, BenchmarkCategory("Sum")]
    public long DuckDb_Sum() => (long)_duckSum.ExecuteScalar()!;

    [Benchmark(Baseline = true), BenchmarkCategory("Min")]
    public long DataVo_Min() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Min(static p => p.Gold);

    [Benchmark, BenchmarkCategory("Min")]
    public long Sqlite_Min() => (long)_sqliteMin.ExecuteScalar()!;

    [Benchmark, BenchmarkCategory("Min")]
    public long DuckDb_Min() => (long)_duckMin.ExecuteScalar()!;

    [Benchmark(Baseline = true), BenchmarkCategory("Max")]
    public double DataVo_Max() => _players.Where(MinLevel, static (p, min) => p.Level >= min).Max(static p => p.Ratio);

    [Benchmark, BenchmarkCategory("Max")]
    public double Sqlite_Max() => (double)_sqliteMax.ExecuteScalar()!;

    [Benchmark, BenchmarkCategory("Max")]
    public double DuckDb_Max() => (double)_duckMax.ExecuteScalar()!;

    private static SqliteCommand Prepare(SqliteConnection connection, string sql)
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$min", MinLevel);
        command.Prepare();
        return command;
    }

    private static DuckDBCommand Prepare(DuckDBConnection connection, string sql)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter("min", MinLevel));
        command.Prepare();
        return command;
    }
}
