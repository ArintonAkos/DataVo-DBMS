using BenchmarkDotNet.Attributes;
using DataVo.Core;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;

namespace DataVo.Benchmarks;

/// <summary>
/// Reads one row by primary key and builds a <see cref="Player"/> from it. Keys are visited in a fixed shuffled
/// order, so successive lookups land in different parts of the table.
/// </summary>
[MemoryDiagnoser]
public class PointLookupBenchmarks
{
    private ScratchDirectory _scratch = null!;
    private DataVoContext _dataVo = null!;
    private DataVoTable<Player> _players = null!;
    private SqliteConnection _sqlite = null!;
    private SqliteCommand _sqliteGet = null!;
    private SqliteParameter _sqliteId = null!;
    private DuckDBConnection _duck = null!;
    private DuckDBCommand _duckGet = null!;
    private DuckDBParameter _duckId = null!;
    private int[] _keys = [];
    private int _next;

    [Params(100_000)]
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
        _sqliteGet = _sqlite.CreateCommand();
        _sqliteGet.CommandText = $"SELECT {Player.Columns} FROM players WHERE Id = $id";
        _sqliteId = _sqliteGet.Parameters.Add("$id", SqliteType.Integer);
        _sqliteGet.Prepare();

        _duck = Engines.OpenDuckDb(Storage, _scratch.Path);
        Engines.Load(_duck, 1, Rows);
        _duckGet = _duck.CreateCommand();
        _duckGet.CommandText = $"SELECT {Player.Columns} FROM players WHERE Id = $id";
        _duckId = new DuckDBParameter("id", 1);
        _duckGet.Parameters.Add(_duckId);
        _duckGet.Prepare();

        var random = new Random(12345);
        _keys = [.. Enumerable.Range(1, Rows)];
        random.Shuffle(_keys);

        // Every engine must return the same row before anything is timed.
        for (int i = 0; i < 3; i++)
        {
            Player expected = Player.Create(_keys[i]);
            _next = i;
            Player fromDataVo = DataVo();
            _next = i;
            Player fromSqlite = Sqlite();
            _next = i;
            Player fromDuckDb = DuckDb();
            foreach (Player found in new[] { fromDataVo, fromSqlite, fromDuckDb })
            {
                if (found.Id != expected.Id || found.Active != expected.Active || found.Level != expected.Level
                    || found.Gold != expected.Gold || found.Ratio != expected.Ratio || found.Name != expected.Name)
                {
                    throw new InvalidOperationException("An engine returned the wrong row.");
                }
            }
        }

        _next = 0;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sqliteGet.Dispose();
        _duckGet.Dispose();
        _dataVo.Dispose();
        _sqlite.Dispose();
        _duck.Dispose();
        _scratch.Dispose();
    }

    [Benchmark(Baseline = true)]
    public Player DataVo() => _players.FirstOrDefault(NextKey(), static (p, id) => p.Id == id)!;

    [Benchmark]
    public Player Sqlite()
    {
        _sqliteId.Value = NextKey();
        using SqliteDataReader reader = _sqliteGet.ExecuteReader();
        reader.Read();
        return Engines.Read(reader);
    }

    [Benchmark]
    public Player DuckDb()
    {
        _duckId.Value = NextKey();
        using var reader = _duckGet.ExecuteReader();
        reader.Read();
        return Engines.Read(reader);
    }

    private int NextKey()
    {
        int index = _next;
        _next = index + 1 == _keys.Length ? 0 : index + 1;
        return _keys[index];
    }
}
