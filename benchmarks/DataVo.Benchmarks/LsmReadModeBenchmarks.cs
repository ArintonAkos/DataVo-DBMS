using System.Text;
using BenchmarkDotNet.Attributes;
using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Benchmarks;

/// <summary>How an LSM SSTable is read in <see cref="LsmReadModeBenchmarks"/>.</summary>
public enum SsTableReadPath
{
    /// <summary><see cref="LsmReadMode.Auto"/>: a read-only memory mapping on 64-bit hosts.</summary>
    AutoMmap,

    /// <summary><see cref="LsmReadMode.PositionalRead"/> with no cache: one pread + CRC check per block read.</summary>
    Positional,

    /// <summary><see cref="LsmReadMode.PositionalRead"/> through the sharded <see cref="LsmBlockCache"/>.</summary>
    PositionalCached,
}

/// <summary>
/// The SSTable read path in its three configurations: random point lookups and a full sequential scan of one
/// 200,000-row file, read straight through <see cref="SsTableV2Reader"/>, plus engine-level <c>ReadRow</c>
/// against a reopened (fully flushed) LSM table.
/// </summary>
[MemoryDiagnoser]
public class LsmReadModeBenchmarks
{
    private const int RowCount = 200_000;

    private ScratchDirectory _scratch = null!;
    private SsTableV2Reader _reader = null!;
    private LsmStorageEngine _engine = null!;
    private long[] _keys = [];
    private int _next;

    [ParamsAllValues]
    public SsTableReadPath Path { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _scratch = new ScratchDirectory();
        string file = System.IO.Path.Combine(_scratch.Path, "rows.sst");
        using (var stream = new FileStream(file, FileMode.CreateNew, FileAccess.Write))
        using (var writer = new SsTableV2Writer(stream))
        {
            for (long rowId = 1; rowId <= RowCount; rowId++)
            {
                writer.Add(rowId, 1, LsmValueType.Put, Payload(rowId));
            }

            writer.Finish();
        }

        LsmReadMode mode = Path == SsTableReadPath.AutoMmap ? LsmReadMode.Auto : LsmReadMode.PositionalRead;
        long cacheBytes = Path == SsTableReadPath.PositionalCached ? 64L * 1024 * 1024 : 0;
        _reader = SsTableV2Reader.Open(file, mode, cacheBytes > 0 ? new LsmBlockCache(cacheBytes) : null);

        string engineDirectory = System.IO.Path.Combine(_scratch.Path, "engine");
        using (var loader = new LsmStorageEngine(engineDirectory, LsmWalDurabilityMode.RelaxedOsBuffer))
        {
            loader.InsertRows("db", "t", [.. Enumerable.Range(1, RowCount).Select(i => Payload(i))]);
        } // shutdown flushes every row into SSTables

        _engine = new LsmStorageEngine(engineDirectory, LsmWalDurabilityMode.RelaxedOsBuffer, mode, cacheBytes);

        var random = new Random(42);
        _keys = [.. Enumerable.Range(1, RowCount).Select(i => (long)i)];
        random.Shuffle(_keys);

        // Warm: every block once, so cached runs measure warm hits and mapped runs warm page-cache pages.
        Scan();
        for (int i = 0; i < _keys.Length; i++)
        {
            if (Lookup() <= 0)
            {
                throw new InvalidOperationException("A row was not found.");
            }
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _engine.Dispose();
        _reader.Dispose();
        _scratch.Dispose();
    }

    /// <summary>One random point lookup, value length returned (no copy).</summary>
    [Benchmark]
    public int Lookup()
    {
        long key = _keys[_next];
        _next = _next + 1 == _keys.Length ? 0 : _next + 1;
        return _reader.Lookup(key, ulong.MaxValue, 0, static (_, _, value, _) => value.Length, -1);
    }

    /// <summary>A full sequential scan of the file through a cursor.</summary>
    [Benchmark]
    public long Scan()
    {
        long total = 0;
        SsTableV2Cursor cursor = _reader.OpenCursor();
        while (cursor.MoveNext())
        {
            total += cursor.Value.Length;
        }

        return total;
    }

    /// <summary>Engine-level point read of a flushed row (includes the row copy every ReadRow makes).</summary>
    [Benchmark]
    public int EngineReadRow()
    {
        long key = _keys[_next];
        _next = _next + 1 == _keys.Length ? 0 : _next + 1;
        return _engine.ReadRow("db", "t", key).Length;
    }

    private static byte[] Payload(long rowId) => Encoding.UTF8.GetBytes($"player-{rowId:D8}|level={rowId % 90}|gold={rowId * 37 % 100_000}");
}
