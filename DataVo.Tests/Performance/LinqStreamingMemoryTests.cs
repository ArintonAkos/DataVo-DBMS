using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Performance;

[DataVoTable("readings")]
public sealed class Reading
{
    [PrimaryKey] public int Id { get; set; }

    public int Sensor { get; set; }

    public string Note { get; set; } = "";
}

/// <summary>Enumerating a table query keeps a bounded amount of memory alive, however many rows it returns.</summary>
public sealed class LinqStreamingMemoryTests : IDisposable
{
    private const int Rows = 25_000;

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_linq_stream_{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [InlineData(StorageMode.Lsm)]
    [InlineData(StorageMode.Disk)]
    public void CompiledWhere_RetainedHeapStaysFlatWhileEnumerating(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        var query = db.Table<Reading>().Where(0, static (r, min) => r.Sensor >= min);
        Assert.True(query.IsCompiled);

        AssertFlat(query);
    }

    [Fact]
    public void ScanFallback_RetainedHeapStaysFlatWhileEnumerating()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        Func<Reading, bool> predicate = static r => r.Sensor >= 0;
        var query = db.Table<Reading>().Where(predicate);
        Assert.False(query.IsCompiled);

        AssertFlat(query);
    }

    [Fact]
    public void Enumeration_StopsReadingWhenTheCallerStops()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        var query = db.Table<Reading>().Where(0, static (r, min) => r.Sensor >= min);
        foreach (Reading _ in query)
        {
            break; // prepare the plan and warm the path
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        int seen = 0;
        foreach (Reading _ in query)
        {
            if (++seen == 10)
            {
                break;
            }
        }

        long firstTen = GC.GetAllocatedBytesForCurrentThread() - before;
        before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(Rows, query.Count());
        long all = GC.GetAllocatedBytesForCurrentThread() - before;

        // Ten rows cost one page, not the table.
        Assert.True(firstTen * 5 < all, $"first ten rows allocated {firstTen} B, the whole scan {all} B");
    }

    private static void AssertFlat<TState>(DataVoQuery<Reading, TState> query)
    {
        Assert.Equal(Rows, query.Count()); // warm up, and prove every row is reached

        long baseline = LiveBytes();
        long peak = 0;
        int seen = 0;
        long checksum = 0;
        var earlier = new List<WeakReference>();
        foreach (Reading reading in query)
        {
            checksum += reading.Id;
            if (seen % 1_000 == 0)
            {
                earlier.Add(new WeakReference(reading));
            }

            if (++seen % 5_000 == 0)
            {
                peak = Math.Max(peak, LiveBytes() - baseline);
            }
        }

        Assert.Equal(Rows, seen);
        Assert.Equal((long)Rows * (Rows + 1) / 2, checksum);

        // Holding every row would keep several megabytes alive; one 1,024-row page is a few hundred kilobytes.
        Assert.True(peak < 1_000_000, $"live heap grew by {peak} bytes during enumeration");

        // Rows the loop has moved past are garbage: nothing in the query keeps them.
        GC.Collect();
        Assert.Equal(0, earlier.Count(static reference => reference.IsAlive));

        long before = LiveBytes();
        List<Reading> held = query.ToList();
        long materialized = LiveBytes() - before;
        Assert.Equal(Rows, held.Count);
        Assert.True(materialized > 2_000_000, $"a list of every row held only {materialized} bytes, so the comparison proves nothing");
        GC.KeepAlive(held);
    }

    /// <summary>Bytes in live objects after a full collection, leaving out dead space the collector has not compacted.</summary>
    private static long LiveBytes()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GCMemoryInfo info = GC.GetGCMemoryInfo(GCKind.FullBlocking);
        return info.HeapSizeBytes - info.FragmentedBytes;
    }

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = _directory });
        db.Execute("CREATE DATABASE S");
        db.Execute("USE S");
        db.EnsureReadingTable();
        for (int start = 1; start <= Rows; start += 500)
        {
            IEnumerable<string> values = Enumerable.Range(start, 500).Select(static id => $"({id}, {id % 16}, 'reading number {id}')");
            QueryResult result = db.Execute($"INSERT INTO readings (Id, Sensor, Note) VALUES {string.Join(", ", values)}")[^1];
            Assert.False(result.IsError, string.Join(" | ", result.Messages));
        }

        return db;
    }
}
