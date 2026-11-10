using System.Diagnostics;
using System.Text;
using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

// Measures DataVoTable<T> queries on an LSM database against the calls they compile down to:
//   (a) primary-key point lookups: latency and bytes allocated per call
//   (b) enumerating every matching row: time, bytes allocated, and how much the live heap grows meanwhile
//
// Usage: dotnet run -c Release --project tools/DataVo.LinqBenchmark -- [rows=100000]

int rows = args.Length > 0 ? int.Parse(args[0]) : 100_000;
string directory = Path.Combine(Path.GetTempPath(), $"datavo_linq_bench_{Guid.NewGuid():N}");
long sink = 0;

#if DEBUG
Console.WriteLine("WARNING: this is a Debug build; the numbers mean nothing. Run with -c Release.");
#endif

try
{
    using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory });
    db.Execute("CREATE DATABASE B");
    db.Execute("USE B");
    db.EnsureReadingTable();

    var seeding = Stopwatch.StartNew();
    for (int start = 1; start <= rows; start += 500)
    {
        var sql = new StringBuilder("INSERT INTO readings (Id, Sensor, Note) VALUES ");
        for (int id = start; id < start + 500 && id <= rows; id++)
        {
            sql.Append(id == start ? string.Empty : ", ").Append($"({id}, {id % 16}, 'reading number {id}')");
        }

        QueryResult inserted = db.Execute(sql.ToString())[^1];
        if (inserted.IsError)
        {
            throw new InvalidOperationException(string.Join(" | ", inserted.Messages));
        }
    }

    Console.WriteLine($"seeded {rows:N0} rows in {seeding.ElapsedMilliseconds} ms");

    DataVoTable<Reading> readings = db.Table<Reading>();
    DataVoPreparedStatement byId = db.Prepare("SELECT Id, Sensor, Note FROM readings WHERE Id = @p0");
    DataVoPreparedStatement bySensor = db.Prepare("SELECT Id, Sensor, Note FROM readings WHERE Sensor >= @p0");

    const int Lookups = 2_000_000;
    Console.WriteLine($"\n(a) point lookup by primary key, {rows:N0} rows, best of 5 x {Lookups:N0}");
    Point("prepared ExecuteBorrowed, read 1 column (baseline)", id => (int)byId.ExecuteBorrowed(("p0", id)).Data[0]["Sensor"]!);
    Point("generated GetReadingById", id => db.GetReadingById(id)!.Sensor);
    Point("Table<T>.FirstOrDefault(id, static (r, k) => r.Id == k)", id => readings.FirstOrDefault(id, static (r, k) => r.Id == k)!.Sensor);
    Point("Table<T>.Where(id, static ...).FirstOrDefault()", id => readings.Where(id, static (r, k) => r.Id == k).FirstOrDefault()!.Sensor);
    Point("Table<T>.FirstOrDefault(id, ...) miss", id => readings.FirstOrDefault(-id, static (r, k) => r.Id == k) is null ? 1 : 0);
    Point("prepared ExecuteBorrowed miss (baseline)", id => byId.ExecuteBorrowed(("p0", -id)).Data.Count);

    Console.WriteLine("\n(b) enumerate every row matching Sensor >= 0; live heap sampled every 5,000 rows");
    Range("Table<T>.Where(min, static (r, m) => r.Sensor >= m) foreach", sample =>
    {
        int seen = 0;
        foreach (Reading reading in readings.Where(0, static (r, m) => r.Sensor >= m))
        {
            sink += reading.Id;
            if (++seen % 5_000 == 0)
            {
                sample();
            }
        }

        return seen;
    });
    Range("Table<T>.Where(...).ToList() then foreach", sample =>
    {
        int seen = 0;
        foreach (Reading reading in readings.Where(0, static (r, m) => r.Sensor >= m).ToList())
        {
            sink += reading.Id;
            if (++seen % 5_000 == 0)
            {
                sample();
            }
        }

        return seen;
    });
    Range("StreamQuery(sql, param) foreach (baseline, no objects)", sample =>
    {
        int seen = 0;
        foreach (DataVoRow row in db.StreamQuery("SELECT Id, Sensor, Note FROM readings WHERE Sensor >= @p0", ("p0", 0)))
        {
            sink += (int)row["Id"]!;
            if (++seen % 5_000 == 0)
            {
                sample();
            }
        }

        return seen;
    });
    Range("prepared Execute(param) foreach (baseline, materialized)", sample =>
    {
        int seen = 0;
        foreach (Dictionary<string, object?> row in bySensor.Execute(("p0", 0)).Data)
        {
            sink += (int)row["Id"]!;
            if (++seen % 5_000 == 0)
            {
                sample();
            }
        }

        return seen;
    });

    Console.WriteLine($"\nchecksum {sink}");
}
finally
{
    if (Directory.Exists(directory))
    {
        Directory.Delete(directory, recursive: true);
    }
}

void Point(string name, Func<int, long> lookup)
{
    for (int i = 0; i < 200_000; i++)
    {
        sink += lookup(1 + (int)((i * 7919L) % rows));
    }

    const int Lookups = 2_000_000;
    double best = double.MaxValue;
    long bytes = 0;
    for (int round = 0; round < 5; round++)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        var timer = Stopwatch.StartNew();
        for (int i = 0; i < Lookups; i++)
        {
            sink += lookup(1 + (int)((i * 7919L) % rows));
        }

        timer.Stop();
        bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        best = Math.Min(best, timer.Elapsed.TotalNanoseconds / Lookups);
    }

    Console.WriteLine($"{name,-62} {best / 1000,7:F3} us/op {bytes / (double)Lookups,7:F1} B/op");
}

void Range(string name, Func<Action, int> enumerate)
{
    enumerate(static () => { }); // warm up: prepare the statement and compile the path

    long baseline = LiveBytes();
    long peak = 0;
    long before = GC.GetAllocatedBytesForCurrentThread();
    int seen = enumerate(() => peak = Math.Max(peak, LiveBytes() - baseline));
    long bytes = GC.GetAllocatedBytesForCurrentThread() - before;

    // Timed separately, because sampling the heap forces collections.
    var timer = Stopwatch.StartNew();
    enumerate(static () => { });
    timer.Stop();

    Console.WriteLine($"{name,-62} rows={seen,7:N0} {timer.Elapsed.TotalMilliseconds,7:F1} ms  alloc={bytes / 1e6,6:F1} MB  live heap growth={peak / 1e6,6:F2} MB");
}

// Bytes in live objects. GC.GetTotalMemory also counts dead space the collector has not compacted yet, which
// rises and falls on its own and looks like a leak.
static long LiveBytes()
{
    GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
    GCMemoryInfo info = GC.GetGCMemoryInfo(GCKind.FullBlocking);
    return info.HeapSizeBytes - info.FragmentedBytes;
}

[DataVoTable("readings")]
public sealed class Reading
{
    [PrimaryKey] public int Id { get; set; }

    public int Sensor { get; set; }

    public string Note { get; set; } = "";
}
