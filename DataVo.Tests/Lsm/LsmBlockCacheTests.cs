using System.Runtime.CompilerServices;
using System.Text;
using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Tests.Performance;
using Xunit.Abstractions;

namespace DataVo.Tests.Lsm;

/// <summary>
/// The sharded block cache behind <see cref="LsmReadMode.PositionalRead"/>: correct data on hits, no managed
/// allocation on warm hits, a CRC check on every physical read (also after eviction), clean errors on
/// truncation, and safe eviction under concurrent readers.
/// </summary>
[Collection(AllocationSensitiveCollection.Name)]
public sealed class LsmBlockCacheTests(ITestOutputHelper output) : IDisposable
{
    private readonly ITestOutputHelper _output = output;
    private readonly string _directory = TestStorageDirectories.Create("lsm_block_cache");

    public void Dispose()
    {
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public void RepeatedLookups_AreServedFromTheCache()
    {
        string path = WriteRows(2_000);
        var cache = new LsmBlockCache(8 * 1024 * 1024);
        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead, cache);

        Assert.Equal("row-1", Get(reader, 1));
        long missesAfterFirst = cache.Misses;
        Assert.Equal(1, missesAfterFirst);

        for (int i = 0; i < 10; i++)
        {
            Assert.Equal("row-1", Get(reader, 1));
        }

        Assert.Equal(missesAfterFirst, cache.Misses);
        Assert.Equal(10, cache.Hits);
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void WarmHits_AllocateNothing()
    {
        string path = WriteRows(4_000);
        var cache = new LsmBlockCache(8 * 1024 * 1024);
        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead, cache);

        // Load every block into the cache, then run the measured pass itself twice so it and everything it calls
        // are compiled before the window.
        for (long rowId = 1; rowId <= 4_000; rowId++)
        {
            Assert.Equal(Value(rowId).Length, Length(reader, rowId));
        }

        for (int pass = 0; pass < 2; pass++)
        {
            Assert.True(LookupAll(reader, passes: 5) > 0);
        }

        var window = new AllocationWindow();
        long missesBefore = cache.Misses;
        window.Start();
        long total = LookupAll(reader, passes: 5);
        long allocated = window.Stop();
        _output.WriteLine($"{window}; {cache.Misses - missesBefore} cache miss(es)");

        Assert.True(total > 0);
        Assert.Equal(0, allocated);
    }

    // The measured pass, shared by the warm-up and the measurement; fully optimized from its first call, so no
    // tier-up or on-stack replacement of this loop can land inside the window.
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static long LookupAll(SsTableV2Reader reader, int passes)
    {
        long total = 0;
        for (int pass = 0; pass < passes; pass++)
        {
            for (long rowId = 1; rowId <= 4_000; rowId++)
            {
                total += Length(reader, rowId);
            }
        }

        return total;
    }

    [Fact]
    public void EveryPhysicalRead_IsVerified_EvenAfterTheBlockWasCachedAndEvicted()
    {
        if (OperatingSystem.IsWindows())
        {
            // Windows forbids writing a file another handle opened without write sharing; the read path
            // under test is identical there, only the in-place corruption cannot be staged.
            return;
        }

        string path = WriteRows(2_000);
        var cache = new LsmBlockCache(8 * 1024 * 1024);
        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead, cache);
        Assert.Equal("row-1", Get(reader, 1)); // verified, then cached

        SsTableV2BlockInfo info = reader.GetBlockInfo(0);
        Corrupt(path, info.Offset + info.Length - 3);

        Assert.Equal("row-1", Get(reader, 1)); // the cached bytes are the verified ones
        cache.Clear(); // eviction: the next read goes back to disk

        Assert.Throws<InvalidDataException>(() => Get(reader, 1));
    }

    [Fact]
    public void UncachedPositionalReads_VerifyEveryRead()
    {
        if (OperatingSystem.IsWindows())
        {
            return; // see EveryPhysicalRead_IsVerified_EvenAfterTheBlockWasCachedAndEvicted
        }

        string path = WriteRows(2_000);
        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
        Assert.Equal("row-1", Get(reader, 1));

        SsTableV2BlockInfo info = reader.GetBlockInfo(0);
        Corrupt(path, info.Offset + info.Length - 3);

        Assert.Throws<InvalidDataException>(() => Get(reader, 1));
    }

    [Fact]
    public void FileTruncatedUnderAnOpenReader_FailsWithInvalidData()
    {
        if (OperatingSystem.IsWindows())
        {
            return; // see EveryPhysicalRead_IsVerified_EvenAfterTheBlockWasCachedAndEvicted
        }

        string path = WriteRows(4_000);
        var cache = new LsmBlockCache(8 * 1024 * 1024);
        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead, cache);
        Assert.True(reader.BlockCount > 2);
        SsTableV2BlockInfo last = reader.GetBlockInfo(reader.BlockCount - 1);

        using (var stream = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            stream.SetLength(last.Offset + 8);
        }

        Assert.Throws<InvalidDataException>(() => Get(reader, 4_000));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void TinyCache_EvictsWithinBudget_AndStaysCorrect()
    {
        string path = WriteRows(20_000);
        var cache = new LsmBlockCache(64 * 1024, shardCount: 4);
        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead, cache);

        for (long rowId = 1; rowId <= 20_000; rowId += 7)
        {
            Assert.Equal($"row-{rowId}", Get(reader, rowId));
        }

        Assert.True(cache.Evictions > 0);
        Assert.True(cache.SizeBytes <= cache.CapacityBytes + (4 * 64 * 1024), $"{cache.SizeBytes} B cached");
    }

    [Fact]
    public void ConcurrentReaders_WithConstantEviction_ReadCorrectBytes()
    {
        string path = WriteRows(20_000);
        var cache = new LsmBlockCache(48 * 1024, shardCount: 4);
        using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead, cache);

        Parallel.For(0, 8, new ParallelOptions { MaxDegreeOfParallelism = 8 }, worker =>
        {
            var random = new Random(worker);
            for (int i = 0; i < 5_000; i++)
            {
                long rowId = random.Next(1, 20_001);
                Assert.Equal($"row-{rowId}", Get(reader, rowId));
            }

            // Cursors hold a cached block across MoveNext and give it back when they pass it.
            SsTableV2Cursor cursor = reader.OpenCursor();
            long expected = 1;
            while (cursor.MoveNext())
            {
                Assert.Equal((long)cursor.Key, expected);
                Assert.Equal($"row-{expected}", Encoding.UTF8.GetString(cursor.Value));
                expected++;
            }

            Assert.Equal(20_001, expected);
        });

        Assert.True(cache.Evictions > 0);
    }

    [Fact]
    public void ClosingAReader_DropsItsBlocks()
    {
        string path = WriteRows(4_000);
        var cache = new LsmBlockCache(8 * 1024 * 1024);
        using (SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead, cache))
        {
            SsTableV2Cursor cursor = reader.OpenCursor();
            while (cursor.MoveNext())
            {
            }

            Assert.True(cache.Count > 1);
        }

        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.SizeBytes);
    }

    [Fact]
    public void Engine_UsesTheCache_ForPositionalReads_AndNotForMappedReads()
    {
        string positional = Path.Combine(_directory, "positional");
        using (var engine = new LsmStorageEngine(positional, LsmWalDurabilityMode.RelaxedOsBuffer, LsmReadMode.PositionalRead, blockCacheSizeBytes: 4 * 1024 * 1024))
        {
            List<RowKey> ids = engine.InsertRows("db", "t", [.. Enumerable.Range(1, 3_000).Select(i => Encoding.UTF8.GetBytes($"payload-{i}"))]);
            engine.FlushTableForTesting("db", "t");
            Assert.NotNull(engine.BlockCache);

            for (int pass = 0; pass < 3; pass++)
            {
                foreach (RowKey id in ids.Where(id => (long)id % 50 == 0))
                {
                    Assert.Equal($"payload-{(long)id}", Encoding.UTF8.GetString(engine.ReadRow("db", "t", id)));
                }
            }

            Assert.True(engine.BlockCache!.Hits > 0);
        }

        using var mapped = new LsmStorageEngine(Path.Combine(_directory, "mapped"), LsmWalDurabilityMode.RelaxedOsBuffer, LsmReadMode.MemoryMapped, blockCacheSizeBytes: 4 * 1024 * 1024);
        Assert.Null(mapped.BlockCache);
    }

    private string WriteRows(int count)
    {
        string path = Path.Combine(_directory, $"{Guid.NewGuid():N}.sst");
        using (var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write))
        using (var writer = new SsTableV2Writer(stream))
        {
            for (long rowId = 1; rowId <= count; rowId++)
            {
                writer.Add(rowId, 1, LsmValueType.Put, Value(rowId));
            }

            writer.Finish();
        }

        return path;
    }

    private static void Corrupt(string path, long offset)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        stream.Position = offset;
        int value = stream.ReadByte();
        stream.Position = offset;
        stream.WriteByte((byte)(value ^ 0x5A));
        stream.Flush(flushToDisk: true);
    }

    private static byte[] Value(long rowId) => Encoding.UTF8.GetBytes($"row-{rowId}");

    private static string? Get(SsTableV2Reader reader, RowKey rowId) =>
        reader.Lookup(
            rowId,
            ulong.MaxValue,
            0,
            static (type, _, value, _) => type == LsmValueType.Deletion ? "<deleted>" : Encoding.UTF8.GetString(value),
            (string?)null);

    private static int Length(SsTableV2Reader reader, RowKey rowId) =>
        reader.Lookup(rowId, ulong.MaxValue, 0, static (_, _, value, _) => value.Length, -1);
}
