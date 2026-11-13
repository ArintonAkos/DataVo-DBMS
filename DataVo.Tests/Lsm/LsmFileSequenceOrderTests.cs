using System.Text;
using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Two SSTables of a table that share part of their key range never hold interleaved sequence numbers: all
/// versions in one are older than all versions in the other. The out-of-core columnar scan relies on this to
/// decide, from file properties alone, which file's version of a row is the newest.
/// </summary>
public sealed class LsmFileSequenceOrderTests : IDisposable
{
    private const string Db = "db";
    private const string Table = "items";
    private readonly string _root = TestStorageDirectories.Create("lsm_file_sequence_order");

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

    [Fact]
    public void FilesWithOverlappingKeys_HoldOrderedSequenceRanges_UnderConcurrentCommitsFlushesAndCompaction()
    {
        using var engine = new LsmStorageEngine(_root, LsmWalDurabilityMode.RelaxedOsBuffer)
        {
            MemTableFlushThresholdOverrideBytes = 48 * 1024, // rotate often, so background flushes race commits
        };
        engine.InsertRow(Db, Table, Payload(0, 0));

        int compared = 0;
        for (int round = 0; round < 6; round++)
        {
            using var stop = new CancellationTokenSource();
            Task[] writers = [.. Enumerable.Range(0, 4).Select(worker => Task.Run(() => Write(engine, (round * 31) + worker, stop.Token)))];
            for (int flush = 0; flush < 3; flush++)
            {
                Thread.Sleep(5);
                engine.FlushTableForTesting(Db, Table);
            }

            stop.Cancel();
            Task.WaitAll(writers);
            engine.FlushTableForTesting(Db, Table);

            // A read drops the engine's hold on files that compaction replaced, so only live files remain on disk.
            _ = engine.ReadAllRows(Db, Table).Count();
            compared += AssertOrdered();
        }

        Assert.True(compared > 0, "No two files ever shared a key range, so the test compared nothing.");
    }

    private static void Write(LsmStorageEngine engine, int seed, CancellationToken stop)
    {
        var random = new Random(seed);
        var mine = new List<RowKey>();
        for (int step = 0; !stop.IsCancellationRequested && step < 1_500; step++)
        {
            int action = random.Next(10);
            if (mine.Count == 0 || action < 5)
            {
                mine.Add(engine.InsertRow(Db, Table, Payload(seed, step)));
            }
            else if (action < 8)
            {
                engine.OverwriteRowForTesting(Db, Table, mine[random.Next(mine.Count)], Payload(seed, step));
            }
            else
            {
                int at = random.Next(mine.Count);
                engine.DeleteRow(Db, Table, mine[at]);
                mine.RemoveAt(at);
            }
        }
    }

    // Returns how many pairs of files shared part of their key range.
    private int AssertOrdered()
    {
        var files = new List<(string Name, SsTableV2Properties Properties)>();
        foreach (string path in Directory.GetFiles(_root, "*.sst", SearchOption.AllDirectories))
        {
            if (SsTableV2Format.IsV2File(path))
            {
                using SsTableV2Reader reader = SsTableV2Reader.Open(path, LsmReadMode.PositionalRead);
                files.Add((Path.GetFileName(path), reader.Properties));
            }
        }

        int overlapping = 0;
        for (int i = 0; i < files.Count; i++)
        {
            for (int j = i + 1; j < files.Count; j++)
            {
                SsTableV2Properties a = files[i].Properties;
                SsTableV2Properties b = files[j].Properties;
                if (a.SmallestKey > b.LargestKey || b.SmallestKey > a.LargestKey)
                {
                    continue;
                }

                overlapping++;
                Assert.True(
                    a.LargestSequence < b.SmallestSequence || b.LargestSequence < a.SmallestSequence,
                    $"{files[i].Name} holds sequences {a.SmallestSequence}..{a.LargestSequence} and {files[j].Name} holds {b.SmallestSequence}..{b.LargestSequence}.");
            }
        }

        return overlapping;
    }

    private static byte[] Payload(int seed, int step) => Encoding.UTF8.GetBytes($"row-{seed}-{step}-payload-with-some-length");
}
