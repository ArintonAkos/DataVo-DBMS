using System.Runtime.CompilerServices;
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

        for (int round = 0; round < 6; round++)
        {
            using var stop = new CancellationTokenSource();
            var written = new StrongBox<int>();
            Task[] writers = [.. Enumerable.Range(0, 4).Select(worker => Task.Run(() => Write(engine, (round * 31) + worker, written, stop.Token)))];

            // Flush at write-count milestones rather than after fixed sleeps, so a slow runner (where the writers
            // barely start within a few milliseconds) still races flushes against a known amount of committing.
            for (int flush = 1; flush <= 3; flush++)
            {
                int target = flush * 1_000;
                SpinWait.SpinUntil(() => Volatile.Read(ref written.Value) >= target || writers.All(static writer => writer.IsCompleted), TimeSpan.FromSeconds(30));
                engine.FlushTableForTesting(Db, Table);
            }

            stop.Cancel();
            Task.WaitAll(writers);
            engine.FlushTableForTesting(Db, Table);
            engine.WaitForBackgroundWorkForTesting(Db, Table);

            EnsureOverlappingLevel0File(engine);
            int overlapping = AssertOrdered();
            Assert.True(overlapping > 0, $"Round {round}: no two live files shared a key range, so the round compared nothing.");
        }
    }

    /// <summary>
    /// Leaves a Level-0 file whose key range overlaps another live file, whatever the flush and compaction timing:
    /// it rewrites the smallest live row and flushes that version alone. Its older version stays in the file that
    /// held it (only compaction rewrites files), so the two files share that key. A flush that tips Level 0 over the
    /// compaction threshold merges the new file away; then Level 0 is empty and the next flush cannot tip it again.
    /// </summary>
    private void EnsureOverlappingLevel0File(LsmStorageEngine engine)
    {
        do
        {
            RowKey smallest = engine.ReadAllRows(Db, Table).First().RowId;
            Assert.True(engine.OverwriteRowForTesting(Db, Table, smallest, Payload(-1, 0)));
            engine.FlushTableForTesting(Db, Table);
            engine.WaitForBackgroundWorkForTesting(Db, Table);
        }
        while (!LiveFiles().Any(static file => file.Level == 0));
    }

    /// <summary>The table's live SSTables as its manifest lists them; files compaction replaced are excluded.</summary>
    private LsmTableFileMetadata[] LiveFiles()
    {
        var manifest = new LsmManifest(Path.Combine(TableDirectory, "MANIFEST"));
        return [.. Enumerable.Range(0, LsmTable.DefaultMaxCompactionLevel + 1).SelectMany(manifest.GetLiveFiles)];
    }

    private string TableDirectory =>
        Directory.GetFiles(_root, "MANIFEST", SearchOption.AllDirectories).Select(Path.GetDirectoryName).Single(static dir => dir!.EndsWith(Table, StringComparison.Ordinal))!;

    private static void Write(LsmStorageEngine engine, int seed, StrongBox<int> written, CancellationToken stop)
    {
        var random = new Random(seed);
        var mine = new List<RowKey>();
        for (int step = 0; !stop.IsCancellationRequested && step < 1_500; step++, Interlocked.Increment(ref written.Value))
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

    // Returns how many pairs of live files shared part of their key range.
    private int AssertOrdered()
    {
        var files = new List<(string Name, SsTableV2Properties Properties)>();
        foreach (LsmTableFileMetadata file in LiveFiles())
        {
            using SsTableV2Reader reader = SsTableV2Reader.Open(Path.Combine(TableDirectory, file.FileName), LsmReadMode.PositionalRead);
            files.Add((file.FileName, reader.Properties));
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
