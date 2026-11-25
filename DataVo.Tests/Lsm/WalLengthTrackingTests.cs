using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

/// <summary>
/// The commit log appends at a length it tracks in memory instead of asking the file system on every commit.
/// These tests check that the tracked length always equals the file's real length: across appends, across
/// prunes that rewrite or delete the file, and across crashes that leave a torn tail for recovery to cut.
/// </summary>
public sealed class WalLengthTrackingTests : IDisposable
{
    private readonly string _directory = TestStorageDirectories.Create("wal_length_tracking");

    private string LogPath => Path.Combine(_directory, "commit.wal");

    [Theory]
    [InlineData(LsmWalDurabilityMode.StrictFsync)]
    [InlineData(LsmWalDurabilityMode.RelaxedOsBuffer)]
    public void Appends_WriteAtTheTrackedLength_ReadingTheFileLengthOncePerOpen(LsmWalDurabilityMode mode)
    {
        using var log = new LsmCommitLog(LogPath, mode);
        int lengthReads = log.WalLengthReadCount;

        for (int i = 1; i <= 200; i++)
        {
            Append(log, "t", i, payloadBytes: 1 + (i * 37 % 300));
            AssertTrackedLengthIsTheFileLength(log);
        }

        Assert.Equal(lengthReads + 1, log.WalLengthReadCount);
        Assert.Equal(Enumerable.Range(1, 200).Select(i => (long)i), log.ReadAll().Select(f => f.Lsn));
    }

    [Fact]
    public void Prunes_RewriteTheFile_AndTheNextAppendStartsAtItsNewLength()
    {
        var expected = new List<long>();
        using (var log = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync))
        {
            for (int round = 0; round < 6; round++)
            {
                for (int i = 0; i < 20; i++)
                {
                    expected.Add(Append(log, $"r{round}", i, payloadBytes: 50 + i));
                    AssertTrackedLengthIsTheFileLength(log);
                }

                long keepFrom = expected[^5];
                log.PruneThrough(keepFrom - 1);
                expected.RemoveAll(lsn => lsn < keepFrom);
                Assert.Equal(-1, log.ReadWalLengths().Tracked);
                Assert.Equal(expected, log.ReadAll().Select(f => f.Lsn));

                int lengthReads = log.WalLengthReadCount;
                expected.Add(Append(log, $"r{round}", 99, payloadBytes: 7));
                AssertTrackedLengthIsTheFileLength(log);
                Assert.Equal(lengthReads + 1, log.WalLengthReadCount);
            }

            // Pruning every frame deletes the file; the next append creates it again at offset 0.
            log.PruneThrough(expected[^1]);
            expected.Clear();
            Assert.Equal((-1L, -1L), log.ReadWalLengths());
            expected.Add(Append(log, "fresh", 1, payloadBytes: 10));
            (long tracked, long onDisk) = log.ReadWalLengths();
            Assert.Equal(onDisk, tracked);
            Assert.True(onDisk > 0);
        }

        using var reopened = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync);
        Assert.Equal(expected, reopened.ReadAll().Select(f => f.Lsn));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(31)]
    [InlineData(200)]
    public void CrashWithATornTail_RecoveryCutsIt_AndAppendsResumeAtTheValidLength(int tornBytes)
    {
        // The first owner crashes: it is never disposed, so its pooled handle and tracked length stay behind.
        var crashed = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync);
        for (int i = 1; i <= 10; i++)
        {
            Append(crashed, "before", i, payloadBytes: 64);
        }

        long validLength = new FileInfo(LogPath).Length;
        using (var stream = new FileStream(LogPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
        {
            stream.Write(Enumerable.Repeat((byte)0xAB, tornBytes).ToArray()); // a frame cut off mid-write
        }

        using (var recovered = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync))
        {
            Assert.Equal(validLength, new FileInfo(LogPath).Length);
            Assert.Equal(Enumerable.Range(1, 10).Select(i => (long)i), recovered.ReadAll().Select(f => f.Lsn));

            for (int i = 11; i <= 15; i++)
            {
                Assert.Equal(i, Append(recovered, "after", i, payloadBytes: 64));
                AssertTrackedLengthIsTheFileLength(recovered);
            }
        }

        using var reopened = new LsmCommitLog(LogPath, LsmWalDurabilityMode.StrictFsync);
        Assert.Equal(Enumerable.Range(1, 15).Select(i => (long)i), reopened.ReadAll().Select(f => f.Lsn));
    }

    [Fact]
    public void Engine_FlushesPruneTheLog_AndEveryRowSurvivesRepeatedCrashes()
    {
        // Each life writes a new table: tables replayed while the engine opens are created before the
        // small MemTable threshold is set, so only a new table rotates (and prunes) every few batches.
        var expected = new Dictionary<(string Table, RowKey Id), byte[]>();
        for (int life = 0; life < 3; life++)
        {
            var engine = new LsmStorageEngine(_directory, LsmWalDurabilityMode.RelaxedOsBuffer) { MemTableFlushThresholdOverrideBytes = 4_096 };
            AssertRows(engine, expected);
            string table = $"t{life}";

            bool sawPrune = false;
            long previousOnDisk = 0;
            for (int batch = 0; batch < 30; batch++)
            {
                List<byte[]> rows = [.. Enumerable.Range(0, 8).Select(i => Enumerable.Repeat((byte)(life + batch + i), 200).ToArray())];
                List<RowKey> ids = engine.InsertRows("db", table, rows);
                for (int i = 0; i < ids.Count; i++)
                {
                    expected[(table, ids[i])] = rows[i];
                }

                (long tracked, long onDisk) = engine.CommitLogForTesting.ReadWalLengths();
                if (tracked >= 0)
                {
                    Assert.Equal(onDisk, tracked);
                }

                sawPrune |= tracked < 0 || onDisk < previousOnDisk;
                previousOnDisk = onDisk;
            }

            Assert.True(sawPrune, "no flush pruned the commit log, so no rotation was exercised");
            engine.SimulateCrashForTesting();
        }

        using var final = new LsmStorageEngine(_directory, LsmWalDurabilityMode.RelaxedOsBuffer);
        AssertRows(final, expected);
        foreach (IGrouping<string, (string Table, RowKey Id)> table in expected.Keys.GroupBy(key => key.Table))
        {
            Assert.Equal(table.Count(), final.EstimateRowCount("db", table.Key));
        }
    }

    private static void AssertRows(LsmStorageEngine engine, Dictionary<(string Table, RowKey Id), byte[]> expected)
    {
        foreach (((string table, RowKey id), byte[] row) in expected)
        {
            Assert.Equal(row, engine.ReadRow("db", table, id));
        }
    }

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

    private static void AssertTrackedLengthIsTheFileLength(LsmCommitLog log)
    {
        (long tracked, long onDisk) = log.ReadWalLengths();
        Assert.True(onDisk > 0, "the commit log file is missing");
        Assert.Equal(onDisk, tracked);
    }

    private static long Append(LsmCommitLog log, string table, long rowId, int payloadBytes)
    {
        var batch = new LsmCommitBatch();
        batch.Put("Db", table, rowId, Enumerable.Repeat((byte)rowId, payloadBytes).ToArray());
        LsmCommitTicket ticket = log.Append(batch);
        ticket.Wait();
        return ticket.Lsn;
    }
}
