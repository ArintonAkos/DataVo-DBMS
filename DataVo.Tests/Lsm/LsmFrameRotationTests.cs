using DataVo.Core;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

/// <summary>
/// A commit frame is replayed whole or skipped whole, by its LSN. A MemTable that rotated (and flushed)
/// halfway through applying a frame made the flushed file claim that LSN while the rest of the frame was
/// still only in memory, so a crash then lost those rows. Found by <see cref="LsmModelBasedTests"/>.
/// </summary>
public sealed class LsmFrameRotationTests : IDisposable
{
    private readonly string _root = TestStorageDirectories.Create("lsm_frame_rotation");

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
    public void FrameLargerThanTheMemTable_SurvivesACrash()
    {
        var engine = new LsmStorageEngine(_root, LsmWalDurabilityMode.RelaxedOsBuffer) { MemTableFlushThresholdOverrideBytes = 4_096 };
        List<byte[]> rows = [.. Enumerable.Range(0, 40).Select(i => Enumerable.Repeat((byte)i, 300).ToArray())];
        List<RowKey> rowIds = engine.InsertRows("db", "t", rows); // one frame of ~12 KB: rotates mid-frame
        engine.SimulateCrashForTesting();

        using var reopened = new LsmStorageEngine(_root, LsmWalDurabilityMode.RelaxedOsBuffer);
        for (int i = 0; i < rowIds.Count; i++)
        {
            Assert.Equal(rows[i], reopened.ReadRow("db", "t", rowIds[i]));
        }

        Assert.Equal(rowIds.Count, reopened.EstimateRowCount("db", "t"));
        Assert.True(reopened.InsertRow("db", "t", [1]) > rowIds[^1], "a row id was reused");
    }
}
