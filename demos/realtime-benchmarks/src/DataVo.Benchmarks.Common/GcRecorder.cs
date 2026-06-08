namespace DataVo.Benchmarks.Common;

public sealed class GcRecorder
{
    private readonly long _allocatedBytes;
    private readonly long _memoryBytes;
    private readonly int _gen0;
    private readonly int _gen1;
    private readonly int _gen2;
    private readonly TimeSpan[] _pauseDurations;

    private GcRecorder()
    {
        _allocatedBytes = GC.GetTotalAllocatedBytes(precise: false);
        _memoryBytes = GC.GetTotalMemory(forceFullCollection: false);
        _gen0 = GC.CollectionCount(0);
        _gen1 = GC.CollectionCount(1);
        _gen2 = GC.CollectionCount(2);
        _pauseDurations = [.. GC.GetGCMemoryInfo().PauseDurations];
    }

    public static GcRecorder Capture() => new();

    public GcSummary Since(GcRecorder before)
    {
        TimeSpan[] afterPauses = _pauseDurations;
        TimeSpan[] newPauses = afterPauses.Length > before._pauseDurations.Length
            ? afterPauses.Skip(before._pauseDurations.Length).ToArray()
            : [];

        double[] pauseMs = newPauses.Select(pause => pause.TotalMilliseconds).OrderBy(x => x).ToArray();

        return new GcSummary(
            AllocatedBytes: _allocatedBytes - before._allocatedBytes,
            LiveMemoryDeltaBytes: _memoryBytes - before._memoryBytes,
            Gen0Collections: _gen0 - before._gen0,
            Gen1Collections: _gen1 - before._gen1,
            Gen2Collections: _gen2 - before._gen2,
            PauseCount: pauseMs.Length,
            PauseP99Ms: Percentile.FromSorted(pauseMs, 99),
            PauseMaxMs: pauseMs.Length == 0 ? 0 : pauseMs[^1]);
    }
}
