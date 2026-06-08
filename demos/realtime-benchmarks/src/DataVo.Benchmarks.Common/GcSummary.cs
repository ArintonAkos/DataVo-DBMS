namespace DataVo.Benchmarks.Common;

public sealed record GcSummary(
    long AllocatedBytes,
    long LiveMemoryDeltaBytes,
    int Gen0Collections,
    int Gen1Collections,
    int Gen2Collections,
    int PauseCount,
    double PauseP99Ms,
    double PauseMaxMs);
