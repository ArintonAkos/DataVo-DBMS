using DataVo.Core.Runtime.Reactive;

namespace DataVo.Benchmarks.Common;

public sealed class ReactiveDeltaCounter
{
    public long Added { get; private set; }
    public long Removed { get; private set; }
    public long Updated { get; private set; }
    public long Batches { get; private set; }

    public long TotalRows => Added + Removed + Updated;

    public void Apply(QueryChange change)
    {
        Batches++;
        Added += change.Added.Count;
        Removed += change.Removed.Count;
        Updated += change.Updated.Count;
    }

    public DeltaSummary Snapshot() => new(Added, Removed, Updated, TotalRows, Batches);
}
