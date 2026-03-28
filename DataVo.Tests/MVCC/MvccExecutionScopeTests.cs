using DataVo.Core.MVCC;
using Xunit;

namespace DataVo.Tests.MVCC;

public class MvccExecutionScopeTests
{
    [Fact]
    public void PushSnapshot_SetsAndRestoresSnapshot()
    {
        Assert.Null(MvccExecutionScope.CurrentSnapshot);

        var snapshot = new TransactionSnapshot(10, 10);
        using (MvccExecutionScope.PushSnapshot(snapshot))
        {
            Assert.NotNull(MvccExecutionScope.CurrentSnapshot);
            Assert.Equal(10, MvccExecutionScope.CurrentSnapshot!.SnapshotTimestamp);
        }

        Assert.Null(MvccExecutionScope.CurrentSnapshot);
    }

    [Fact]
    public void PushSnapshot_NestedScope_RestoresOuterSnapshot()
    {
        var outer = new TransactionSnapshot(20, 20);
        var inner = new TransactionSnapshot(30, 30);

        using (MvccExecutionScope.PushSnapshot(outer))
        {
            Assert.Equal(20, MvccExecutionScope.CurrentSnapshot!.SnapshotTimestamp);

            using (MvccExecutionScope.PushSnapshot(inner))
            {
                Assert.Equal(30, MvccExecutionScope.CurrentSnapshot!.SnapshotTimestamp);
            }

            Assert.Equal(20, MvccExecutionScope.CurrentSnapshot!.SnapshotTimestamp);
        }

        Assert.Null(MvccExecutionScope.CurrentSnapshot);
    }
}
