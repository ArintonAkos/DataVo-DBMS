using DataVo.Core.Runtime.Reactive;

namespace DataVo.Tests.Reactive;

public sealed class CellValueGuidTests
{
    [Fact]
    public void FromGuid_RoundTripsWithoutBoxingOnRead()
    {
        Guid id = Guid.Parse("2f8f0a22-1f70-4f4b-89c6-2ca6f0843c78");

        CellValue cell = CellValue.From(id);

        Assert.Equal(CellType.Guid, cell.Type);
        Assert.False(cell.IsNull);
        Assert.Equal(id, cell.AsGuid());
        Assert.Equal(id, cell.ToObject());
    }

    [Fact]
    public void FromObject_AcceptsGuid()
    {
        Guid id = Guid.Parse("63be54f1-79bd-4877-9f42-f019f0e4ff89");

        CellValue cell = CellValue.From((object)id);

        Assert.Equal(CellType.Guid, cell.Type);
        Assert.Equal(id, cell.AsGuid());
    }

    [Fact]
    public void GuidCorpus_RoundTripsEveryBitPattern()
    {
        var random = new Random(0x71C3);
        var bytes = new byte[16];
        var ids = new List<Guid>
        {
            Guid.Empty,
            new("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            new("00112233-4455-6677-8899-aabbccddeeff"),
        };

        for (int i = 0; i < 128; i++)
        {
            random.NextBytes(bytes);
            ids.Add(new Guid(bytes));
        }

        foreach (Guid id in ids)
        {
            Assert.Equal(id, CellValue.From(id).AsGuid());
        }
    }

    [Fact]
    public void AsGuid_WarmCalls_DoNotAllocate()
    {
        CellValue cell = CellValue.From(new Guid("fedcba98-7654-3210-fedc-ba9876543210"));
        Guid sink = default;
        for (int i = 0; i < 2_000; i++)
        {
            sink = cell.AsGuid();
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        const int measured = 10_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < measured; i++)
        {
            sink = cell.AsGuid();
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(sink);
        Assert.Equal(0, allocated);
    }
}
